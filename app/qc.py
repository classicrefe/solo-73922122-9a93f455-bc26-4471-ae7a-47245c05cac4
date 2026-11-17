"""
质控报告：找出"测值持续不变"的站点区段，供安排仪器检修。

判定规则
--------
只把 **相邻整点**（UTC）且测值相同的观测连成一段：
  * 时刻必须正好落在整点（分/秒/微秒均为 0），非整点观测不参与；
  * 下一条必须是上一条 + 1 小时，否则断段；
  * 只统计 [UTC 月首, 次月首) 内的观测，跨月不连段；
  * 段内观测条数 >= 阈值（3～12）才进入报告。

一致性与并发
------------
* 每个报告是一份不可变"视图"：一次计算期间在 **同一个 REPEATABLE READ 只读事务**
  里读取 `qc_watch` 代次与全部观测，写入只发生在最后的短事务里；因此报告内容
  只取自同一已提交快照。
* `qc_watch(station_id, month)` 由 `observations` 上的行级触发器维护：任何
  INSERT/UPDATE/DELETE（无论落到父表、默认分区还是已挂载月分区）都会把该
  站该月的代次 +1，并把该站该月所有"当前"报告置为 stale。即使值后来改回，
  代次也只增不减，旧报告不会"复活"。
* 发布前在短事务里锁住 `qc_watch` 行并复核代次：与快照开始时不同 => 本次计算
  期间相关观测已变化，结果标记 superseded，绝不覆盖旧报告。
* 计算进程中途失败/退出：只留下一条 computing 记录（启动时转为 failed），
  旧报告与其 segment 仍在库里，可继续查看并重算。
* 月分区在线迁移（物理复制/切换）不是逻辑变更：迁移的最终切换事务里设置
  `qc.suppress_watch = 'on'`，触发器跳过代次递增，报告既不失效也不改变判定。
"""

from __future__ import annotations

import threading
import time
from datetime import date, datetime, timedelta, timezone

from db import connect, log

THRESHOLD_MIN = 3
THRESHOLD_MAX = 12

ST_COMPUTING = "computing"
ST_READY = "ready"
ST_STALE = "stale"
ST_FAILED = "failed"
ST_SUPERSEDED = "superseded"

# 计算中状态（仅进程内；持久状态在 qc_reports.status）
_running: dict[tuple[str, str, int], threading.Thread] = {}
_running_lock = threading.Lock()


# ------------------------------------------------------------------ 触发器
QC_FN_SQL = r"""
CREATE OR REPLACE FUNCTION qc_watch_fn() RETURNS trigger AS $qc$
DECLARE
    m_new date;
    m_old date;
BEGIN
    -- 迁移的物理搬迁不是逻辑变更：其切换事务里会 SET LOCAL 本开关
    IF coalesce(current_setting('qc.suppress_watch', true), '') = 'on' THEN
        RETURN COALESCE(NEW, OLD);
    END IF;

    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        m_new := date_trunc('month', (NEW.ts AT TIME ZONE 'UTC'))::date;
        INSERT INTO qc_watch (station_id, month, gen, updated_at)
        VALUES (NEW.station_id, m_new, 1, clock_timestamp())
        ON CONFLICT (station_id, month) DO UPDATE
            SET gen = qc_watch.gen + 1, updated_at = clock_timestamp();
        UPDATE qc_reports
           SET is_current = false,
               status = 'stale',
               stale_at = clock_timestamp(),
               stale_reason = '该站该月观测发生变化',
               updated_at = clock_timestamp()
         WHERE station_id = NEW.station_id AND month = m_new AND is_current;
    END IF;

    IF TG_OP = 'DELETE'
       OR (TG_OP = 'UPDATE'
           AND (OLD.station_id IS DISTINCT FROM NEW.station_id
                OR date_trunc('month', (OLD.ts AT TIME ZONE 'UTC'))::date
                   IS DISTINCT FROM m_new)) THEN
        m_old := date_trunc('month', (OLD.ts AT TIME ZONE 'UTC'))::date;
        INSERT INTO qc_watch (station_id, month, gen, updated_at)
        VALUES (OLD.station_id, m_old, 1, clock_timestamp())
        ON CONFLICT (station_id, month) DO UPDATE
            SET gen = qc_watch.gen + 1, updated_at = clock_timestamp();
        UPDATE qc_reports
           SET is_current = false,
               status = 'stale',
               stale_at = clock_timestamp(),
               stale_reason = '该站该月观测发生变化',
               updated_at = clock_timestamp()
         WHERE station_id = OLD.station_id AND month = m_old AND is_current;
    END IF;

    RETURN COALESCE(NEW, OLD);
END
$qc$ LANGUAGE plpgsql
"""


def ensure_trigger():
    """在父表 observations 上建/确认质控变更触发器（自动传播到所有分区）。

    父表尚未就绪（如 init 脚本仍在执行）时跳过，下次启动再建；不阻塞应用启动。
    """
    with connect() as conn:
        ready = conn.execute("SELECT to_regclass('observations')").fetchone()[0]
        if ready is None:
            log("[质控] observations 尚未就绪，跳过触发器创建")
            return
        conn.execute(QC_FN_SQL)
        exists = conn.execute(
            "SELECT 1 FROM pg_trigger WHERE tgname = 'qc_watch_trg'"
            " AND tgrelid = 'observations'::regclass"
        ).fetchone()
        if not exists:
            conn.execute(
                "CREATE TRIGGER qc_watch_trg "
                "AFTER INSERT OR UPDATE OR DELETE ON observations "
                "FOR EACH ROW EXECUTE FUNCTION qc_watch_fn()"
            )
            log("[质控] 已在 observations 上创建变更跟踪触发器")


# ------------------------------------------------------------------ 查询/发布
def _correct_threshold(threshold: int) -> int:
    if not THRESHOLD_MIN <= threshold <= THRESHOLD_MAX:
        raise ValueError(
            f"连续次数阈值应在 {THRESHOLD_MIN}～{THRESHOLD_MAX}，收到 {threshold}"
        )
    return threshold


def month_bounds(month: date) -> tuple[datetime, datetime]:
    lo = datetime(month.year, month.month, 1, tzinfo=timezone.utc)
    if month.month == 12:
        hi = datetime(month.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        hi = datetime(month.year, month.month + 1, 1, tzinfo=timezone.utc)
    return lo, hi


def segments_from_rows(rows, threshold: int) -> list[dict]:
    """rows: [(ts, value)]，须按 ts 升序。返回达标段（纯函数，便于单测）。

    只连"相邻整点且测值相同"的观测；月界由调用方限定读取范围，故自然截断。
    """
    segs: list[dict] = []
    cur: dict | None = None

    def flush():
        if cur is not None and cur["length"] >= threshold:
            segs.append(
                {"start_ts": cur["start_ts"], "end_ts": cur["end_ts"],
                 "value": cur["value"], "length": cur["length"]}
            )

    for ts, value in rows:
        ts = ts.astimezone(timezone.utc)
        if ts.minute or ts.second or ts.microsecond:
            continue  # 非整点观测不参与连段
        if (cur is not None
                and value == cur["value"]
                and ts - cur["end_ts"] == timedelta(hours=1)):
            cur["end_ts"] = ts
            cur["length"] += 1
        else:
            flush()
            cur = {"start_ts": ts, "end_ts": ts, "value": value, "length": 1}
    flush()
    return segs


def _latest_attempt(conn, station: str, month: date, threshold: int):
    return conn.execute(
        "SELECT id, status, is_current, gen, computed_at, stale_at, stale_reason,"
        "       error, started_at, updated_at"
        " FROM qc_reports WHERE station_id = %s AND month = %s AND threshold = %s"
        " ORDER BY id DESC LIMIT 1",
        (station, month, threshold),
    ).fetchone()


def _segments(conn, report_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT start_ts, end_ts, value, length FROM qc_segments"
        " WHERE report_id = %s ORDER BY start_ts",
        (report_id,),
    ).fetchall()
    return [
        {"start_ts": r[0].isoformat(), "end_ts": r[1].isoformat(),
         "value": r[2], "length": r[3]}
        for r in rows
    ]


def get_report(station: str, month: date, threshold: int) -> dict:
    """返回该站该月该阈值的报告视图与最近重算状态。

    优先取当前有效视图；没有 current 时回退到最近一次成功计算（可能已 stale），
    因此计算失败/进程退出后旧报告仍然可看。所有读取在同一个已提交快照里完成。
    """
    threshold = _correct_threshold(threshold)
    conn = connect(autocommit=False)
    try:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        current = conn.execute(
            "SELECT id, status, is_current, gen, computed_at, stale_at,"
            "       stale_reason, error, started_at, updated_at"
            " FROM qc_reports WHERE station_id = %s AND month = %s"
            "   AND threshold = %s AND is_current"
            " ORDER BY id DESC LIMIT 1",
            (station, month, threshold),
        ).fetchone()
        fallback = conn.execute(
            "SELECT id, status, is_current, gen, computed_at, stale_at,"
            "       stale_reason, error, started_at, updated_at"
            " FROM qc_reports WHERE station_id = %s AND month = %s"
            "   AND threshold = %s AND computed_at IS NOT NULL"
            "   AND status IN ('ready', 'stale', 'superseded')"
            " ORDER BY id DESC LIMIT 1",
            (station, month, threshold),
        ).fetchone()
        shown = current or fallback
        attempts = conn.execute(
            "SELECT id, status, is_current, computed_at, stale_at, stale_reason,"
            "       error, started_at, updated_at"
            " FROM qc_reports WHERE station_id = %s AND month = %s"
            "   AND threshold = %s ORDER BY id DESC LIMIT 10",
            (station, month, threshold),
        ).fetchall()
        watch = conn.execute(
            "SELECT gen, updated_at FROM qc_watch"
            " WHERE station_id = %s AND month = %s",
            (station, month),
        ).fetchone()
        segs = _segments(conn, shown[0]) if shown else []
        conn.rollback()
    finally:
        conn.close()

    def attempt(r):
        return {
            "id": r[0], "status": r[1], "is_current": r[2],
            "computed_at": r[3].isoformat() if r[3] else None,
            "stale_at": r[4].isoformat() if r[4] else None,
            "stale_reason": r[5], "error": r[6],
            "started_at": r[7].isoformat() if r[7] else None,
            "updated_at": r[8].isoformat() if r[8] else None,
        }

    report = None
    if shown:
        report = {
            "id": shown[0], "status": shown[1], "is_current": shown[2],
            "gen": shown[3],
            "computed_at": shown[4].isoformat() if shown[4] else None,
            "stale_at": shown[5].isoformat() if shown[5] else None,
            "stale_reason": shown[6], "error": shown[7],
            "segments": segs,
            "segment_count": len(segs),
            "observation_count": sum(s["length"] for s in segs),
        }
    latest = attempts[0] if attempts else None
    return {
        "station_id": station,
        "month": month.isoformat()[:7],
        "threshold": threshold,
        "report": report,
        "is_current": bool(current),
        "watch_gen": watch[0] if watch else 0,
        "watch_updated_at": watch[1].isoformat() if watch and watch[1] else None,
        "latest_attempt": attempt(latest) if latest else None,
        "attempts": [attempt(r) for r in attempts],
        "computing": is_running(station, month.isoformat()[:7], threshold),
    }


# ------------------------------------------------------------------ 计算
def list_options() -> dict:
    """报告页下拉可选项：有观测的站号与月份（UTC）。"""
    with connect() as conn:
        stations = [r[0] for r in conn.execute(
            "SELECT DISTINCT station_id FROM observations ORDER BY station_id"
        )]
        months = [r[0].isoformat()[:7] for r in conn.execute(
            "SELECT DISTINCT date_trunc('month', ts AT TIME ZONE 'UTC')::date AS m"
            " FROM observations ORDER BY m DESC"
        )]
    return {"stations": stations, "months": months,
            "threshold_min": THRESHOLD_MIN, "threshold_max": THRESHOLD_MAX}


def is_running(station: str, month: str, threshold: int) -> bool:
    key = (station, month, threshold)
    with _running_lock:
        t = _running.get(key)
        return bool(t and t.is_alive())


def start_compute(station: str, month: date, threshold: int,
                  delay_ms: int = 0, fail_after_snapshot: bool = False) -> int:
    """登记一次计算并启动后台线程，返回 qc_reports.id。"""
    threshold = _correct_threshold(threshold)
    station = (station or "").strip()
    if not station:
        raise ValueError("站号不能为空")
    key = (station, month.isoformat()[:7], threshold)
    with _running_lock:
        t = _running.get(key)
        if t and t.is_alive():
            raise RuntimeError(f"{station} {month.isoformat()[:7]} 阈值 {threshold} 正在计算")
        with connect() as conn:
            rid = conn.execute(
                "INSERT INTO qc_reports (station_id, month, threshold, status, gen)"
                " VALUES (%s, %s, %s, 'computing', 0) RETURNING id",
                (station, month, threshold),
            ).fetchone()[0]
        th = threading.Thread(
            target=_compute_worker,
            args=(rid, station, month, threshold, delay_ms, fail_after_snapshot),
            daemon=True, name=f"qc-{station}-{key[1]}-{threshold}",
        )
        _running[key] = th
        th.start()
    return rid


def _set_attempt(conn, rid: int, status: str, **fields):
    sets = ["status = %s", "updated_at = now()"]
    params: list = [status]
    for k, v in fields.items():
        sets.append(f"{k} = %s")
        params.append(v)
    params.append(rid)
    conn.execute(f"UPDATE qc_reports SET {', '.join(sets)} WHERE id = %s", params)


def _compute_worker(rid: int, station: str, month: date, threshold: int,
                    delay_ms: int, fail_after_snapshot: bool):
    key = (station, month.isoformat()[:7], threshold)
    lo, hi = month_bounds(month)
    try:
        # 1) 同一快照里读代次 + 全部观测（REPEATABLE READ 只读）
        conn = connect(autocommit=False)
        try:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            row = conn.execute(
                "SELECT gen FROM qc_watch WHERE station_id = %s AND month = %s",
                (station, month),
            ).fetchone()
            gen_before = row[0] if row else 0
            obs = conn.execute(
                "SELECT ts, value FROM observations"
                " WHERE station_id = %s AND ts >= %s AND ts < %s"
                " ORDER BY ts",
                (station, lo, hi),
            ).fetchall()
        finally:
            conn.rollback()
            conn.close()

        # 2) 慢计算演示：快照已固定，但尚未发布；期间采集可自由提交
        if fail_after_snapshot:
            raise RuntimeError("故障注入：快照读取后人为失败")
        if delay_ms:
            time.sleep(min(delay_ms, 60000) / 1000)

        segments = segments_from_rows(obs, threshold)

        # 3) 发布前复核代次（短事务，锁住 watch 行与变更串行化）
        pub = connect(autocommit=False)
        try:
            pub.execute(
                "INSERT INTO qc_watch (station_id, month, gen) VALUES (%s, %s, 0)"
                " ON CONFLICT (station_id, month) DO NOTHING",
                (station, month),
            )
            gen_now = pub.execute(
                "SELECT gen FROM qc_watch WHERE station_id = %s AND month = %s"
                " FOR UPDATE",
                (station, month),
            ).fetchone()[0]
            if gen_now != gen_before:
                pub.rollback()
                raise _Superseded(
                    f"计算期间观测已变化（代次 {gen_before} -> {gen_now}），"
                    f"本次结果作废，保留旧报告"
                )
            # 同站同月同阈值：旧当前报告让位（内容不删除，仍可查看）
            pub.execute(
                "UPDATE qc_reports SET is_current = false, status = 'stale',"
                "       stale_at = now(), stale_reason = '已被更新的报告替代',"
                "       updated_at = now()"
                " WHERE station_id = %s AND month = %s AND threshold = %s"
                "   AND is_current AND id <> %s",
                (station, month, threshold, rid),
            )
            _set_attempt(
                pub, rid, ST_READY,
                is_current=True, gen=gen_before,
                computed_at=datetime.now(timezone.utc), error=None,
            )
            cur = pub.cursor()
            cur.executemany(
                "INSERT INTO qc_segments (report_id, start_ts, end_ts, value, length)"
                " VALUES (%s, %s, %s, %s, %s)",
                [(rid, s["start_ts"], s["end_ts"], s["value"], s["length"])
                 for s in segments],
            )
            pub.commit()
        except _Superseded:
            raise
        except Exception:
            pub.rollback()
            raise
        finally:
            pub.close()

        log(f"[质控] {station} {month.isoformat()[:7]} 阈值 {threshold}："
            f"{len(segments)} 段（快照代次 {gen_before}）")
    except _Superseded as exc:
        try:
            with connect() as c:
                _set_attempt(c, rid, ST_SUPERSEDED, is_current=False,
                             error=str(exc))
        except Exception:  # noqa: BLE001
            pass
        log(f"[质控] {station} {month.isoformat()[:7]} 阈值 {threshold}：{exc}")
    except Exception as exc:  # noqa: BLE001
        try:
            with connect() as c:
                _set_attempt(c, rid, ST_FAILED, is_current=False, error=str(exc))
        except Exception:  # noqa: BLE001
            pass
        log(f"[质控] {station} {month.isoformat()[:7]} 阈值 {threshold} 计算失败：{exc!r}")
    finally:
        with _running_lock:
            if _running.get(key) is threading.current_thread():
                _running.pop(key, None)


class _Superseded(Exception):
    pass
