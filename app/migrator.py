"""
分区迁移引擎：把默认分区中某月的历史观测在线迁移到新建的月分区。

阶段（全部可断点续迁，状态持久化在 migration_runs）：
  preparing   建影子表(带 CHECK 约束)、在默认分区上建增量捕获触发器、登记运行记录
  waiting     等待"触发器创建之前已开启且写过数据"的老事务结束（只等待，不阻塞新写入）
  copying     键集分页分批复制（可 sleep 放慢），每批一个事务，进度落库
  applying    按 seq 顺序回放 migration_delta 增量到影子表
  finalizing  单个事务：SHARE 锁整棵分区树（只挡写、只此短暂锁表）→ 回放剩余增量 →
              摘触发器 → DELETE...RETURNING 搬走漏网行 → ATTACH PARTITION →
              清理约束/函数/增量 → 状态置 done。原子提交，失败整体回滚。

并发正确性要点：
  * 影子表在 ATTACH 之前不属于父表，父表查询永远只看到一份数据，不重复、不缺失；
  * 复制期间默认分区上的 INSERT/UPDATE/DELETE 由触发器写入 migration_delta，
    回放是幂等的（insert/update -> upsert，delete -> 按主键删）；
  * 触发器创建前已开启的老事务可能逃过触发器，故 copying 之前先等它们结束，
    finalizing 里的"漏网行搬迁"再兜底一次；
  * 任意时刻进程崩溃：影子表/增量表/状态都在库里，重试从断点继续即可。
"""

from __future__ import annotations

import re
import threading
import time
from datetime import date, datetime, timezone

import psycopg

from db import connect, log

MONTH_RE = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")

# 状态常量
ST_PREPARING = "preparing"
ST_WAITING = "waiting"
ST_COPYING = "copying"
ST_APPLYING = "applying"
ST_FINALIZING = "finalizing"
ST_DONE = "done"
ST_FAILED = "failed"


class MigrationBusy(Exception):
    """已有迁移在进行（一次只迁一个月）。"""


class AlreadyDone(Exception):
    """该月已完成迁移，不再搬迁。"""


class InjectedFailure(Exception):
    """故障注入（仅用于演示/自测断点续迁）。"""


def parse_month(month: str) -> date:
    m = MONTH_RE.match(month or "")
    if not m:
        raise ValueError(f"月份格式应为 YYYY-MM，收到: {month!r}")
    return date(int(m.group(1)), int(m.group(2)), 1)


def next_month(month: date) -> date:
    if month.month == 12:
        return date(month.year + 1, 1, 1)
    return date(month.year, month.month + 1, 1)


def ident_for(month: date) -> dict:
    suffix = f"{month.year}_{month.month:02d}"
    return {
        "part": f"observations_{suffix}",
        "chk": f"observations_{suffix}_range_chk",
        "fn": f"obs_delta_fn_{suffix}",
        "trg": f"obs_delta_trg_{suffix}",
    }


def _set_status(conn, month: date, status: str, **fields):
    sets = ["status = %s", "updated_at = now()"]
    params = [status]
    for k, v in fields.items():
        sets.append(f"{k} = %s")
        params.append(v)
    params.append(month)
    conn.execute(
        f"UPDATE migration_runs SET {', '.join(sets)} WHERE month = %s", params
    )


def _get_run(conn, month: date):
    cur = conn.execute(
        "SELECT month, status, total_estimate, copied_rows, applied_delta,"
        "       last_station, last_ts, error, updated_at"
        " FROM migration_runs WHERE month = %s",
        (month,),
    )
    return cur.fetchone()


class MigrationWorker(threading.Thread):
    """后台迁移线程。整个应用同一时刻只允许一个实例运行。"""

    def __init__(
        self,
        month: date,
        batch_size: int = 500,
        sleep_ms: int = 0,
        fail_at: str | None = None,
    ):
        super().__init__(daemon=True, name=f"migrate-{month}")
        self.month = month
        self.next_month = next_month(month)
        self.batch_size = max(1, min(batch_size, 10000))
        self.sleep_ms = max(0, min(sleep_ms, 10000))
        self.fail_at = fail_at  # 故障注入点：preparing/waiting/copying/applying/finalizing
        self.ids = ident_for(month)
        self.lo = f"{month.isoformat()} 00:00:00+00"
        self.hi = f"{self.next_month.isoformat()} 00:00:00+00"

    # ------------------------------------------------------------------ util
    def _maybe_fail(self, stage: str):
        if self.fail_at == stage:
            raise InjectedFailure(f"故障注入：在 {stage} 阶段人为失败")

    def _log(self, msg: str):
        log(f"[migrate {self.month}] {msg}")

    # ---------------------------------------------------------------- stages
    def stage_prepare(self, conn):
        """建影子表 + 增量触发器 + 登记运行记录（幂等）。"""
        _set_status(conn, self.month, ST_PREPARING)
        p = self.ids["part"]
        # 1) 影子表：显式列定义 + 主键 + 月份 CHECK（CHECK 可让 ATTACH 跳过校验扫描）
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {p} (
                station_id  text          NOT NULL,
                ts          timestamptz   NOT NULL,
                value       integer       NOT NULL,
                PRIMARY KEY (station_id, ts)
            )
            """
        )
        row = conn.execute(
            "SELECT 1 FROM pg_constraint WHERE conname = %s", (self.ids["chk"],)
        ).fetchone()
        if not row:
            conn.execute(
                f"ALTER TABLE {p} ADD CONSTRAINT {self.ids['chk']} "
                f"CHECK (ts >= TIMESTAMPTZ '{self.lo}' AND ts < TIMESTAMPTZ '{self.hi}')"
            )
        # 2) 增量捕获触发器（只记录落在本月的行；正确处理跨月 UPDATE）
        conn.execute(
            f"""
            CREATE OR REPLACE FUNCTION {self.ids['fn']}() RETURNS trigger AS $fn$
            DECLARE
                lo timestamptz := TIMESTAMPTZ '{self.lo}';
                hi timestamptz := TIMESTAMPTZ '{self.hi}';
                m  date        := DATE '{self.month.isoformat()}';
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    IF NEW.ts >= lo AND NEW.ts < hi THEN
                        INSERT INTO migration_delta (month, op, station_id, ts, value)
                        VALUES (m, 'insert', NEW.station_id, NEW.ts, NEW.value);
                    END IF;
                    RETURN NEW;
                ELSIF TG_OP = 'UPDATE' THEN
                    IF (OLD.ts >= lo AND OLD.ts < hi) AND NOT (NEW.ts >= lo AND NEW.ts < hi) THEN
                        INSERT INTO migration_delta (month, op, station_id, ts, value)
                        VALUES (m, 'delete', OLD.station_id, OLD.ts, NULL);
                    ELSIF (NEW.ts >= lo AND NEW.ts < hi) AND NOT (OLD.ts >= lo AND OLD.ts < hi) THEN
                        INSERT INTO migration_delta (month, op, station_id, ts, value)
                        VALUES (m, 'insert', NEW.station_id, NEW.ts, NEW.value);
                    ELSIF (NEW.ts >= lo AND NEW.ts < hi) THEN
                        INSERT INTO migration_delta (month, op, station_id, ts, value)
                        VALUES (m, 'update', NEW.station_id, NEW.ts, NEW.value);
                    END IF;
                    RETURN NEW;
                ELSE
                    IF OLD.ts >= lo AND OLD.ts < hi THEN
                        INSERT INTO migration_delta (month, op, station_id, ts, value)
                        VALUES (m, 'delete', OLD.station_id, OLD.ts, NULL);
                    END IF;
                    RETURN OLD;
                END IF;
            END
            $fn$ LANGUAGE plpgsql
            """
        )
        trg = conn.execute(
            "SELECT 1 FROM pg_trigger WHERE tgname = %s", (self.ids["trg"],)
        ).fetchone()
        if not trg:
            # CREATE TRIGGER 需要 SHARE ROW EXCLUSIVE，瞬间阻塞写，属正常的短暂 DDL 锁
            conn.execute(
                f"CREATE TRIGGER {self.ids['trg']} AFTER INSERT OR UPDATE OR DELETE "
                f"ON observations_default FOR EACH ROW "
                f"EXECUTE FUNCTION {self.ids['fn']}()"
            )
        # 3) 登记/刷新运行记录（保留已有进度以支持断点续迁）
        total = conn.execute(
            "SELECT count(*) FROM observations_default WHERE ts >= %s AND ts < %s",
            (self.lo, self.hi),
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO migration_runs (month, status, total_estimate)
            VALUES (%s, %s, %s)
            ON CONFLICT (month) DO UPDATE
              SET status = EXCLUDED.status,
                  total_estimate = EXCLUDED.total_estimate,
                  error = NULL,
                  updated_at = now()
            """,
            (self.month, ST_PREPARING, total),
        )
        self._log(f"影子表 {p} 就绪，增量触发器已挂载，本月默认分区约 {total} 行")

    def stage_wait_old_xacts(self, conn, t0):
        """等待 t0 之前开启且持有写事务号的老事务结束，避免它们逃过触发器。"""
        _set_status(conn, self.month, ST_WAITING)
        deadline = time.monotonic() + 120
        while True:
            n = conn.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE backend_xid IS NOT NULL AND xact_start < %s",
                (t0,),
            ).fetchone()[0]
            if n == 0:
                return
            if time.monotonic() > deadline:
                raise RuntimeError(f"等待老事务结束超时（仍有 {n} 个）")
            self._log(f"等待 {n} 个老事务结束…")
            time.sleep(0.2)

    def stage_copy(self, conn):
        """键集分页分批复制；每批一个事务；批间可 sleep 放慢演示。"""
        _set_status(conn, self.month, ST_COPYING)
        run = _get_run(conn, self.month)
        last_station = run[5] or ""
        last_ts = run[6] or datetime.min.replace(tzinfo=timezone.utc)
        copied = run[3]
        while True:
            with conn.transaction():
                rows = conn.execute(
                    """
                    SELECT station_id, ts, value FROM observations_default
                    WHERE ts >= %s AND ts < %s
                      AND (station_id > %s OR (station_id = %s AND ts > %s))
                    ORDER BY station_id, ts
                    LIMIT %s
                    """,
                    (self.lo, self.hi, last_station, last_station, last_ts,
                     self.batch_size),
                ).fetchall()
                if rows:
                    cur = conn.cursor()
                    cur.executemany(
                        f"INSERT INTO {self.ids['part']} (station_id, ts, value) "
                        f"VALUES (%s, %s, %s) "
                        f"ON CONFLICT (station_id, ts) DO UPDATE SET value = EXCLUDED.value",
                        rows,
                    )
                    copied += len(rows)
                    last_station, last_ts = rows[-1][0], rows[-1][1]
                    _set_status(
                        conn, self.month, ST_COPYING,
                        copied_rows=copied, last_station=last_station, last_ts=last_ts,
                    )
            if len(rows) < self.batch_size:
                break
            # 故障注入：复制到约 1/3 时人为中断，验证断点续迁
            if self.fail_at == "copying":
                run = _get_run(conn, self.month)
                if run[2] > 0 and copied >= run[2] / 3:
                    self._maybe_fail("copying")
            if self.sleep_ms:
                time.sleep(self.sleep_ms / 1000)
        self._log(f"批量复制完成，共复制 {copied} 行")

    def _apply_delta_tx(self, conn, limit: int) -> int:
        """在调用方事务内：取出一批增量并按 seq 顺序回放到影子表。返回条数。"""
        rows = conn.execute(
            """
            WITH batch AS (
                SELECT seq FROM migration_delta
                WHERE month = %s ORDER BY seq LIMIT %s
            )
            DELETE FROM migration_delta
            WHERE seq IN (SELECT seq FROM batch)
            RETURNING seq, op, station_id, ts, value
            """,
            (self.month, limit),
        ).fetchall()
        rows.sort(key=lambda r: r[0])  # RETURNING 不保证顺序，必须按 seq 回放
        cur = conn.cursor()
        for _seq, op, station_id, ts, value in rows:
            if op in ("insert", "update"):
                cur.execute(
                    f"INSERT INTO {self.ids['part']} (station_id, ts, value) "
                    f"VALUES (%s, %s, %s) "
                    f"ON CONFLICT (station_id, ts) DO UPDATE SET value = EXCLUDED.value",
                    (station_id, ts, value),
                )
            else:  # delete
                cur.execute(
                    f"DELETE FROM {self.ids['part']} WHERE station_id = %s AND ts = %s",
                    (station_id, ts),
                )
        return len(rows)

    def stage_apply(self, conn):
        """复制结束后追赶增量（此时写入仍在继续，可能要多轮）。"""
        _set_status(conn, self.month, ST_APPLYING)
        total_applied = _get_run(conn, self.month)[4]
        idle_rounds = 0
        while idle_rounds < 2:
            with conn.transaction():
                n = self._apply_delta_tx(conn, self.batch_size)
                if n:
                    total_applied += n
                    _set_status(conn, self.month, ST_APPLYING,
                                applied_delta=total_applied)
            if n == 0:
                idle_rounds += 1
            else:
                idle_rounds = 0
            if self.fail_at == "applying" and total_applied > 0:
                self._maybe_fail("applying")
            if n and self.sleep_ms:
                time.sleep(min(self.sleep_ms, 500) / 1000)
        self._log(f"增量回放完成，累计回放 {total_applied} 条")

    def stage_finalize(self, conn):
        """最终切换：唯一会短暂锁表的步骤，且全部在一个事务里原子完成。"""
        _set_status(conn, self.month, ST_FINALIZING)
        p = self.ids["part"]
        conn.execute("SET lock_timeout = '10s'")
        conn.execute("SET statement_timeout = '120s'")
        with conn.transaction():
            # 1) 一次性锁住整棵分区树（SHARE：挡写不挡读），避免与写入方
            #    “父表→分区”的加锁顺序形成循环等待；只此一步短暂锁表
            self._log("获取 observations 树 SHARE 锁（短暂挡写）…")
            conn.execute("LOCK TABLE observations IN SHARE MODE")
            # 2) 回放锁前最后的增量
            applied_now = 0
            while True:
                n = self._apply_delta_tx(conn, 1000)
                applied_now += n
                if n < 1000:
                    break
            # 3) 摘掉触发器，避免下面的搬迁 DELETE 产生伪增量
            conn.execute(
                f"DROP TRIGGER IF EXISTS {self.ids['trg']} ON observations_default"
            )
            # 4) 漏网行搬迁（正常情况下为 0 行；影子表里的新值优先）
            cur = conn.execute(
                f"""
                WITH moved AS (
                    DELETE FROM observations_default
                    WHERE ts >= %s AND ts < %s
                    RETURNING station_id, ts, value
                )
                INSERT INTO {p} SELECT * FROM moved
                ON CONFLICT (station_id, ts) DO NOTHING
                """,
                (self.lo, self.hi),
            )
            moved = cur.rowcount
            # 5) 故障注入：锁已拿、增量已回放，在 ATTACH 前失败 —— 验证整体回滚
            self._maybe_fail("finalizing")
            # 6) 挂载分区（影子表带 CHECK，跳过校验扫描；需短暂 ACCESS EXCLUSIVE）
            conn.execute(
                f"ALTER TABLE observations ATTACH PARTITION {p} "
                f"FOR VALUES FROM ('{self.lo}') TO ('{self.hi}')"
            )
            # 7) 清理：冗余 CHECK、函数、增量、状态
            conn.execute(
                f"ALTER TABLE {p} DROP CONSTRAINT IF EXISTS {self.ids['chk']}"
            )
            conn.execute(f"DROP FUNCTION IF EXISTS {self.ids['fn']}()")
            conn.execute(
                "DELETE FROM migration_delta WHERE month = %s", (self.month,)
            )
            run = _get_run(conn, self.month)
            _set_status(
                conn, self.month, ST_DONE,
                applied_delta=(run[4] or 0) + applied_now, error=None,
            )
        self._log(
            f"切换完成：ATTACH {p}；本阶段回放增量 {applied_now} 条，"
            f"漏网行 {moved} 行"
        )

    # ------------------------------------------------------------------ main
    def migrate(self):
        with connect() as conn:
            # 已完成直接返回（防御；上层已判断）
            run = _get_run(conn, self.month)
            if run and run[1] == ST_DONE:
                self._log("该月已完成迁移，跳过")
                return
            t0 = conn.execute("SELECT now()").fetchone()[0]
            self.stage_prepare(conn)
            self._maybe_fail("preparing")
            self.stage_wait_old_xacts(conn, t0)
            self._maybe_fail("waiting")
            self.stage_copy(conn)
            self.stage_apply(conn)
            self.stage_finalize(conn)

    def run(self):
        try:
            self.migrate()
        except Exception as exc:  # noqa: BLE001 - 记录到库里供重试
            self._log(f"迁移中断：{exc!r}")
            try:
                with connect() as conn:
                    _set_status(conn, self.month, ST_FAILED, error=str(exc))
            except Exception:  # noqa: BLE001
                pass
        finally:
            worker_finished(self)


# --------------------------------------------------------------------- 控制
_worker_lock = threading.Lock()
_current: MigrationWorker | None = None


def start_migration(month_str: str, batch_size: int, sleep_ms: int,
                    fail_at: str | None) -> dict:
    """启动一个月份的迁移。返回状态描述。"""
    global _current
    month = parse_month(month_str)
    with connect() as conn:
        run = _get_run(conn, month)
        if run and run[1] == ST_DONE:
            raise AlreadyDone(f"{month_str} 已完成迁移，无需再次搬迁")
    with _worker_lock:
        if _current is not None and _current.is_alive():
            raise MigrationBusy(f"正在迁移 {_current.month}，一次只能迁一个月")
        _current = MigrationWorker(month, batch_size, sleep_ms, fail_at)
        _current.start()
    return {"month": month_str, "started": True}


def worker_finished(worker: MigrationWorker):
    global _current
    with _worker_lock:
        if _current is worker:
            _current = None


def current_worker_month() -> str | None:
    w = _current
    if w is not None and w.is_alive():
        return w.month.isoformat()[:7]
    return None
