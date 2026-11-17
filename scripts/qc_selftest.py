#!/usr/bin/env python3
"""
质控异常报告端到端自测（真实 PostgreSQL + 真实 HTTP API + 真实进程退出）。

覆盖：
  1. 相邻整点等值连段、月界截断、阈值 3～12、非整点忽略（与独立地面真值对比）
  2. 直连 SQL 增/删/改 -> 该站该月报告过期；改回也不复活；其他站/月不受影响
  3. 慢计算期间采集照常提交；相关观测变化 -> 本次结果作废、不覆盖旧报告
  4. 计算失败 / 进程硬退出 -> 旧报告仍可查看 -> 重启后可重算
  5. 在线月迁移（物理复制 + 切换）不使报告过期、不改变判定；迁移后直连写入仍能使其过期

运行（要求库已初始化）：
  DATABASE_URL=postgresql://envmon@127.0.0.1:5544/envmon python3 scripts/qc_selftest.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg
import requests

APP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app"))
APP_PORT = int(os.environ.get("APP_PORT", "8011"))
DSN = os.environ.get("DATABASE_URL", "postgresql://envmon@127.0.0.1:5544/envmon")
BASE = os.environ.get("BASE_URL", f"http://127.0.0.1:{APP_PORT}")

_failures: list[str] = []
_app: subprocess.Popen | None = None


def check(name: str, cond: bool, detail: str = ""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}"
          + (f" — {detail}" if detail else ""), flush=True)
    if not cond:
        _failures.append(f"{name}: {detail}")


def db():
    return psycopg.connect(DSN, autocommit=True, options="-c timezone=UTC")


def utc(y, m, d, h=0, mi=0):
    return datetime(y, m, d, h, mi, tzinfo=timezone.utc)


def month_lo(ym: str) -> datetime:
    y, m = map(int, ym.split("-"))
    return utc(y, m, 1)


def month_hi(ym: str) -> datetime:
    lo = month_lo(ym)
    return utc(lo.year + (lo.month == 12), 1 if lo.month == 12 else lo.month + 1, 1)


# --------------------------------------------------------------- 地面真值
def ground_truth(station: str, ym: str, threshold: int) -> list[dict]:
    """独立实现（不复用 app 代码）：相邻整点等值连段。"""
    with db() as c:
        rows = c.execute(
            "SELECT ts, value FROM observations WHERE station_id = %s"
            " AND ts >= %s AND ts < %s ORDER BY ts",
            (station, month_lo(ym), month_hi(ym)),
        ).fetchall()
    out, run = [], None
    for ts, value in rows:
        if ts.minute or ts.second or ts.microsecond:
            continue
        if run and value == run[2] and ts - run[1] == timedelta(hours=1):
            run[1] = ts
            run[3] += 1
        else:
            if run and run[3] >= threshold:
                out.append({"start_ts": run[0], "end_ts": run[1],
                            "value": run[2], "length": run[3]})
            run = [ts, ts, value, 1]
    if run and run[3] >= threshold:
        out.append({"start_ts": run[0], "end_ts": run[1],
                    "value": run[2], "length": run[3]})
    return out


def seed(station: str, ym: str, spec: list[tuple[int, int, int]]):
    """spec: [(小时偏移, 分钟, 值)]，写入父表（默认分区）。"""
    lo = month_lo(ym)
    with db() as c:
        for hour, minute, value in spec:
            c.execute(
                "INSERT INTO observations (station_id, ts, value)"
                " VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (station, lo + timedelta(hours=hour, minutes=minute), value),
            )


def cleanup(*stations: str):
    with db() as c:
        c.execute("DELETE FROM observations WHERE station_id = ANY(%s)", (list(stations),))
        c.execute("DELETE FROM qc_watch WHERE station_id = ANY(%s)", (list(stations),))
        c.execute(
            "DELETE FROM qc_segments WHERE report_id IN"
            " (SELECT id FROM qc_reports WHERE station_id = ANY(%s))",
            (list(stations),),
        )
        c.execute("DELETE FROM qc_reports WHERE station_id = ANY(%s)", (list(stations),))


# ------------------------------------------------------------------ app
def start_app():
    global _app
    logf = open("/tmp/pg-runtime/qc-app.log", "ab")
    _app = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app",
         "--host", "127.0.0.1", "--port", str(APP_PORT)],
        cwd=APP_DIR, stdout=logf, stderr=subprocess.STDOUT,
        env={**os.environ, "DATABASE_URL": DSN},
    )
    wait_healthy()


def wait_healthy(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{BASE}/api/state", timeout=2).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(0.3)
    raise RuntimeError("应用未就绪")


def crash_app():
    global _app
    try:
        requests.post(f"{BASE}/api/debug/crash", timeout=3)
    except requests.RequestException:
        pass
    if _app:
        try:
            _app.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    _app = None
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            requests.get(f"{BASE}/api/state", timeout=1)
            time.sleep(0.3)
        except requests.RequestException:
            return
    raise RuntimeError("应用未退出")


# ------------------------------------------------------------------ API
def recompute(station, ym, threshold, **kw):
    body = {"station_id": station, "month": ym, "threshold": threshold, **kw}
    return requests.post(f"{BASE}/api/qc/recompute", json=body, timeout=10)


def report(station, ym, threshold):
    r = requests.get(f"{BASE}/api/qc/report",
                     params={"station_id": station, "month": ym,
                             "threshold": threshold}, timeout=10)
    r.raise_for_status()
    return r.json()


def wait_ready(station, ym, threshold, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = report(station, ym, threshold)
        la = d["latest_attempt"]
        if la and la["status"] != "computing" and not d["computing"]:
            return d
        time.sleep(0.2)
    raise RuntimeError(f"{station} {ym} {threshold} 计算超时")


def segs(d):
    return [(s["start_ts"], s["end_ts"], s["value"], s["length"])
            for s in (d["report"]["segments"] if d["report"] else [])]


def gt(rows):
    return [(s["start_ts"].isoformat(), s["end_ts"].isoformat(),
             s["value"], s["length"]) for s in rows]


# ------------------------------------------------------------------ 用例
def test_rule():
    print("\n== 用例1：连段规则 / 月界 / 阈值（QCT1）==", flush=True)
    cleanup("QCT1", "QCT2")
    ym = "2026-09"
    # 0..4 等值 5（5条）；6..9 等值 8（4条，中间无缺）；11 单点；
    # 13:30 等非整点；月末 23:00 与次月 00:00 同值但不跨月
    seed("QCT1", ym, [(h, 0, 5) for h in range(5)]
         + [(h, 0, 8) for h in range(6, 10)]
         + [(11, 0, 9)]
         + [(13, 30, 9), (14, 30, 9), (15, 30, 9)])
    seed("QCT1", "2026-10", [(0, 0, 5)])
    with db() as c:
        c.execute("INSERT INTO observations VALUES ('QCT1', %s, 5) ON CONFLICT DO NOTHING",
                  (month_lo(ym) - timedelta(hours=2),))
        c.execute("INSERT INTO observations VALUES ('QCT1', %s, 5) ON CONFLICT DO NOTHING",
                  (month_lo(ym) - timedelta(hours=1),))
    check("重算启动", recompute("QCT1", ym, 3).ok)
    d = wait_ready("QCT1", ym, 3)
    check("t=3 报告有效", d["is_current"] and d["report"]["status"] == "ready")
    check("t=3 结果 == 独立地面真值",
          segs(d) == gt(ground_truth("QCT1", ym, 3)), str(segs(d)))
    exp3 = [
        ("2026-09-01T00:00:00+00:00", "2026-09-01T04:00:00+00:00", 5, 5),
        ("2026-09-01T06:00:00+00:00", "2026-09-01T09:00:00+00:00", 8, 4),
    ]
    check("t=3 具体段（含非整点/月界截断）", segs(d) == exp3, str(segs(d)))
    check("次月同值不并入本月",
          all(s[0].startswith("2026-09") for s in segs(d)))

    recompute("QCT1", ym, 4)
    d4 = wait_ready("QCT1", ym, 4)
    check("t=4 两段都达标(4>=4)",
          segs(d4) == exp3, str(segs(d4)))
    recompute("QCT1", ym, 5)
    d5 = wait_ready("QCT1", ym, 5)
    check("t=5 只剩 5 条那段",
          segs(d5) == [("2026-09-01T00:00:00+00:00",
                        "2026-09-01T04:00:00+00:00", 5, 5)], str(segs(d5)))
    recompute("QCT1", ym, 6)
    d6 = wait_ready("QCT1", ym, 6)
    check("t=6 无达标段", segs(d6) == [])
    check("阈值 2 被拒",
          recompute("QCT1", ym, 2).status_code in (400, 422))
    check("阈值 13 被拒(422/400)",
          recompute("QCT1", ym, 13).status_code in (400, 422))


def test_stale_isolation():
    print("\n== 用例2：直连 SQL 变更 -> 过期；改回不复活；其他站月不受影响 ==",
          flush=True)
    ym = "2026-09"
    # QCT1 t=3 当前报告
    recompute("QCT1", ym, 3)
    wait_ready("QCT1", ym, 3)
    check("QCT1 报告初始有效", report("QCT1", ym, 3)["is_current"])
    # QCT2 另一站报告
    seed("QCT2", ym, [(h, 0, 42) for h in range(4)])
    recompute("QCT2", ym, 3)
    wait_ready("QCT2", ym, 3)
    check("QCT2 报告初始有效", report("QCT2", ym, 3)["is_current"])

    with db() as c:
        c.execute("INSERT INTO observations VALUES ('QCT1', %s, 123) ON CONFLICT DO NOTHING",
                  (month_lo(ym) + timedelta(hours=20),))
        c.execute("DELETE FROM observations WHERE station_id='QCT1' AND ts=%s",
                  (month_lo(ym) + timedelta(hours=20),))
        c.execute("UPDATE observations SET value=999 WHERE station_id='QCT1' AND ts=%s",
                  (month_lo(ym),))
        c.execute("UPDATE observations SET value=5 WHERE station_id='QCT1' AND ts=%s",
                  (month_lo(ym),))  # 改回
    d = report("QCT1", ym, 3)
    check("直连增删改后 QCT1 报告过期", not d["is_current"]
          and d["report"]["status"] == "stale")
    check("改回后仍为过期（不复活）", d["report"]["status"] == "stale")
    check("旧报告内容仍可查看", len(d["report"]["segments"]) == 2)
    d2 = report("QCT2", ym, 3)
    check("其他站 QCT2 不受影响", d2["is_current"] and d2["report"]["status"] == "ready")
    # 其他月不受影响：9 月的变更不应给无关月份留下代次或报告
    other = report("QCT1", "2026-07", 3)
    check("同站无关月份无报告且代次为 0",
          other["report"] is None and other["watch_gen"] == 0,
          f"gen={other['watch_gen']}")


def test_slow_interleave():
    print("\n== 用例3：慢计算期间采集照常提交；相关变化 -> 作废且不覆盖旧报告 ==",
          flush=True)
    ym = "2026-09"
    recompute("QCT2", ym, 3)
    base = wait_ready("QCT2", ym, 3)
    check("基准报告有效", base["is_current"])
    base_id = base["report"]["id"]
    base_segs = segs(base)

    check("慢计算启动", recompute("QCT2", ym, 3, delay_ms=3000).ok)
    time.sleep(0.5)
    # 计算进行中，采集写入必须能立即提交
    t0 = time.time()
    with db() as c:
        c.execute("UPDATE observations SET value=43 WHERE station_id='QCT2' AND ts=%s",
                  (month_lo(ym),))
    commit_ms = (time.time() - t0) * 1000
    check("慢计算期间直连写入立即提交(<1s)", commit_ms < 1000,
          f"{commit_ms:.0f}ms")
    check("写入使旧报告过期", not report("QCT2", ym, 3)["is_current"])
    d = wait_ready("QCT2", ym, 3, timeout=20)
    check("本次结果标记 superseded", d["latest_attempt"]["status"] == "superseded",
          d["latest_attempt"]["error"])
    check("旧报告未被覆盖（仍为 #%d 及其内容）" % base_id,
          d["report"]["id"] == base_id and segs(d) == base_segs)
    check("无新的 current 报告", not d["is_current"])
    # 重算成功后可得到反映新值的报告
    recompute("QCT2", ym, 3)
    d2 = wait_ready("QCT2", ym, 3)
    check("重算成功并成为 current",
          d2["is_current"] and d2["report"]["status"] == "ready")
    check("新报告与独立地面真值一致",
          segs(d2) == gt(ground_truth("QCT2", ym, 3)), str(segs(d2)))


def test_failure_and_crash():
    print("\n== 用例4：计算失败 / 进程退出 -> 旧报告可看 -> 可重算 ==", flush=True)
    ym = "2026-09"
    base = wait_ready("QCT2", ym, 3)
    check("存在有效基准报告", base["is_current"])
    base_id, base_segs = base["report"]["id"], segs(base)

    check("故障注入计算启动", recompute("QCT2", ym, 3,
                                    fail_after_snapshot=True).ok)
    d = wait_ready("QCT2", ym, 3)
    check("本次记录 failed", d["latest_attempt"]["status"] == "failed")
    check("失败不覆盖旧报告",
          d["report"]["id"] == base_id and segs(d) == base_segs)

    # 进程硬退出：慢计算进行中杀掉应用
    check("慢计算启动", recompute("QCT2", ym, 3, delay_ms=8000).ok)
    time.sleep(0.8)
    crash_app()
    check("进程已退出", _app is None)
    start_app()
    d = wait_healthy_report = report("QCT2", ym, 3)
    check("重启后旧报告仍可查看",
          d["report"]["id"] == base_id and segs(d) == base_segs)
    check("中断的计算记录为 failed",
          d["latest_attempt"]["status"] == "failed")
    check("启动时无残留 computing", not d["computing"])
    recompute("QCT2", ym, 3)
    d = wait_ready("QCT2", ym, 3)
    check("重启后可重算成功",
          d["is_current"] and d["report"]["status"] == "ready")


def test_migration_boundary():
    print("\n== 用例5：在线月迁移不得使报告过期或改变判定 ==", flush=True)
    cleanup("QCT3")
    ym = "2026-08"
    seed("QCT3", ym, [(h, 0, 7) for h in range(0, 6)]
         + [(h, 0, 11) for h in range(10, 16)]
         + [(h, 0, 12) for h in range(20, 40)])
    recompute("QCT3", ym, 3)
    before = wait_ready("QCT3", ym, 3)
    check("迁移前报告有效", before["is_current"])
    before_segs, before_gen = segs(before), before["watch_gen"]

    r = requests.post(f"{BASE}/api/migrate", json={
        "month": ym, "batch_size": 50, "sleep_ms": 10}, timeout=10)
    check("迁移启动", r.ok, r.text[:200])
    deadline = time.time() + 180
    status = None
    while time.time() < deadline:
        with db() as c:
            row = c.execute("SELECT status FROM migration_runs WHERE month=%s",
                            (month_lo(ym),)).fetchone()
        status = row[0] if row else None
        if status in ("done", "failed"):
            break
        time.sleep(0.3)
    check("迁移完成", status == "done", f"status={status}")
    with db() as c:
        part = c.execute("SELECT tableoid::regclass::text FROM observations"
                         " WHERE station_id='QCT3' LIMIT 1").fetchone()[0]
    check("数据已在月分区", part == "observations_2026_08", part)
    after = report("QCT3", ym, 3)
    check("迁移后报告仍未过期", after["is_current"]
          and after["report"]["status"] == "ready")
    check("迁移后判定不变", segs(after) == before_segs, str(segs(after)))
    check("迁移后变更代次不变", after["watch_gen"] == before_gen,
          f"{before_gen}->{after['watch_gen']}")

    # 迁移后直连写月分区（绕过父表）仍须使报告过期
    with db() as c:
        c.execute("UPDATE observations_2026_08 SET value=77"
                  " WHERE station_id='QCT3' AND ts=%s", (month_lo(ym),))
    d = report("QCT3", ym, 3)
    check("迁移后直连写月分区仍使报告过期",
          not d["is_current"] and d["report"]["status"] == "stale")
    recompute("QCT3", ym, 3)
    d = wait_ready("QCT3", ym, 3)
    check("迁移后重算与地面真值一致",
          d["is_current"] and segs(d) == gt(ground_truth("QCT3", ym, 3)))


def main():
    os.makedirs("/tmp/pg-runtime", exist_ok=True)
    with db() as c:
        c.execute("SELECT 1")
    start_app()
    try:
        test_rule()
        test_stale_isolation()
        test_slow_interleave()
        test_failure_and_crash()
        test_migration_boundary()
    finally:
        crash_app()
        cleanup("QCT1", "QCT2", "QCT3")

    print("\n" + "=" * 60)
    if _failures:
        print(f"质控自测失败 {len(_failures)} 项：")
        for f in _failures:
            print("  -", f)
        sys.exit(1)
    print("质控自测全部通过 ✓")


if __name__ == "__main__":
    main()
