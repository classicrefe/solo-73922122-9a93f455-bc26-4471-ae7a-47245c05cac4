#!/usr/bin/env python3
"""
端到端自测（真实 PostgreSQL + 真实 HTTP API + 真实进程崩溃）：

  1. 慢速复制 + 并发直接 SQL 增删改 → 切换后父表内容与期望逐行一致
  2. 一次只迁一个月（并发启动第二个 → 409）
  3. 已完成月份再次迁移 → 409 且数据不变
  4. copying / finalizing 阶段故障注入 → 父表完整可查 → 重试完成
  5. SIGKILL 应用进程（复制进行中）→ 父表仍完整可查可写 → 重启后重试完成
  6. 收尾检查：默认分区无残留、无重复主键、触发器/增量清理干净

运行：DATABASE_URL=... python3 scripts/selftest.py
（脚本自己拉起/杀掉 uvicorn 进程；要求 DATABASE_URL 指向已初始化的库）
"""

from __future__ import annotations

import os
import random
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg
import requests

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8010")
DSN = os.environ.get(
    "DATABASE_URL", "postgresql://envmon@127.0.0.1:5544/envmon"
)
APP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app"))
APP_PORT = int(os.environ.get("APP_PORT", "8010"))
STATIONS = [f"ST{i:03d}" for i in range(1, 9)]

_failures: list[str] = []


def check(name: str, cond: bool, detail: str = ""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail else ""), flush=True)
    if not cond:
        _failures.append(f"{name}: {detail}")


def db():
    return psycopg.connect(DSN, autocommit=True, options="-c timezone=UTC")


def month_range(month: str):
    lo = datetime.fromisoformat(month + "-01").replace(tzinfo=timezone.utc)
    hi = (lo.replace(day=28) + timedelta(days=8)).replace(day=1)
    return lo, hi


def snapshot(month: str) -> dict:
    """父表该月全部内容（ground truth，直接 SQL）。"""
    lo, hi = month_range(month)
    with db() as c:
        rows = c.execute(
            "SELECT station_id, ts, value FROM observations"
            " WHERE ts >= %s AND ts < %s",
            (lo, hi),
        ).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def default_count(month: str) -> int:
    lo, hi = month_range(month)
    with db() as c:
        return c.execute(
            "SELECT count(*) FROM observations_default WHERE ts >= %s AND ts < %s",
            (lo, hi),
        ).fetchone()[0]


def run_row(month: str):
    with db() as c:
        return c.execute(
            "SELECT status, copied_rows, applied_delta, error"
            " FROM migration_runs WHERE month = %s::date", (month + "-01",)
        ).fetchone()


def partition_name(month: str) -> str:
    return "observations_" + month.replace("-", "_")


def partition_exists(month: str) -> bool:
    with db() as c:
        return bool(
            c.execute(
                "SELECT 1 FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid"
                " WHERE i.inhparent = 'observations'::regclass AND c.relname = %s",
                (partition_name(month),),
            ).fetchone()
        )


def partition_count(month: str) -> int:
    with db() as c:
        return c.execute(
            f"SELECT count(*) FROM {partition_name(month)}"
        ).fetchone()[0]


class ChaosWriter(threading.Thread):
    """独立采集连接：直接 SQL 随机 新增/更正/删除，并维护期望结果。"""

    def __init__(self, month: str, expected: dict):
        super().__init__(daemon=True)
        self.month = month
        self.expected = expected
        self.stop_event = threading.Event()
        self.ops = 0
        self.errors: list[str] = []
        self.lo, self.hi = month_range(month)

    def run(self):
        try:
            conn = psycopg.connect(DSN, autocommit=True,
                                   options="-c timezone=UTC")
        except Exception as e:  # noqa: BLE001
            self.errors.append(f"connect: {e!r}")
            return
        with conn:
            while not self.stop_event.is_set():
                op = random.choices(["insert", "update", "delete"],
                                    weights=[5, 3, 2])[0]
                try:
                    self._do(conn, op)
                    self.ops += 1
                except Exception as e:  # noqa: BLE001
                    self.errors.append(f"{op}: {e!r}")
                time.sleep(random.uniform(0.01, 0.06))

    def _do(self, conn, op: str):
        if op == "insert":
            station = random.choice(STATIONS)
            span = int((self.hi - self.lo).total_seconds())
            ts = self.lo + timedelta(seconds=random.randrange(span))
            value = random.randint(0, 100)
            row = conn.execute(
                "INSERT INTO observations (station_id, ts, value)"
                " VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
                " RETURNING station_id, ts, value",
                (station, ts, value),
            ).fetchone()
            if row:
                self.expected[(row[0], row[1])] = row[2]
        elif op == "update":
            row = conn.execute(
                "SELECT station_id, ts FROM observations"
                " WHERE ts >= %s AND ts < %s ORDER BY random() LIMIT 1",
                (self.lo, self.hi),
            ).fetchone()
            if row:
                value = random.randint(0, 100)
                conn.execute(
                    "UPDATE observations SET value = %s"
                    " WHERE station_id = %s AND ts = %s",
                    (value, row[0], row[1]),
                )
                self.expected[(row[0], row[1])] = value
        else:
            row = conn.execute(
                "SELECT station_id, ts FROM observations"
                " WHERE ts >= %s AND ts < %s ORDER BY random() LIMIT 1",
                (self.lo, self.hi),
            ).fetchone()
            if row:
                conn.execute(
                    "DELETE FROM observations WHERE station_id = %s AND ts = %s",
                    (row[0], row[1]),
                )
                self.expected.pop((row[0], row[1]), None)


# ------------------------------------------------------------------ app 进程
_app_proc: subprocess.Popen | None = None


def start_app() -> subprocess.Popen:
    global _app_proc
    logf = open("/tmp/pg-runtime/app.log", "ab")
    _app_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app",
         "--host", "127.0.0.1", "--port", str(APP_PORT)],
        cwd=APP_DIR, stdout=logf, stderr=subprocess.STDOUT,
        env={**os.environ, "DATABASE_URL": DSN},
    )
    wait_healthy()
    return _app_proc


def crash_app():
    """通过调试端点让应用硬退出（os._exit，等价于宕机/SIGKILL 的效果）。"""
    global _app_proc
    try:
        requests.post(f"{BASE}/api/debug/crash", timeout=3)
    except requests.RequestException:
        pass
    if _app_proc:
        try:
            _app_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    _app_proc = None
    # 等端口彻底拒绝连接
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            requests.get(f"{BASE}/api/state", timeout=1)
            time.sleep(0.3)
        except requests.RequestException:
            return
    raise RuntimeError("应用进程未能退出")


def wait_healthy(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{BASE}/api/state", timeout=2).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(0.3)
    raise RuntimeError("应用未在预期时间内就绪")


def api_migrate(month: str, **kw) -> requests.Response:
    body = {"month": month, "batch_size": 200, "sleep_ms": 80}
    body.update(kw)
    return requests.post(f"{BASE}/api/migrate", json=body, timeout=10)


def wait_status(month: str, want: set[str], timeout=180) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = run_row(month)
        if row and row[0] in want:
            return row[0]
        time.sleep(0.4)
    raise RuntimeError(f"{month} 等待状态 {want} 超时，当前 {run_row(month)}")


def verify_month(month: str, expected: dict, label: str):
    """迁移完成后的一致性校验（全部用直接 SQL 独立验证）。"""
    got = snapshot(month)
    check(f"{label}: 父表内容与期望逐行一致", got == expected,
          f"父表 {len(got)} 行 vs 期望 {len(expected)} 行"
          + ("" if got == expected else
             f"，差异样例 {list((got.items() ^ expected.items()))[:3]}"))
    check(f"{label}: 默认分区无残留", default_count(month) == 0,
          f"剩余 {default_count(month)}")
    check(f"{label}: 月分区已挂载", partition_exists(month))
    if partition_exists(month):
        check(f"{label}: 分区行数 == 父表行数",
              partition_count(month) == len(got),
              f"{partition_count(month)} vs {len(got)}")
    with db() as c:
        dup = c.execute(
            "SELECT count(*) - count(DISTINCT (station_id, ts))"
            " FROM observations WHERE ts >= %s AND ts < %s",
            month_range(month),
        ).fetchone()[0]
        trg = c.execute(
            "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'obs_delta%'"
        ).fetchone()[0]
        delta = c.execute(
            "SELECT count(*) FROM migration_delta WHERE month = %s::date",
            (month + "-01",),
        ).fetchone()[0]
    check(f"{label}: 父表主键无重复", dup == 0, f"重复 {dup}")
    check(f"{label}: 增量触发器已清理", trg == 0, f"剩余 {trg}")
    check(f"{label}: 增量日志已清理", delta == 0, f"剩余 {delta}")
    r = requests.get(f"{BASE}/api/verify", params={"month": month}, timeout=10)
    check(f"{label}: /api/verify 判定一致", r.ok and r.json()["consistent"],
          r.text[:200])


# ---------------------------------------------------------------------- 用例
def test_happy_path(month: str):
    print(f"\n== 用例1：慢速复制 + 并发直接SQL增删改（{month}）==", flush=True)
    expected = snapshot(month)
    base_rows = len(expected)
    r = api_migrate(month, batch_size=200, sleep_ms=120)
    check("迁移启动成功", r.ok, r.text[:200])
    # 一次只迁一个月
    r2 = api_migrate(NEXT_MONTHS[1])
    check("迁移进行中再启动另一个月被拒绝(409)", r2.status_code == 409,
          f"rc={r2.status_code}")
    writer = ChaosWriter(month, expected)
    writer.start()
    status = wait_status(month, {"done", "failed"})
    writer.stop_event.set()
    writer.join(timeout=10)
    check("迁移完成", status == "done", f"status={status} err={run_row(month)}")
    check("并发写入无错误", not writer.errors,
          "; ".join(writer.errors[:3]))
    print(f"  并发写入 {writer.ops} 次；迁移前 {base_rows} 行 → 期望 {len(expected)} 行")
    verify_month(month, expected, "用例1")
    # 幂等：已完成月份再次操作不再搬迁
    before = snapshot(month)
    r3 = api_migrate(month)
    check("已完成月份再次迁移被拒绝(409)", r3.status_code == 409, f"rc={r3.status_code}")
    check("重复操作后数据不变", snapshot(month) == before)


def test_fail_and_retry(month: str, fail_at: str):
    print(f"\n== 用例：{fail_at} 阶段故障注入后重试（{month}）==", flush=True)
    expected = snapshot(month)
    r = api_migrate(month, fail_at=fail_at, sleep_ms=40)
    check("迁移启动成功", r.ok, r.text[:200])
    status = wait_status(month, {"failed", "done"}, timeout=120)
    check("故障注入后状态为 failed", status == "failed", f"status={status}")
    # 父表在失败后仍完整可查
    got = snapshot(month)
    check("失败后父表内容完整不变", got == expected,
          f"{len(got)} vs {len(expected)}")
    check("失败后默认分区仍持有全部数据", default_count(month) == len(expected))
    # 重试（无故障注入）+ 并发写入
    r2 = api_migrate(month, sleep_ms=60)
    check("重试启动成功", r2.ok, r2.text[:200])
    writer = ChaosWriter(month, expected)
    writer.start()
    status = wait_status(month, {"done", "failed"})
    writer.stop_event.set()
    writer.join(timeout=10)
    check("重试后迁移完成", status == "done", f"status={status}")
    check("重试期间并发写入无错误", not writer.errors,
          "; ".join(writer.errors[:3]))
    verify_month(month, expected, f"重试({fail_at})")


def test_process_kill(month: str):
    print(f"\n== 用例：复制进行中应用进程硬退出（{month}）==", flush=True)
    expected = snapshot(month)
    r = api_migrate(month, batch_size=150, sleep_ms=150)
    check("迁移启动成功", r.ok, r.text[:200])
    # 等复制真正开始
    deadline = time.time() + 60
    while time.time() < deadline:
        row = run_row(month)
        if row and row[1] and row[1] > 0:
            break
        time.sleep(0.2)
    row = run_row(month)
    check("复制已开始（copied_rows>0）", bool(row and row[1] and row[1] > 0),
          f"run={row}")
    crash_app()
    time.sleep(1)
    # 应用已死：父表仍完整可查，且采集写入（直接 SQL）仍可提交
    got = snapshot(month)
    check("进程被杀后父表内容完整", got == expected,
          f"{len(got)} vs {len(expected)}")
    probe = ChaosWriter(month, expected)
    probe.start()
    time.sleep(2)
    probe.stop_event.set()
    probe.join(timeout=10)
    check("应用停机期间直接 SQL 写入正常", probe.ops > 0 and not probe.errors,
          f"ops={probe.ops} err={probe.errors[:2]}")
    # 重启应用，重试迁移
    start_app()
    r2 = api_migrate(month, sleep_ms=60)
    check("重启后重试启动成功", r2.ok, r2.text[:200])
    writer = ChaosWriter(month, expected)
    writer.start()
    status = wait_status(month, {"done", "failed"})
    writer.stop_event.set()
    writer.join(timeout=10)
    check("崩溃恢复后迁移完成", status == "done", f"status={status}")
    verify_month(month, expected, "崩溃恢复")


def main():
    global NEXT_MONTHS
    random.seed(20261116)
    with db() as c:
        months = [r[0].isoformat()[:7] for r in c.execute(
            "SELECT (date_trunc('month', ts AT TIME ZONE 'UTC'))::date AS m,"
            "       count(*) FROM observations_default"
            " GROUP BY 1 ORDER BY 1"
        )]
    check("种子数据至少 4 个月份", len(months) >= 4, f"months={months}")
    NEXT_MONTHS = months
    m1, m2, m3, m4 = months[:4]

    start_app()
    try:
        r = requests.get(f"{BASE}/api/state", timeout=10)
        check("GET /api/state 可用", r.ok)
        test_happy_path(m1)
        test_fail_and_retry(m2, "copying")
        test_fail_and_retry(m3, "finalizing")
        test_process_kill(m4)
    finally:
        crash_app()

    print("\n" + "=" * 60)
    if _failures:
        print(f"自测失败 {len(_failures)} 项：")
        for f in _failures:
            print("  -", f)
        sys.exit(1)
    print("全部自测通过 ✓")


if __name__ == "__main__":
    main()
