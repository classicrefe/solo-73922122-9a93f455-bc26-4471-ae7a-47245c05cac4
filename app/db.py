"""数据库连接与进程内日志缓冲。"""

from __future__ import annotations

import os
import threading
from collections import deque
from datetime import datetime, timezone

import psycopg

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://envmon:envmon@localhost:5432/envmon"
)

_lock = threading.Lock()
_logs: deque[dict] = deque(maxlen=500)
_seq = 0


def connect(autocommit: bool = True) -> psycopg.Connection:
    """新建独立连接（采集写入演示也用它，模拟独立采集连接）。

    autocommit=False 用于需要显式事务（快照一致性/行锁）的场景。
    """
    return psycopg.connect(
        DATABASE_URL, autocommit=autocommit, options="-c timezone=UTC"
    )


def log(msg: str):
    global _seq
    with _lock:
        _seq += 1
        _logs.append(
            {
                "seq": _seq,
                "at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "msg": msg,
            }
        )
    print(f"[log] {msg}", flush=True)


def logs_since(seq: int) -> list[dict]:
    with _lock:
        return [e for e in _logs if e["seq"] > seq]


def ensure_meta_tables():
    """元数据表兜底创建（init.sql 已建则跳过）。"""
    with connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_runs (
                month           date        PRIMARY KEY,
                status          text        NOT NULL DEFAULT 'preparing',
                total_estimate  bigint      NOT NULL DEFAULT 0,
                copied_rows     bigint      NOT NULL DEFAULT 0,
                applied_delta   bigint      NOT NULL DEFAULT 0,
                last_station    text,
                last_ts         timestamptz,
                error           text,
                updated_at      timestamptz NOT NULL DEFAULT now(),
                created_at      timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_delta (
                seq         bigserial PRIMARY KEY,
                month       date        NOT NULL,
                op          text        NOT NULL,
                station_id  text        NOT NULL,
                ts          timestamptz NOT NULL,
                value       integer
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS migration_delta_month_seq "
            "ON migration_delta (month, seq)"
        )
        # ---- 质控（测值持续不变）报告元数据 ---------------------------------
        # 报告本身是一张不可变的"视图"：每次成功计算登记一行，内容存 qc_segments；
        # is_current 标记当前有效视图，过期只置 stale_at（不删内容，仍可查看）。
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS qc_reports (
                id          bigserial   PRIMARY KEY,
                station_id  text        NOT NULL,
                month       date        NOT NULL,      -- UTC 月首日
                threshold   integer     NOT NULL CHECK (threshold BETWEEN 3 AND 12),
                status      text        NOT NULL,      -- computing / ready / stale / failed
                is_current  boolean     NOT NULL DEFAULT false,
                gen         bigint      NOT NULL DEFAULT 0,  -- 计算快照的变更代次
                stale_at    timestamptz,
                stale_reason text,
                error       text,
                computed_at timestamptz,
                started_at  timestamptz NOT NULL DEFAULT now(),
                updated_at  timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS qc_reports_one_current "
            "ON qc_reports (station_id, month, threshold) WHERE is_current"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS qc_reports_lookup "
            "ON qc_reports (station_id, month, threshold, updated_at DESC)"
        )
        # 旧卷升级兜底：早期 qc_reports 没有 gen 列
        conn.execute("ALTER TABLE qc_reports ADD COLUMN IF NOT EXISTS gen bigint"
                     " NOT NULL DEFAULT 0")
        # 一次计算产出的一段连续等值观测（start_ts 升序即结果顺序）
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS qc_segments (
                report_id   bigint      NOT NULL
                            REFERENCES qc_reports(id) ON DELETE CASCADE,
                start_ts    timestamptz NOT NULL,
                end_ts      timestamptz NOT NULL,
                value       integer     NOT NULL,
                length      integer     NOT NULL,
                PRIMARY KEY (report_id, start_ts)
            )
            """
        )
        # 变更代次：每站每月一行。observations 上的触发器每次逻辑变更 +1，
        # 只增不减 —— 值改回也不会让旧报告"复活"。计算时用它做快照复核。
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS qc_watch (
                station_id  text        NOT NULL,
                month       date        NOT NULL,
                gen         bigint      NOT NULL DEFAULT 0,
                updated_at  timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (station_id, month)
            )
            """
        )
        # 启动时把"上次进程退出时正在计算"的记录标记为失败（不覆盖旧报告）
        conn.execute(
            """
            UPDATE qc_reports
               SET status = 'failed',
                   error = coalesce(error, '计算进程已退出'),
                   updated_at = now()
             WHERE status = 'computing'
            """
        )
