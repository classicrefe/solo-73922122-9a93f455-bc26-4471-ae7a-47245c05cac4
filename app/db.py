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


def connect() -> psycopg.Connection:
    """新建独立连接（采集写入演示也用它，模拟独立采集连接）。"""
    return psycopg.connect(
        DATABASE_URL, autocommit=True, options="-c timezone=UTC"
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
