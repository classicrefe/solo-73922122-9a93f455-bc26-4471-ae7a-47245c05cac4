#!/usr/bin/env bash
# 本地开发/演示启动脚本（无 docker 时使用）：
#   1) 启动 PostgreSQL（已初始化则直接启动）
#   2) 初始化库表与种子数据
#   3) 启动 FastAPI 应用（前台，Ctrl-C 停止）
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PGHOME=${PGHOME:-/tmp/pg-runtime/pgsql}
PGDATA=${PGDATA:-/tmp/pg-runtime/data}
PGPORT=${PGPORT:-5544}
PY=${PY:-/tmp/venv/bin/python}
APP_PORT=${APP_PORT:-8000}
DB=${DB:-envmon}

export PATH="$PGHOME/bin:$PATH"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "== initdb =="
  initdb -D "$PGDATA" -U envmon --auth=trust -E UTF8 --locale=C
  echo "port = $PGPORT" >> "$PGDATA/postgresql.conf"
  echo "listen_addresses = '127.0.0.1'" >> "$PGDATA/postgresql.conf"
fi

pg_ctl -D "$PGDATA" -l /tmp/pg-runtime/pg.log -w start || true

"$PY" - "$PGPORT" "$DB" "$ROOT/db/init.sql" <<'EOF'
import sys, pathlib, psycopg
port, db, init_sql = sys.argv[1], sys.argv[2], pathlib.Path(sys.argv[3])
base = f"postgresql://envmon@127.0.0.1:{port}"
with psycopg.connect(base + "/postgres", autocommit=True) as c:
    if not c.execute("SELECT 1 FROM pg_database WHERE datname=%s", (db,)).fetchone():
        c.execute(f"CREATE DATABASE {db}")
with psycopg.connect(base + f"/{db}", autocommit=True) as c:
    c.execute(init_sql.read_text())
print("db ready")
EOF

cd "$ROOT/app"
DATABASE_URL="postgresql://envmon@127.0.0.1:$PGPORT/$DB" \
  exec "$PY" -m uvicorn main:app --host 0.0.0.0 --port "$APP_PORT"
