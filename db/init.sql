-- ============================================================================
-- 环境监测数仓 · 观测表分区迁移演示 — 数据库初始化
-- ----------------------------------------------------------------------------
--  * observations  : 父表，按 UTC 时刻 RANGE 分区，主键 (station_id, ts)
--  * observations_default : 唯一的默认分区，历史观测最初全部落在这里
--  * migration_runs  : 每个月份迁移的状态机记录（幂等/断点续迁的依据）
--  * migration_delta : 迁移期间由触发器捕获的增量变更（新增/更正/删除）
-- ============================================================================

CREATE TABLE IF NOT EXISTS observations (
    station_id  text          NOT NULL,
    ts          timestamptz   NOT NULL,
    value       integer       NOT NULL,
    PRIMARY KEY (station_id, ts)
) PARTITION BY RANGE (ts);

CREATE TABLE IF NOT EXISTS observations_default
    PARTITION OF observations DEFAULT;

-- 迁移状态机：idle → preparing → copying → applying → finalizing → done
-- failed 状态仅作展示；重试时从断点继续（影子表/增量表均保留）。
CREATE TABLE IF NOT EXISTS migration_runs (
    month           date        PRIMARY KEY,   -- 月份首日（UTC）
    status          text        NOT NULL DEFAULT 'preparing',
    total_estimate  bigint      NOT NULL DEFAULT 0,
    copied_rows     bigint      NOT NULL DEFAULT 0,
    applied_delta   bigint      NOT NULL DEFAULT 0,
    last_station    text,
    last_ts         timestamptz,
    error           text,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- 增量变更日志：迁移期间默认分区上该月数据的 INSERT/UPDATE/DELETE
CREATE TABLE IF NOT EXISTS migration_delta (
    seq         bigserial PRIMARY KEY,
    month       date        NOT NULL,
    op          text        NOT NULL CHECK (op IN ('insert','update','delete')),
    station_id  text        NOT NULL,
    ts          timestamptz NOT NULL,
    value       integer
);
CREATE INDEX IF NOT EXISTS migration_delta_month_seq
    ON migration_delta (month, seq);

-- ============================================================================
-- 质控报告（测值持续不变）所需元数据
-- ============================================================================
-- 变更代次：每站每月一行，observations 上的触发器在每次逻辑变更时 +1。
CREATE TABLE IF NOT EXISTS qc_watch (
    station_id  text        NOT NULL,
    month       date        NOT NULL,
    gen         bigint      NOT NULL DEFAULT 0,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, month)
);

-- 报告 = 不可变视图。每次成功计算登记一行；is_current 标记当前有效视图，
-- 过期只置 stale_at/status（内容保留，仍可查看）。
CREATE TABLE IF NOT EXISTS qc_reports (
    id           bigserial   PRIMARY KEY,
    station_id   text        NOT NULL,
    month        date        NOT NULL,
    threshold    integer     NOT NULL CHECK (threshold BETWEEN 3 AND 12),
    status       text        NOT NULL,     -- computing/ready/stale/superseded/failed
    is_current   boolean     NOT NULL DEFAULT false,
    gen          bigint      NOT NULL DEFAULT 0,  -- 计算快照的变更代次
    stale_at     timestamptz,
    stale_reason text,
    error        text,
    computed_at  timestamptz,
    started_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS qc_reports_one_current
    ON qc_reports (station_id, month, threshold) WHERE is_current;
CREATE INDEX IF NOT EXISTS qc_reports_lookup
    ON qc_reports (station_id, month, threshold, updated_at DESC);

-- 一次计算产出的一段连续等值观测（start_ts 升序即结果顺序）。
CREATE TABLE IF NOT EXISTS qc_segments (
    report_id   bigint      NOT NULL REFERENCES qc_reports(id) ON DELETE CASCADE,
    start_ts    timestamptz NOT NULL,
    end_ts      timestamptz NOT NULL,
    value       integer     NOT NULL,
    length      integer     NOT NULL,
    PRIMARY KEY (report_id, start_ts)
);

-- ============================================================================
-- 演示种子数据：最近 6 个自然月（UTC），8 个站点，每小时一条整数测值。
-- 全部写入默认分区 —— 即"历史观测 initially 都在默认分区"。
-- ============================================================================
INSERT INTO observations (station_id, ts, value)
SELECT 'ST' || lpad(s::text, 3, '0'),
       date_trunc('hour', t),
       (20 + 60 * random())::int
FROM generate_series(1, 8) AS s
CROSS JOIN generate_series(
        date_trunc('month', now()) - interval '5 months',
        now() - interval '1 hour',
        interval '1 hour'
    ) AS t
ON CONFLICT (station_id, ts) DO NOTHING;

-- ============================================================================
-- 质控变更跟踪触发器：父子表同用（CREATE TRIGGER ... ON observations 会自动
-- 传播到之后 ATTACH 的月分区；因此在线迁移不会漏记逻辑变更）。
-- 迁移的物理搬迁/切换在事务内 SET LOCAL qc.suppress_watch='on' 跳过代次递增。
-- ============================================================================
CREATE OR REPLACE FUNCTION qc_watch_fn() RETURNS trigger AS $qc$
DECLARE
    m_new date;
    m_old date;
BEGIN
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
           SET is_current = false, status = 'stale',
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
           SET is_current = false, status = 'stale',
               stale_at = clock_timestamp(),
               stale_reason = '该站该月观测发生变化',
               updated_at = clock_timestamp()
         WHERE station_id = OLD.station_id AND month = m_old AND is_current;
    END IF;

    RETURN COALESCE(NEW, OLD);
END
$qc$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS qc_watch_trg ON observations;
CREATE TRIGGER qc_watch_trg AFTER INSERT OR UPDATE OR DELETE ON observations
    FOR EACH ROW EXECUTE FUNCTION qc_watch_fn();
