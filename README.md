# 环境监测数仓 · 默认分区 → 月分区在线迁移 + 质控异常报告

## 一、在线月分区迁移

把 `observations` 父表**默认分区**中的历史观测，按自然月（UTC）在线搬迁到新建的月分区。
复制期间采集链路（独立连接）对该月的**新增 / 测值更正 / 删除**不中断；
仅最终切换瞬间短暂锁表；失败或进程崩溃后可断点续迁；已完成月份重复操作不再搬迁。

## 二、质控异常报告（测值持续不变）

为质控员找出"测值持续不变"的站点区段，用于安排仪器检修。
网页（⑥ 区）按 **站号 + UTC 月份 + 连续次数阈值(3～12)** 重算并查看报告。

判定规则：

- 只有**相邻整点**（UTC，分/秒/微秒为 0）且**测值相同**的观测才连成一段；
- 下一条必须是上一条 +1 小时，否则断段；非整点观测不参与；
- 只在 `[UTC 月首, 次月首)` 内连段，**月界截断**，跨月不连；
- 段内观测条数 `>= 阈值` 才进入报告；每段列出起止时刻、测值、条数。

一致性与并发保证：

| 要求 | 实现 |
|---|---|
| 报告取自同一已提交视图 | 计算时在**一个 REPEATABLE READ 只读事务**里读变更代次与全部观测；写入只在最后短事务完成 |
| 直连 SQL 增删改（即使改回）使旧报告过期 | `observations` 上的行级触发器把该站该月 `qc_watch.gen` 只增不减，并把当前报告置 `stale`；值改回代次也不回退 |
| 其他站/月变化不影响 | 代次与过期都按 `(station_id, month)` 精确限定 |
| 慢计算时采集照常提交 | 快照读取后立即结束只读事务；慢计算只 sleep，不持锁 |
| 相关观测已变化则本次不得覆盖旧报告 | 发布前 `SELECT ... FOR UPDATE` 复核代次；不同则本次标记 `superseded`，旧报告原样保留 |
| 计算失败/进程退出仍可看旧报告并重算 | 失败/启动时中断的 `computing` 记为 `failed`；旧报告与 segment 不删，读取时回退展示 |
| 在线月迁移不使报告过期、不改变判定 | 迁移最终切换事务内 `SET LOCAL qc.suppress_watch='on'`，物理搬迁/挂载跳过代次递增 |
| 迁移后直连写月分区仍能触发过期 | 父表触发器会传播到之后 ATTACH 的分区，且对直接写分区同样逐行触发 |

## 数据模型（续）

质控元数据表：`qc_watch`（每站每月变更代次）、`qc_reports`（每次计算的不可变视图，
`is_current` 为当前有效，过期置 `stale`）、`qc_segments`（报告内的达标段）。

## 快速开始

```sql
observations (station_id text, ts timestamptz, value integer,
              PRIMARY KEY (station_id, ts)) PARTITION BY RANGE (ts);
observations_default  -- 初始唯一分区（DEFAULT），历史数据都在这
```

元数据表：`migration_runs`（每月迁移状态机 + 进度断点）、
`migration_delta`（迁移期间触发器捕获的增量变更）。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

`db` 服务首次启动会执行 `db/init.sql`：建父表/默认分区/元数据表，
并写入最近 6 个自然月 × 8 站点 × 每小时一条的整数测值（约 3 万行，全部在默认分区）。

### 本地（无 docker）

需要本机有 PostgreSQL 16 服务端二进制与 Python 3.11+，然后：

```bash
pip install -r app/requirements.txt
PGHOME=/path/to/pgsql PY=python3 bash scripts/dev_up.sh   # 初始化DB+种子+启动应用
```

## 网页操作

1. **月份总览**：各月在默认分区的行数、已挂载月分区、迁移状态。
2. **迁移控制**：选月份 → 设批大小/批间休眠（慢复制）→ 开始迁移；进度条实时刷新。
   一次只迁一个月（进行中再发起会收到 409）。
3. **采集写入演示**：对选定月份手动或自动（可调间隔）做直接 SQL 增/改/删——
   每次都用**独立连接**写父表，模拟真实采集链路。
4. **父表查询**：迁移任意时刻查询父表，行数、主键重复数（恒为 0）、样本行。
5. **一致性校验**：父表行数 / 默认分区剩余（完成后为 0）/ 月分区行数三方核对。
6. **质控异常报告**：选站号 + UTC 月份 + 连续次数阈值(3～12) → 重算/查看；
   显示当前报告是否有效、过期原因、变更代次与最近几次重算状态；
   “慢计算(ms)”可放慢计算以观察并发交错。

迁移流程（`app/migrator.py`）：

| 阶段 | 做什么 | 是否锁写 |
|---|---|---|
| preparing | 建带 CHECK 的影子表、默认分区上挂增量触发器、登记运行记录 | 仅 DDL 瞬间 |
| waiting | 等触发器创建前开启的老事务结束（轮询 pg_stat_activity） | 否 |
| copying | 键集分页分批 `INSERT..ON CONFLICT` 复制，进度落库，可 sleep 放慢 | 否 |
| applying | 按 seq 顺序回放 `migration_delta`（insert/update→upsert，delete→删） | 否 |
| finalizing | **单事务**：SHARE 锁分区树 → 回放剩余增量 → 摘触发器 → 搬漏网行 → `ATTACH PARTITION` → 清理 → 置 done | **仅此短暂** |

正确性要点：

- 影子表在 `ATTACH` 前不属于父表，父表查询任何时刻都只见到一份数据——不重复、不缺失；
- 复制期间的所有变更由触发器落 `migration_delta`，回放幂等，可重复执行；
- 最终切换是**一个事务**，要么全部生效要么整体回滚，崩溃不会留下半吊子状态；
- 任意时刻进程死亡：影子表/增量/进度都在库里，重试从断点继续（影子表不清空、复制按
  最后主键续传、增量继续累积）；
- `migration_runs.status='done'` 的月份再次迁移直接拒绝（409），数据不变。

## 自测

```bash
# 需要已初始化的库；脚本自己拉起/崩溃/重启应用进程
DATABASE_URL=postgresql://envmon@127.0.0.1:5544/envmon python3 scripts/selftest.py   # 迁移
DATABASE_URL=postgresql://envmon@127.0.0.1:5544/envmon python3 scripts/qc_selftest.py # 质控报告
python3 scripts/qc_unit.py                                                            # 连段规则纯函数
```

- `selftest.py`：慢复制+并发直接 SQL 增删改后父表逐行一致；一次只迁一个月；已完成月份幂等；
  copying/finalizing 故障注入后重试成功；复制进行中进程硬退出（`os._exit`）后
  父表仍完整可查可写、重启后续迁完成；触发器/增量/默认分区残留清理检查。
- `qc_selftest.py`：连段规则/月界/阈值与非 app 代码的独立地面真值逐段比对；
  直连增删改使对应站月过期（改回不复活）、其他站月不受影响；慢计算期间采集立即提交、
  相关变化使本次 `superseded` 且不覆盖旧报告；计算失败/进程硬退出后旧报告可看、重启可重算；
  在线迁移自动切换/手动切换前后报告仍有效且判定不变，迁移后直连写月分区仍能触发过期。

## API 摘要

- `GET  /api/state` 月份总览与迁移进度
- `POST /api/migrate` `{month, batch_size, sleep_ms, fail_at}` 启动迁移（409=进行中/已完成）
- `GET  /api/query?month=` 父表查询（行数/重复数/样本）
- `POST /api/demo/write` `{op, month}` 独立连接直接 SQL 增/改/删
- `POST /api/demo/auto` `{enabled, month, interval_ms}` 自动采集写入开关
- `GET  /api/verify?month=` 一致性校验
- `GET  /api/qc/options` 报告可选站号/月份与阈值范围
- `POST /api/qc/recompute` `{station_id, month, threshold, delay_ms?, fail_after_snapshot?}`
  启动重算（409=同站同月同阈值正在计算；阈值须 3～12）
- `GET  /api/qc/report?station_id=&month=&threshold=` 当前/最近报告、段明细、重算状态
- `POST /api/debug/crash` 进程硬退出（仅自测用）

## 已知边界（如实说明）

- 单应用进程内用互斥锁保证"一次只迁一个月"；多副本部署需改用数据库咨询锁。
- `ATTACH PARTITION` 需对父表短暂 `ACCESS EXCLUSIVE`，并会顺序扫描默认分区
  校验无越界行（演示数据量下为毫秒级）；默认分区极大时该扫描会变长，
  生产上可先把数据搬空再挂载以缩短。
- `CREATE TRIGGER` 与 `ATTACH` 等 DDL 本身需要瞬间锁，无法完全避免；
  除此之外复制/回放阶段完全不锁写。
- 种子数据与月份边界均按 UTC；父表查询按 `ts` 的 UTC 自然月切分。
- 质控重算在同一应用进程内用互斥表保证"同站同月同阈值"不并行；阈值不同可并行。
  多副本部署时该互斥不跨进程（数据库代次复核仍能防止旧结果覆盖，但可能重复计算）。
- 变更代次触发器对每次行级逻辑写入都执行（含迁移回放），属写放大；演示数据量下可忽略。
- “报告取自同一已提交视图”指**单次计算内**所有观测与代次读自同一 REPEATABLE READ 快照；
  不同报告之间不保证同一快照（每次重算即一次新视图）。
