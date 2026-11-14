# 震例目录逻辑复制镜像（PostgreSQL → SQLite）

地震台网持续修订的震例目录需要交给重定位分析。本项目在**不停源库写入**的前提下，
用 PostgreSQL 逻辑复制（wal2json）把一张震例表镜像到本地 SQLite，并提供网页查询与
CSV 下载。FastAPI + psycopg2 + SQLite 实现，Docker Compose 一键启动。

## 目录结构

```
.
├── docker-compose.yml          # postgres(源库) + mirror(网页/镜像服务)
├── db/
│   ├── Dockerfile              # postgres:15 + postgresql-15-wal2json
│   ├── init/                   # 容器首启自动执行：角色/库、震例表与 17 条历史样例
│   └── demo/                   # 三个演示场景的 SQL
├── mirror/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # FastAPI 路由：状态 / 查询 / CSV / 开始建镜像
│       ├── engine.py           # 后台状态机：初始镜像 + 持续续采
│       ├── source.py           # psycopg2 访问层：建槽、peek、advance
│       ├── storage.py          # SQLite 持久层：镜像表、复制状态、快照读取
│       ├── config.py           # 环境变量配置
│       └── static/index.html   # 单页网页
├── demo/run_demo.sh            # 三个演示场景的编排脚本
└── tests/e2e_test.py           # 不依赖 Docker 的端到端测试（本机 PG15 实测）
```

## 快速启动

```bash
docker compose up -d --build
# 浏览器打开
open http://localhost:8000        # 本机
# 源库也已映射到主机：psql -h 127.0.0.1 -p 55432 -U postgres -d seisnet
```

1. 打开网页，点击 **“开始建立镜像”**。
2. 状态从 `INITIALIZING` 变为 `STREAMING` 后即可查询、下载 CSV。
3. 在源库执行任意增/改/删，0.5 秒轮询内自动同步到镜像。

镜像库 `mirror.db` 与源库数据分别持久化在 Compose 卷 `mirror-data`、`pgdata`。

## 数据模型

源表 `public.quake`（主键 `quake_no`，逻辑复制的 REPLICA IDENTITY 走主键）：

| 列 | 类型 | 含义 |
|---|---|---|
| `quake_no` | TEXT PK | 震例号 |
| `origin_utc` | TIMESTAMPTZ | UTC 发震时刻 |
| `longitude` / `latitude` | DOUBLE PRECISION | 经度 / 纬度 |
| `magnitude` | DOUBLE PRECISION | 震级 |

## 一致性设计（不漏、不重、不回退、整笔可见）

### 1. 初始镜像与续采无缝衔接，源库不停写

`engine._initialize` 的顺序是关键：

1. 在一个 **REPEATABLE READ 只读事务**里 `pg_create_logical_replication_slot('...','wal2json')`
   并 `pg_export_snapshot()`，同时拿到**起始 LSN** 与**事务快照**；
2. 用同一事务的服务端游标按快照读取全表，分批写入 SQLite；
3. 装载期间源库照常提交——这些事务的 LSN 都晚于槽位点，WAL 被槽保留，
   快照里看不到它们，但续采阶段从槽位点开始全部补齐；
4. 全量落盘后把状态切到 `STREAMING`，才开始对网页供数。

因此快照数据与槽之后的 WAL 严格衔接，**不漏不重**。

### 2. 只处理行变更

`source.peek_changes` 调用 `pg_logical_slot_peek_changes` 并带 wal2json 选项
`add-tables=public.quake`、`include-pk=true`；应用层只接受 `insert/update/delete`
三类事件，DDL 等其它消息一律忽略。`delete` 通过 `oldkeys` 拿到主键震例号。

### 3. 落盘后才确认，写入失败不前移

续采循环对源事务逐个处理（`engine._stream_forever`）：

1. `peek` 只偷看，不推进槽；
2. 把**整笔源事务**的所有行变更连同新水位线 `confirmed_lsn` 在**一个 SQLite 事务**里
   提交（`storage.apply_transaction`）→ 整笔可见，不会出现半笔；
3. SQLite `synchronous=FULL` + WAL，提交确认落盘后，才调用
   `pg_replication_slot_advance` 前移复制槽；
4. 任何写入失败：SQLite 回滚、水位线不动、槽不前移，下一轮整体重试。

### 4. 崩溃恢复按水位线幂等去重

SQLite 的 `replication_state` 持久化了 `confirmed_lsn`。重启后旧消息会被再次 peek 到：

- 消息 LSN `<= confirmed_lsn`：该事务已落盘，只补一次槽确认，不再改数据；
- 消息 LSN `> confirmed_lsn`：整笔重放。insert 靠主键、update/delete 按主键定位，
  结果幂等，所以**不重不漏，旧值不会盖回新值**（更新严格按源库提交顺序应用）。

### 5. 重启行为

- 已建完（`STREAMING`）：自动续采；
- 未建完（`INITIALIZING`）：重启后**重做整个初始镜像**，完成前网页查询/CSV 一律
  返回 HTTP 503，绝不供半成品数据。

### 6. 每次取数只读同一已提交状态

所有网页读取都经 `storage.Store.snapshot()`：显式 `BEGIN` 一个只读事务，
请求内全部 SELECT 命中同一快照，事务结束才释放。CSV 先在同一快照内取全量行再导出，
与列表查询口径一致。

## 三个演示场景

样例库含 17 条历史震例（1960 智利 ~ 2024）。

### 演示一：装载过程中源库修订（新增/更正/删除）

```bash
docker compose up -d --build
# 网页点“开始建立镜像”，在 INITIALIZING 期间（或之后）执行：
docker compose exec postgres psql -U postgres -d seisnet -f /demo/01_loading_phase.sql
# 或一键编排（自动等追平并打印结果）
./demo/run_demo.sh loading
```

该脚本是一整笔事务：新增 `CE20240403075812`、把汶川震例修订为 8.2、删除
`CE20240101071010`。追平后镜像与源库逐行一致。

### 演示二：连续更正，旧值不盖新值

```bash
docker compose exec postgres psql -U postgres -d seisnet -f /demo/02_streaming_phase.sql
./demo/run_demo.sh streaming
```

同一震例经历 `6.1 → 6.4 → 6.5` 三次提交，镜像最终稳定在 **6.5**。

### 演示三：保存后、确认复制进度前重启

```bash
# ACK_DELAY 让服务在“SQLite 已提交、槽尚未 advance”之间停留 20 秒
ACK_DELAY=20 docker compose up -d --build
# 网页点“开始建立镜像”，进入 STREAMING 后：
docker compose exec postgres psql -U postgres -d seisnet -f /demo/03_ack_pending.sql
# 状态页看到“水位线与槽位点差距”非 0 时：
docker compose restart mirror
./demo/run_demo.sh ack        # 或用脚本自动抓窗口、重启、校验
```

重启后 `CE20260305111200` **恰好一条**，差距归零，无重复。

> 未建完重启的演示：`docker compose volume rm <项目>_mirror-data` 后重来，
> 在 `INITIALIZING` 阶段 `docker compose restart mirror`，页面在完成前查询均为 503。

## HTTP 接口

| 方法/路径 | 说明 |
|---|---|
| `GET /` | 状态 + 查询 + 下载单页 |
| `POST /api/mirror/start` | 开始（或重做）初始镜像 |
| `GET /api/status` | 采集状态：状态机、SQLite 水位线、槽确认位点、差距、错误等 |
| `GET /api/quakes?keyword=&min_mag=&limit=&offset=` | 分页查询（未建完返回 503） |
| `GET /api/quakes.csv` | 下载当前目录 CSV（同一已提交快照） |

## 端到端测试（无需 Docker）

测试脚本自行 `initdb` 一套带 wal2json 的临时 PostgreSQL 15，并拉起真实 uvicorn：

```bash
# 需要本机有 PG15（含 wal2json）与 Python 依赖 fastapi/uvicorn/psycopg2
python3 -P tests/e2e_test.py
```

覆盖：T1 装载并发增改删逐行一致；T2 连续更正收敛到最终值；
T3 落盘后确认前杀掉进程、重启幂等不重复；T4 装载中重启返回 503、重做收敛；
T5 快照读取与 CSV 行数一致。

## 配置项（mirror 服务环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `PG_DSN` | （compose 内 postgres） | 源库连接串，账号需 `REPLICATION` |
| `SQLITE_PATH` | `/data/mirror.db` | 镜像库路径（持久卷） |
| `SLOT_NAME` | `quake_mirror_slot` | 逻辑复制槽名 |
| `SOURCE_TABLE` | `public.quake` | 源表 |
| `POLL_INTERVAL` | `0.5` | 续采轮询秒数 |
| `SNAPSHOT_BATCH` | `500` | 初始装载每批行数 |
| `ACK_DELAY` | `0` | 落盘后确认前的停留秒数（仅演示崩溃窗口用） |
