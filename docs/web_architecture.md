# 现代 Web 架构

GridEdge 的交易核心和 Web 展示层是两个明确边界：

1. Rust 核心拥有策略状态机、权利 tranche、执行账户及唯一账本写入口。
2. Rust 在仅回环地址上提供 `gridedge.api/v1` JSON 快照、增量事件和命令接口。
3. Python FastAPI/NiceGUI 是 browser-facing backend-for-frontend。
4. 浏览器通过 NiceGUI WebSocket 接收组件更新，不刷新整个文档。
5. Python 不直接写 SQLite；所有命令带内部令牌并进入 Rust 服务。

## 当前接口

- `GET /api/v1/runs`
- `GET /api/v1/runs/{run_id}`
- `GET /api/v1/runs/{run_id}/events?after={sequence}&limit={n}`
- `GET /api/v1/runs/{run_id}/opportunities?after={sequence}&through={snapshot_sequence}&limit={n}`
- `GET /api/v1/runs/{run_id}/bars?max_points={100..1500}`
- `GET /api/v1/pending-commands`
- `POST /api/v1/pending-commands/retry`
- `POST /api/v1/commands`

机会接口是事件账本的只读聚合，不是第二份事实来源。`through` 必须固定为调用方已取得的
snapshot sequence；所有分页都回显同一 `through_sequence`。每页返回 stable
`opportunity_id`、touch/resolution sequence、时间、cycle、symbol、网格、价格，以及唯一
`GRANTED` 或 `SKIPPED` 结论；Granted 绑定 right/decision，Skipped 绑定非空 reason。
`next_sequence` 在未完成时必须严格前进，`complete` 明确终页。Current 前缀严格满足
`touched = granted + skipped`；早期账若无法把 schema1 Touch 唯一绑定到同一条 Grant/Skip，
则逐条标为 `LEGACY_UNBOUND` 并单列 `legacy_unbound`，只声明
`touched = granted + skipped + legacy_unbound`，绝不猜测历史处置。BFF 必须追完全部页面；
游标停滞、跨 run、跨 prefix、重复或前视记录全部 fail closed。

`GRANTED` 只表示机械权利曾被授予，不是算法终态。当前 contract v3 记录必须继续显示唯一
`RESERVED/DEFERRED/RESIDUAL_HELD/BLOCKED`、终态原因、算法是否成功，以及每条 partial block
的 T+1/risk/no-profit 股数和 lot IDs；`pre_trade_capacity` 还须逐字保留 Grant 的 eligible、
T+1、risk、no-profit 分区及 lot/source/tranche IDs，mixed partial block 必须与它一致。这样
full T+1 阻断即使 C…R 全为零也不会丢失被阻断的真实股数和 lot 来源。否则全递延和算法失败
可能被错误地展示为同一结果。
冻结的 contract v1/v2 只标为 `LEGACY_RECORDED`，保留账本实际记录的 E/D/intent，不补造
C/P/A/B/R、不按当前 Q 换算为“份”。

机会只有在同一 `run/symbol/event_time` 的 `MARKET_BAR_PROCESSED` 也位于 `through`
前缀内才可见；已经原子写入 Touch/Grant/Skip、但行情尚未完成的恢复中间态必须继续隐藏。
查询以 M10 的 event-type/sequence、M11 的 market identity 和 M6 的 correlation 索引为界，
每页只聚合最多 250 个 touch，不扫描或装载全账本。Skipped reason 目前是受枚举约束的账本
原文，DTO 明确标记 `RECORDED_UNVERIFIED`；页面称为“账本记录”，不得冒充同源机械重算。

命令包括 `start`、`step`、`play`、`pause` 和 `finish`。写接口必须提供
`X-GridEdge-Api-Token`；浏览器不持有这个令牌。

每个命令都必须携带 `api_version=gridedge.api/v1`、全局唯一且可稳定重试的
`request_id`、调用方看到的 `expected_sequence` 和 `expected_version`。快照返回
当前 `command_version`，成功回执返回 `accepted_sequence/accepted_version`。
SQLite durable inbox 先记录 `PENDING` claim，再执行命令，最后保存完整的
`COMPLETED` 回执。同一 ID 与同一规范化请求在串行、并发、响应丢失和服务重启后
都返回相同回执；同一 ID 改变 run 或任一命令字段返回 `409`。两个不同 ID 使用
同一旧版本时只有一个可以成功。

FINISH 是长命令，三年 5 分钟数据可以超过 Python BFF 的普通读取 timeout。连接超时或浏览器
断开不能结束服务端已 claim 的业务，也不能释放允许第二个同 run writer 进入的保护边界；
per-run single-flight 必须独立于请求 handler future 存活。相同 request ID 的随后入口只加入
原在途执行或读取 durable response，不得再次启动 FINISH。BFF 对 `ReadTimeout` 绝不盲目重发
完整 `/api/v1/commands` envelope，而只按原 `run_id/request_id` 查询或续作持久回执。

每个新 claim 还必须在同一 SQLite 事务保存规范 `request_json`，以及覆盖规范请求、
accepted version、目标 cursor、数据集身份、`config_sha256` 和 `algorithm_sha256` 的
`plan_sha256`。这些持久字段是服务端续作的唯一
输入，不能由页面缓存、当前默认数据集或新填写的命令字段重建。`GET
/api/v1/pending-commands` 必须枚举所有未完成 claim，包括尚未写入任何 run event、因而不会
出现在 `/api/v1/runs` 的 START。它只公开 run、request ID、命令、accepted version 和
`retryable/blocked` 状态，不返回原始请求正文。

对于业务尚未提交的 retryable claim，页面调用 `POST /api/v1/pending-commands/retry`，正文
严格只有 `run_id/request_id`。Rust 从数据库取回并复核 request、plan hash 和当前 durable
前缀后续作；两个并发重试只能执行一次业务并返回同一回执。START 重启后仍可创建唯一
descriptor，STEP 在 `MARKET_DATA_RECEIVED` 后中断时只补完同一 bar，FINISH 从完整 bar
前缀继续到终态。未提交 START 的当前数据、配置或算法身份发生变化时必须保持零事件并转为
`blocked`；重试返回结构化 `409 PENDING_PLAN_CONFLICT`。篡改 request、plan 或其任一身份/
目标字段的记录，以及迁移前没有 request/plan/runtime identity 的旧 PENDING 记录，也必须仍可
被页面发现但不得猜测执行。历史
COMPLETED 回执保持只读兼容。

`PENDING` 恢复不是 PLAY 专用例外。对于已经达到 receipt
`target_processed_bars/accepted_version`、但保存 `COMPLETED` 回执失败的
START、STEP、FINISH 或 PAUSE，快照刷新、任一后续命令和进程重启都必须先从持久化
业务状态重建规范回执并原子收口旧 claim；不得再次执行业务，也不得要求页面继续复用
已经丢失的旧 `request_id`。旧回执完成后，新 request 才能按最新 sequence/version
继续执行。已提交 START 的恢复只能依据账本内冻结的 descriptor、配置和算法身份；当前数据
文件或配置即使已经变化，也只能补原回执，不能阻断、重放或改写账本。故障注入门禁必须
同时证明原命令入口和专用 retry 入口并发收口 STEP/PLAY 时，由 receipt CAS 返回逐字相同的
持久响应、旧 receipt 唯一、STEP bar 数不重复以及后续命令
不会永久收到“awaiting recovery”。

FINISH 不能只用 `processed == target_processed_bars` 判断业务已提交。其终态还必须存在唯一
`SERVICE_STOPPED`，且 durable response 的 `accepted_sequence` 不得早于该停止事实。若最后一根
bar 的三阶段事实均已提交、停止事实写入却失败，pending 查询必须继续公开这条 retryable FINISH，
不得自行把它收口为 COMPLETED；同 request ID 续作只补停止事实，不重复 bar。

终态证明接受 `STOPPED`、`SAFE` 或 `READ_ONLY` 投影，但拒绝 `RUNNING`。SAFE/READ_ONLY 下的
FINISH 仍要记录每根行情三阶段事实、唯一停止事实和 durable COMPLETED 回执，同时保持受保护模式
并禁止新订单；终态证明不能为了通过检查而把安全状态改回 Running 或伪装成 Stopped。

数据库升级必须按物理旧版本验证：version 8 只有 `request_json/plan_sha256` 的 inbox 在升级
version 9 后新增 `config_sha256/algorithm_sha256`；旧 PENDING 因缺少运行身份继续可见但为
`blocked`，绝不通过填充当前默认值获得续作资格。

PLAY 的 active 状态、速度和 generation 同样持久化。PAUSE 可以使用该 generation
开始时看到的行情序号，因为播放期间行情序号会自然前移；但旧 generation 和未来
version 都不得中断当前 worker。重复 PLAY 只恢复或复用同一 generation 的一个
worker。API 读写均要求内部令牌，未认证请求返回 `403`。

PLAY 的 `COMPLETED` 回执先于 worker 创建；若服务在 claim 后中断，快照或下一条
命令会先补齐同一 PENDING PLAY 回执并恢复同一 generation 的 worker，即使新命令
随后因 PLAY 正在运行而返回 `409`，原 PLAY 也不会停留在“已接受但不运行”。worker 在每根 bar 前
重新核对数据库中的 active/generation。Web 进程还对配置数据库持有操作系统级独占
旧 `COMPLETED` 命令的回执快路径也先执行这项恢复。租约锁身份使用数据库规范路径，
并在目标尚不存在时解析最多 32 层 symlink；因此直接路径或多层 dangling alias 指向同一数据库时，
第二个核心都会在 health-ready 前失败，避免跨进程双 worker。

Web 核心启动时冻结完整配置对象，所有 API handler、后台 worker 和管理动作都使用
这一份事实。运行中即使配置文件被改写为另一数据库，现有核心也不会切换账本；新配置
只在明确重启后生效。

`/health` 是纯进程 liveness；发布、launcher 与 BFF 使用 `/ready`，并继续以内部令牌读取
`/api/v1/runs`。核心持有数据库租约后、监听端口前只执行一次允许建库的 migration，并验证当前
schema 与关键表可读；默认启动不对大账执行全库 `integrity_check`，完整检查属于认证/运维门禁。
M13 为每个数据库创建唯一且不可覆盖的实例 UUID。启动成功后冻结数据库规范路径、文件身份和
库内 UUID，所有 HTTP 业务入口、后台播放和管理动作只允许打开这个已存在的同一实例且不再
migration；PLAY 每根 bar 前使用只读轻量探针复核 schema 与 UUID，不扫描 run 列表。文件丢失、
rename/原位覆盖、实例 UUID 不符或 schema 异常会把当前进程永久置为
not-ready：`/health` 仍为 200，`/ready` 与全部业务读写为 503，且不会创建空白替代账本；恢复必须
显式重启。SIGINT 与 SIGTERM 均先撤销 readiness、取消播放，再由 Axum 排空请求并释放租约。
数据库备份保留原实例 UUID，因此恢复备份只能在 Web 核心完全停服后进行，再以新进程重新完成
启动检查；项目不支持、也不认证对同一实例执行在线原位 restore。

Rust Web 启动时对每个可选 CSV 执行一次“同一字节读取 → SHA-256 → 严格解析”，
并在进程生命周期内持有该不可变 bar 集。`REPLAY_INITIALIZED` 将数据集 ID、哈希、
证券代码、总根数和首末时刻冻结到 run；STEP、PLAY、FINISH 和 CLI 完整 replay
只消费这份冻结事实，不会在
两根 bar 之间重新打开可能已变化的路径。进程重启会重新读取文件，但任何字节变化
都会在旧 descriptor 的哈希边界停止续写，账本 head 和已处理 cursor 保持不变。
该冲突在 durable inbox claim 之前返回结构化 `409 COMMAND_CONFLICT`，因此
`command_version` 及回执表也保持不变。
新 run 的配置、初始网格和 `REPLAY_INITIALIZED` 由同一 LedgerWriter 原子批次写入；
descriptor 失败会回滚整个业务账本，而 durable START request 保留为 `PENDING`，
同一 request ID 重试后只产生一个先于所有行情事实的 descriptor。

行情接口只按 run 的 `REPLAY_INITIALIZED` descriptor 解析数据集，返回
`processed_bars` 以内的可见前缀以及相同的 `dataset_id/data_sha256`。采样超过
`max_points` 时按连续桶聚合，并保留每桶的 open、high、low、close；不会把未来
bar 放入响应。缺少 descriptor 的旧运行返回 `409`，Python BFF 显示“旧运行缺少
数据绑定”，不会读取或展示进程全局默认 CSV。BFF 还会逐次核对行情批次与快照的
数据集 ID 和 SHA，身份不一致即停止渲染。

页面图表直接消费服务端聚合后的 OHLC，使用 ECharts candlestick 时间轴，并把算法
机会的原始时刻放在更高 z-order；机会落在聚合桶首、中、末都拥有有限坐标。指标区
分别显示总、已实现和未实现网格收益，且明确总收益等于已实现加未实现；累计费用
已经计入这些收益，不在页面再次扣减。

原生 HTML 的 `/actions/step/start|next|play|pause|finish` 表单与 JSON API 共用同一个
durable command dispatcher。表单 request 同样产生 `web_command_inbox` 的
`PENDING → COMPLETED` 回执，不存在绕过 inbox 直接调用 STEP/PLAY 的第二写路径。
原生 start/replay 表单明确请求不存在的数据集时会在 claim 前失败，不能由页面当前
选中的数据集或默认数据集替代，因而不会产生 run 事件或 inbox 回执。
现代 NiceGUI 页面必须同时提供“运行至结束”按钮；它与原生 finish 表单一样调用 durable
command dispatcher，终态刷新后仍保持同一回执、cursor 和机会历史。

## 运行

```text
.venv/bin/gridedge-platform
```

默认页面是 `http://127.0.0.1:8787/`。Rust 内部 API 默认使用 8790，
不作为用户页面暴露。

## 后续收口

当前 Python 层以 750ms 的轻量快照轮询 Rust API，浏览器端已经是 WebSocket
增量组件更新。命令 durable inbox 和 run-bound 行情 DTO 均位于 Rust/SQLite
边界；下一步将核心侧轮询替换为可断点续传的 SSE，不改变账本领域模型。
