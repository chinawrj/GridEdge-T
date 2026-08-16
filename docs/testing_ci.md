# 测试与 CI 门禁

`.github/workflows/ci.yml` 是每个 pull request 和 `main` push 的最小发布门禁。单一
Ubuntu job 使用只读仓库权限和同一检出 revision，任何步骤失败都会阻止合并：

1. `cargo fmt --check`；
2. `cargo clippy --locked --all-targets --all-features -- -D warnings`；
3. `cargo test --locked --all-targets --all-features`；
4. `uv lock --project webapp --check`，确保项目元数据没有绕过已提交 lock；
5. 从 `webapp/pyproject.toml` 构建并安装 BFF wheel 到全新 Python 3.12 venv，复制测试
   到 checkout 外后执行 pytest，防止源码目录意外遮蔽已安装包；
6. 从临时工作目录以优化构建运行 sample CSV 的短 replay 性能门禁，硬性限制 replay
   不超过 1 秒、64 MiB RSS 和 2 MiB SQLite，并上传结构化指标；
7. 仍在 checkout 外，通过已安装的 `gridedge-platform` 启动 Rust core 与 NiceGUI BFF，
   等待 Rust `/ready`、带内部令牌的 `/api/v1/runs` 和 BFF 页面均成功后再干净停止；`/health`
   若保留只代表进程存活，不能作为迁移或账本可读的发布判据；
8. 使用 lock 中的 Playwright 和固定 Chromium，从 checkout 外实际点击创建、单步、自动播放、
   暂停和刷新，验证 NiceGUI 局部更新、ECharts、持久游标和移动端基本布局。

Web 启动前置门禁必须真实覆盖损坏 SQLite、未来或不连续 `schema_migrations`，以及迁移 SQL
失败：这些输入不能得到 `/ready=200` 或成功的认证业务读取，失败迁移也不得留下部分 version。
正常数据库只有在一次性 migration、当前 schema 和关键业务读取完成后才 ready；同一数据库第二 core 仍须在 health
前被 lease 拒绝。launcher、clean-wheel smoke、CI 和 Chromium 都必须等待 readiness 加认证
`runs`，不能因 liveness 200 提前放行。运行期探针只读打开已经存在的同一数据库文件，不执行
migration；文件丢失、被替换或 schema 漂移会永久撤销当前进程的 readiness，所有业务入口返回
`503 DATABASE_UNAVAILABLE`，也不得初始化替代账本。SIGTERM 必须停止播放、完成干净退出并释放数据库租约。

机械机会历史是独立阻断合同。Rust 黑盒测试必须以小页遍历专用 opportunities API，覆盖已处理
前缀、双 run 隔离和重启一致，sample 终态精确为 `Touched 12 = Granted 8 + Skipped 4`。
BFF DTO 必须 strict，client 必须在同一 through sequence 追完全部页面并拒绝不前进游标；网页
显示总数和每个 Skipped reason。真实 Chromium 必须自动播放到短数据终态，看到 `12/8/4`、
至少一个 `OBSERVATION_BOUNDARY` 跳过原因，并在刷新后保持完全相同。
冻结 certification v6/v8/v9/v10 也必须逐页读到终点：可证明绑定的 contract v1/v2 只能显示
原账记录的终态与股数；不能唯一绑定的 schema1 touch 必须计入 `legacy_unbound`，不能猜成 Grant
或 Skip。当前 Grant 必须让 Deferred、算法失败 Blocked 和含 lot IDs 的 partial block 在 DTO/UI
中保持可区分，不能退化成同一个 `GRANTED` 标签。
全量 T+1 反例必须证明 `C/P/A/E/D/B/I/R` 即使全零，`pre_trade_capacity` 仍保留被阻断
股数与 lot IDs；mixed partial 的三类阻断数量和 lot IDs 必须与 Grant 容量逐字段相同。
Page 与合并后的 History 都必须拒绝任一 record 的 `standard_quantity` 与顶层 Q 不一致，冻结
contract v1/v2 也必须拒绝混入 current C/P/A/B/R 或 pre-trade 容量。
机会查询计划也是阻断门禁：touch anchor 必须命中 M10 的 run/type/sequence 索引，processed-bar
反查必须命中 M11 market-identity covering index，单机会事件必须命中 correlation covering index；
SQLite 可按数据分布为 totals 的 resolution 子查询选择 correlation 或 run/type 索引，但不允许
退化成全表扫描。发布 wheel 必须在 checkout 外的全新 Python 3.12 环境完成上述 DTO/client 测试，
防止源码目录遮蔽漏装模块。

Rust 依赖始终使用提交的 `Cargo.lock`（`--locked`）。Python 发布依赖受
`webapp/pyproject.toml` 声明兼容范围，`webapp/uv.lock` 冻结完整解析结果，CI 固定
Python 3.12 并以 `uv sync --frozen` 安装；pytest 也从实际 wheel
安装环境运行。工作流启用并发取消，新的同分支提交会停止旧的尚未完成门禁。

本地等价检查：

```sh
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
uv lock --project webapp --check
uv venv --python 3.12 /tmp/gridedge-bff-test
UV_PROJECT_ENVIRONMENT=/tmp/gridedge-bff-test \
  uv sync --project webapp --frozen --extra test --no-install-project
uv build --project webapp --wheel --out-dir /tmp/gridedge-bff-dist
uv pip install --python /tmp/gridedge-bff-test/bin/python --no-deps \
  /tmp/gridedge-bff-dist/gridedge_web-*.whl
/tmp/gridedge-bff-test/bin/python -m pytest -q webapp/tests
/tmp/gridedge-bff-test/bin/python -m playwright install chromium
GRIDEDGE_BINARY="$PWD/target/debug/gridedge" \
GRIDEDGE_PLATFORM_EXECUTABLE=/tmp/gridedge-bff-test/bin/gridedge-platform \
  /tmp/gridedge-bff-test/bin/python -m pytest -q \
    tests/browser/test_dashboard_playwright.py
```

短 E2E 必须使用临时数据库，不能复用开发者的 `gridedge.db`。平台启动 smoke 也必须
通过绝对 `GRIDEDGE_BINARY/GRIDEDGE_CONFIG/GRIDEDGE_DATA` 明确输入，从任意当前
目录运行；这同时验证 wheel console entry point 和 Rust/Python 进程生命周期。

Rust 编译器、rustfmt 与 Clippy 由根目录 `rust-toolchain.toml` 固定为 1.97.1；Cargo
命令始终带 `--locked`，依赖图必须与 `Cargo.lock` 一致。

三年数据不进入 PR 的耗时门禁；nightly/release 的性能基线、告警阈值、指标 artifact
以及本地短模式命令见 [performance.md](performance.md)。
PR short 与三年任务都先读取仓库内版本化性能输入 manifest；数据、配置、run ID、采样次数、
算法、账本 bootstrap 或确定性业务结果任一身份不符均属于正确性失败，而不是可降级的性能
告警。`v*` release-candidate tag 必须等待 enforced performance job 成功后才可人工发布 GitHub
Release；当前仓库没有自动发布动作，因此这是一条受控发布流程要求，不冒充 GitHub 平台级
`needs` 依赖。

Web snapshot 热缓存的版本向量、single-flight、PENDING/error 不缓存、黑盒回归和 cold/warm
性能预算见 [snapshot_cache.md](snapshot_cache.md)。

Durable inbox 的故障门禁同时覆盖 claim-before-business 和 business-after-claim。前者必须
验证迁移后的规范 request/plan/config/algorithm identity、无事件 START 的全局发现、只含 run/request ID 的
自助重试、重启与并发幂等、STEP 半根 bar、FINISH 续作，以及篡改/旧 PENDING
fail-closed；未提交 START 在数据、配置或算法身份漂移后必须为 `blocked`、返回
`PENDING_PLAN_CONFLICT` 且保持零事件。BFF 模型、client 和页面必须使用专用 pending API，
不能重新生成原命令。后者继续验证业务已提交但回执仍为 PENDING 时，即使当前数据文件与
配置已变化，仍按账本冻结 descriptor/runtime identity 自动补同一回执，且事件、descriptor 和
业务执行次数完全不变。计划摘要的每个输入字段都要有独立篡改反例。原命令与专用 retry
并发收口同一已提交 STEP/PLAY 时必须返回逐字相同的 durable response；物理 version 8 数据库
升级到 current version 13 后必须新增配置/算法身份列、M10/M11 精确查询索引，并删除 M12 指定的三个
冗余事件索引，而旧 PENDING 保持 `blocked`。物理 version 8/10/11 回归必须分别还原对应列、
migration 记录和索引集合，避免只测到已存在对象的假阳性；重复启动必须保持 schema 13 且不
改变业务表。M12 迁移只向前执行；删除索引只释放 freelist 页，测试和启动流程均不得自动
`VACUUM`，也不得把文件字节立即缩小误当作迁移正确性的前置条件。unfinished-bar 的外层
received 扫描必须命中 M10，内层完整身份反查必须命中 M11。

长 FINISH 的超时门禁必须用足够长、但可在 CI 数秒完成的确定性行情：首个真实 TCP 请求在
PENDING 且已有部分完整 bar 时断开，随后相同 request ID 进入。两者必须共享一个不依赖 handler
future 生命周期的 per-run single-flight，最终只有一个 COMPLETED receipt，Received/Committed/
Processed 各恰好总根数、SERVICE_STOPPED 恰一且 duplicate/error 为零；再次读取必须返回逐字
相同回执。BFF MockTransport 必须让 `ReadTimeout` 后的下一请求只携 run/request ID 查询 durable
receipt，禁止第二次 POST 完整命令。真实 Chromium 还必须点击现代页面的“运行至结束”，验证无
整页导航、终态 cursor、SQLite FINISH 回执和刷新后机会历史；不能用自动播放到 EOF 冒充 FINISH UI。

FINISH 还必须覆盖最后一根之后的独立崩溃窗口：在 `SERVICE_STOPPED` 写入点注入失败，先严格断言
Received/Committed/Processed 都已等于总根数、停止事实为零且 receipt 仍为 PENDING；调用 pending
列表不得提前补回执。移除故障后用同一 request ID 续作，只能新增唯一停止事实并完成唯一回执，
且 `accepted_sequence >= SERVICE_STOPPED.sequence_number`，三阶段 bar 数保持不变。

当前 SAFE 终态还要走真实 Web FINISH：先用独立 Paper 对账差异进入 SAFE，再要求总根数的三阶段
事实、唯一 SERVICE_STOPPED 和唯一 COMPLETED receipt，终态 mode 仍为 SAFE，订单投影和
`ORDER_INTENT_CREATED` 都为空。对应的 READ_ONLY 用例损坏全部 snapshot checksum，要求全日志恢复后
仍可完成同一三阶段/停止/回执合同，终态保持 READ_ONLY 且无订单。两者防止终态 helper 把合法保护
状态误判成未完成或偷偷恢复交易。

M13 为每个物理数据库创建唯一、不可更新也不可删除的随机实例身份。Web 启动时必须同时冻结
规范路径、设备/inode 和该实例身份，并在每次建连接时复核；因此 rename 替换和保持同 inode 的
原位覆盖都必须撤销 readiness、让所有业务 API 返回 503、停止 active PLAY，且绝不能把旧进程的
事件写入替换进来的合法账本。长循环 FINISH/backfill 还必须逐 bar 做轻量身份探针；测试在不发送
任何额外 HTTP 探针的情况下原位覆盖数据库，要求 FINISH 失败或断连、替换账本零写且服务永久 503。
迁移回归必须验证 singleton、32 位小写十六进制身份和 UPDATE/DELETE/INSERT-REPLACE
保护触发器，并证明物理 M8/M10/M11 均可一次升级到 13。

性能发布签章使用 business-result v2：short golden 为
`d7e3644ba66965104998770b9ebf795f4cec5838f2f101adf9076423c7da9e1d`，三年 golden 为
`1b624dd87114ffe3270c66e91ac31a62d6e94ff00f692bd1a34616001a3c082b`。测试必须验证 snapshot
run/head/checksum，每个 right 的 grant/request context+right/response/唯一 terminal 和逐方向
`C/P/A/E/D/B/I/R`；partial Blocked 后接 Reserved 是合法路径，缺失/重复 terminal 或跨 right
抵消不是。SELL allocation 的价格、commission、tax、cost、worst price、maximum fees 与
worst-case profit 必须闭合，费用不可残留，逐 slice 实际及最坏收益不得为负，lot 成本守恒；
tranche 必须为 QUANTITY 且非负守恒。Paper accepted/rejected/cancelled、intent/order/fill ID、
fill quantity 与终态账户逐项一致，rejected/cancelled 必须零 fill。全局 Decimal canonical helper
必须保留整数位，明确锁定 `10` 与 `1` 的 Q hash 不碰撞。

BUY 最低佣金门禁使用 `Q=1500`、lot size `100`、limit `9.80`、不利成交价 `9.81`、
commission rate `0.0003` 和 minimum `5` 的精确边界：现金 `14719.99` 必须得到
`I=0/B=1500`，现金 `14720.00` 必须得到 `I=1500/B=0`。700+800 两次 Paper fill 的累计
commission 必须恰为 `5`，终态现金为零，snapshot 热重建与完整日志冷重建逐字段一致。
Ledger 回归还要把 terminal、tranche reservation 和 intent 联合伪造成可买一份，证明少一分
钱仍原子拒绝。兼容矩阵固定 schema 5 的旧 `14725` 接受/`14720` 拒绝，以及 schema 6 的新
`14720` 接受/`14725` 拒绝，防止修复当前写入时篡改历史解释。数值边界同时固定：极端单股不利
价格上取整得到 `5010.01`，两份授权只需一次累计比例佣金而得到 `29438.83`；现金 `14720` 时从
两份请求安全回退为一份，现金 `29438.83` 时批准两份。完整前缀还必须重放 schema 3 的旧
Blocked（现金 `14720`、`I=0`）和 Reserved（现金 `29440`、`I=1500`）处置；新写只允许
schema 4 处置与 schema 6 intent，任何试图新写 schema 3 的请求必须零 head、零状态变化地拒绝。

极值算术门禁必须运行在 debug 测试配置并包裹 `catch_unwind`：`Decimal::MAX` anchor 乘合法 ratio、
会把任一节点舍入为零的正 anchor，以及 `i32::MAX` boundary 均要求 `Config::validate` 和新 run 启动
返回普通错误，events/snapshots/Paper 表零行。BUY 风控同时固定 `max_position=i64::MAX`：headroom
`Q-1` 在直接 checker 与 canonical approval 都拒绝，恰好 `Q` 批准一份并精确到 MAX，普通 whole-Q
结果不变。另以同一规范 BUY intent/fill 对照 Ledger 和 Paper：恰好 Q headroom 成功，少一股时
不得 panic/回绕，journal sequence、StrategyState、paper account 和 paper report 必须逐项零变化。

收益门禁使用费用取整敏感的精确反例同时验证盯市和逐 lot 保守退出。Core 必须证明估值与
保本证明共用同一 sell-slice economics，并覆盖每 lot 最低佣金、T+1/冻结仍估值，以及无 mark、
未知成本、缺失冻结策略时的明确不可用。HTTP 必须返回命名清晰的 Decimal 文本字段和策略版本，
兼容字段只能继续表示盯市，正常时必须与新 MTM 字段逐字相等，未知成本或无 mark 时旧新字段
必须同时为 `null`，不能静默改义或退回另一计算；半根 bar/未处理行情不得改变任一估值。相同 head 的
热快照、进程重启后的冷快照和完整日志重建必须逐字段一致。BFF 模型禁止缺字段、额外字段和
数字冒充 Decimal 文本；页面同时展示两套总收益/未实现收益，并用 `—` 表示不可用。

运行上下文恢复门禁必须从 START 后尚无任何 snapshot 的真实数据库开始，随后漂移当前配置的
initial cash/position/sellable，并证明 Web 与 CLI 仍返回账本 A 值，而 STEP 在 claim 前拒绝且
零 receipt。另以 symbol/anchor 漂移验证只读查询仍使用 A；删除或损坏 snapshot 后必须与完整
日志结果一致；未完成 bar 的公开前缀必须使用同一 A 种子且不泄漏内部行情事实。测试还要绕过
append-only trigger 模拟物理篡改 `RUN_STARTED`，并证明它与 `CONFIG_SNAPSHOTTED` 不一致时
Web/CLI 立即 fail closed、账本 head 不变。即使存在 checksum 合法的 snapshot，当前 CONFIG 缺少
内容哈希也必须拒绝；`ALGORITHM_REGISTERED` 缺失、位于 CONFIG 之前，或其 artifact/environment/
platform SHA 为空、短位、大写、非十六进制时同样拒绝且零写入。所有 snapshot 均损坏时，Service
必须从完整日志恢复业务投影、只追加一次 `READ_ONLY` fallback；二次恢复只合法增加自己的
`RECOVERY_COMPLETED`。

Legacy compatibility has an explicit read/write split: a schema-1 fixture with its algorithm identity physically removed must still pass CLI `status` and `rebuild-state`, while direct Service recovery, replay continuation and operator resume all fail with an unchanged journal head and event count.

## 真实浏览器门禁

`tests/browser/test_dashboard_playwright.py` 是独立的 Chromium 交付合同，不使用 mock：
它从 checkout 外启动 Rust core 与已安装的 `gridedge-platform`，先注入一次 START 业务失败，
实际确认页面发现待恢复命令并一键复用原请求；随后点击下一根、自动播放和暂停，并要求 STEP 恰一根、PLAY 自动推进、PAUSE 后至少四个播放周期不再前进、
交互期间只有一次顶层 document 请求、ECharts canvas 已挂载、页面无 JavaScript/console
错误；浏览器还必须从真实 Rust 快照同时读取盯市与逐 lot 保守退出数值，按明确标签核对页面，
并证明刷新后不串位、不丢失。最后验证游标恢复，以及 390×844 视口无横向溢出。失败时保留 Playwright trace、
整页截图和平台日志。

该测试已作为 PR/main 硬门禁运行。Python Playwright 版本由 `webapp/uv.lock` 固定，Chromium
revision 由该版本管理并使用 GitHub Actions cache；Linux 系统依赖通过 Playwright 官方安装器
准备。job 复用 clean-installed wheel 与 Rust binary，设置 `GRIDEDGE_BROWSER_ARTIFACTS`，
失败时上传 trace、截图和平台日志。单次交互预算不超过 90 秒，所有业务输入和 ECharts 资产
均使用仓库本地文件，不允许 CDN 或外部行情请求。不得通过 `skip`、放宽超时或只检查首页
200 来替代真实交互。
