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

Decision v4 的纯算法门禁固定 `RESOURCE_AWARE_WHOLE_Q_V1`：20 根 warmup、`.35/.40/.25`
市场分、`.60` 硬门槛、资金库存不入市场分、`rho=M/(M+4)`、`pace<=.50` 和单机会最多 2 份。
至少覆盖：高现金但 `m=.59` 仍全 Defer；深度为1但 trend/location 为0时 `m=.35`；19根即使
强信号仍 warmup；`X=Q,m=.60,M>=1` 行权1份；`A=Q,M=0` 得 `B0=Q,X=0,D=0`；强信号
`X/Q=2/4/20` 分别只行权 `1/2/2` 份。所有 case 必须验证
`A=B0+X`、`X=E+D`、`E=I+B1`、`R=D+B0+B1`，并锁 rational alpha numerator/
denominator，不能从 Decimal alpha 反推股数。

兼容门禁必须保留一份 Decision v3 context hash golden，并证明 v4 新字段不进入 v3 hash；v3
携带资金库存证据与 v4 缺少该证据都必须拒绝。v4 hash 必须绑定可用现金、仓位、20根已处理行情
身份、市场特征、`A/X/B0`、pace 和整数目标，任一字段变化都改变 hash。BUY 还必须用未完成订单的
剩余数量锁定 `pending_buy_quantity` 与 `position_exposure=position.total+pending`；达到 target/max
后续机会必须得到 `M=0/B0=A`，联合篡改 exposure/headroom/resource 也必须由 Ledger 原子拒绝。
后续 service/Ledger
回归还要独立重算上述事实、拒绝联合伪造，并证明当前或未来 bar 变化不影响本次决策，连续运行与
逐bar恢复生成逐字相同的 v4 request/response。

同花顺模拟账户 UI 驱动必须把 AppleScript 执行边界替换为本地 fixture 后测试，通用 CI 不得启动客户端
或提交委托；显式 ignored 的 macOS 硬件 smoke 只能在人工确认的“模拟练习”机器上运行。测试至少覆盖：唯一“模拟练习”标志、同页实盘控件拒绝、bundle/版本漂移、三个
代码/价格/数量输入框的相对布局、方向与提交文案绑定、股票代码/价格/整手/单笔金额上限，以及
dry-run 永不写 UI、永不点击提交。真实 submit smoke 必须从独立临时 durable outbox 消费一笔 100 股
模拟 intent，取得唯一新增合同后只撤销该合同，并断言完整零成交撤单；不得复用为实盘测试。
该 ignored submit-cancel smoke 的行情前检必须在创建 outbox 或任何提交 UI 动作之前完成；当前 BUY 3.20
只在最新模拟行情不低于其 105% 时才允许继续。普通 CI 必须静态锁定这个顺序、价格缓冲、100 股及
撤单前的零成交证明，防止 ignored 用例未执行时安全合同悄然漂移。
页面行情 probe 必须证明只写证券代码、绝不写价格/数量或点击订单按钮，并在写前后各自重验模拟
身份。行情 `last_price` 只能来自唯一模拟窗口中的只读行情显示（例如
`限价委托 方向 3.53↓ -0.02 -0.56%`），不得读取下单 `price_field`：该输入框为空或残留任意旧限价都不能
改变行情。refresh 必须返回严格的 `QUOTE_REFRESHED<TAB><canonical Decimal>`，Rust 在执行一次 final probe
前解析并验证价格严格为正且 scale 不超过 3；缺值、多值、非 Decimal、非正数或过高精度均须直接失败。
final probe 只用来复核 bundle、版本、唯一模拟身份与代码回读，不能重新从下单限价框取价。refresh 脚本自身
已必须完成 bundle/版本/唯一模拟窗口/实盘负证据/三字段/代码回读/只读行情可用性前检，所以一次行情证据
采集只允许“refresh + 一次 final probe”，不得再执行重复的初始完整 probe。
行情 `source_sha256` 必须绑定规范化后的只读行情价格、symbol、bundle、版本与模拟账户身份；相同只读价格下，
下单限价或数量框的空值/旧残留不得改变该哈希，而只读价格变化必须改变哈希。否则持久 quote 虽保存了价格，
其所谓来源摘要却无法证明该价格，不能作为可审计证据。Decimal 必须先规范化，因此 `3.53` 与 `3.530`
得到同一来源摘要，不能让纯 scale 差异制造虚假行情身份。
`prepare-form` 不能假设行情 probe 已给下单限价框留下值，也不能把该输入框的空值或人工残留当作证券加载
证明。准备脚本必须自己对唯一 codeField 执行“写代码 → focus → Tab/等价 AX 提交”，随后在最多 20 次的
有界循环中，以冻结 static-text 快照同时证明代码回读、几何区间内唯一证券名称，以及“限价委托”锚点上方
唯一严格正只读行情文本。完整证据门禁通过后才允许写订单 limit 与 quantity；缺失、多义、动态元素消失或
超时都必须在这两个写操作之前停止。prepare 全路径仍只允许字段写入，不得包含提交/确认按钮点击或 AXPress。
对唯一 codeField 执行 `set value` 不等于同花顺已接受编辑；脚本必须在不触及价格/数量/下单按钮的前提下，
将该字段设为焦点并用 Tab 或等价 AX 确认提交一次编辑。提交后只能在有界循环中等待，并同时证明 requested symbol
回读、该证券的非空名称证据与严格正价格；缺任一项、超时、身份漂移或 final probe 不一致都不得返回
`QUOTE_REFRESHED`。硬件 smoke 必须依靠 `?` 在创建 outbox 或任何资金 UI 动作之前传播该失败。
每个 refresh wait attempt 必须先一次取得 static-text 列表并物化为冻结引用，不得在先读 count 后再对
每秒重建的活 `static texts of targetWindow` 按 item 取值。冻结的无关元素（例如底部动态时钟）在该轮内以
`-1719/-1728` 消失时，只有已经成功读取并证明 `AXStaticText` role 的单个非证据节点可以局部跳过；
窗口、集合、role 等其他错误仍必须原编号抛到外层重新取证。
证券名称仍必须由 code/price 几何区间内恰好一个
存活、非空、可读 text 证明；该证据消失或多义时本轮不得设置 ready，20 轮内未重新获得完整证据必须失败。
实机 release 观测一次原路径约 14.96 秒，因此以 20 秒作为当前审查的 quote-probe 启动预算：距下一个 5 分钟
边界小于或等于 20 秒时，live 必须先等到边界并 `continue` 重新检查 session，不得启动已知大概率跨桶的 UI probe。
该预算只能在新的硬件 p95 证据后调整；不能为了降低等待而放宽 builder 的跨桶原子拒绝。
五分钟聚合测试必须锁定 OHLC、严格递增观察时间、午休/收盘边界、零合成 volume/amount、
不生成缺失桶，以及已完成桶同 timestamp 内容变化时阻断。`observed_from → observed_at` 跨五分钟桶
必须原子拒绝且不能污染下一桶；持久 quote 的 symbol/价格/小写 SHA、bundle、版本和模拟标志任一
非法也必须在 builder 状态变化前拒绝。连续部署测试还要锁定周末和交易时段、append-only quote/bar
时间倒退与重复冲突，以及失败重启不能刷新 stale 宽限。行情 freshness 以每次完整安全取证的
`observed_at` 严格推进为准，而不是以成交价是否变化为准：同一价格持续数分钟但取证时间持续推进仍是
新鲜行情；完全重复、倒退或不推进的证据即使价格变化也必须 fail closed。重启只恢复最后一个已接受的
证据边界，不能把旧证据重新解释为新采样。
未知合同仅允许明确的全成/全撤终态，其余“未报/已报/待报/未成交/部分成交”和未知文案全部阻断；
durable CANCELLED 与页面仍开放等状态矛盾也必须阻断。旧 v4 前缀在未激活新政策前仍按历史
10 万元现金门禁只读重放；新政策激活必须以唯一事实绑定当前平台、初始部署、Paper 快照、outbox
快照和 journal head。激活后测试须以市场硬门槛已通过的真实机会证明：3.36 元下每份含费预留
`18491.05`，少 0.01 得 0 份，恰好该金额得 1 份；初始部署后的 `103418.53` 全部可用于持续 BUY，
得到 5 份而不是 0，SELL 审批不受现金余额影响。
正式 002256 模拟部署还锁定一次性初始半仓合同：初始现金 20 万元、现金底线 10 万元、`Q=5500`、
首个有效报价 3.51 元时，只能由策略服务通过 durable Ledger 与 Paper 执行生成唯一 BUY `5Q=27500`
股，再由 outbox 从该账本事实派生模拟 UI 动作。重启或重复首证据不得产生第二个 intent；远端必须完整
成交 27500 股且成交价不劣于 Paper 保守均价，才允许继续。正式 live CLI 不得暴露方向、价格、数量或
合同号等临时下单参数，测试和操作员都不能绕过 Ledger/Paper/remote 对账直接驱动资金 UI。
测试还必须把机械上限临时升至 6 份，证明第 6 份会使 projected cash 跌破 10 万元而只能选择 5 份；
schema-7 typed origin 必须逐字绑定尚未 Processed 的行情 event ID 与 canonical bar hash，改 hash、降级
schema 或在 Processed 后补写都拒绝。Paper partial 必须以一个 intent 守恒到 27500 股，Paper reject
必须使 outbox `eligible=0`。opening-allocation lot 不得制造 grid right/历史行权；同日完整计入 T+1
blocked 分类，次日完整转入 eligible SELL lot/tranche 分类。
live 编排还必须锁定每根新 bar 的顺序为“未知合同预检 → outbox 对账 → 已绑定合同严格终态 permit →
行情/`process_bar`”；known 未成交不能取得 permit，且失败前后源账 head、intent 和 outbox 状态不变。
11:25 与 14:55 是上午/下午最后结算点，分支必须先于普通 market 分支，午间只能幂等对账且 14:55
结算后进程成功退出，不能在 11:30 或 15:00 后新增行情驱动的提交。
影子执行 permit 必须以源账本同一 order 的 durable Paper fills 重算数量和加权均价：远端少于 `Q`、
BUY 成交价高于保守均价、SELL 成交价低于保守均价都阻断；相等或更优价格才许可。费用因委托表
不可观测而不能宣称已对账，测试须继续锁定 Paper 保守费用与 10 万元报告指标，并锁文档/启动合同不允许把该模式用于
实盘。真正实盘阶段必须改为 remote fill/fee 直接写 Ledger。
委托页不再显示已绑定合同时，不得从“零行”推断已撤单。自动回归必须使用完整成交页 fixture：
物化行/列，以原生非空成交编号去重，按唯一 contract ID 聚合 checked 数量与 Decimal 成交额。已经
`cancel_state=AMBIGUOUS` 的合同只有在“委托页唯一全部成交记录 + 成交页唯一完整聚合”
同合同、同 symbol/方向/数量时，才可原子转为独立
`FILLED` terminal resolution，不能转为 `CANCELLED`。转换之前必须只读对账，不调用 submit/cancel；相同证据
重试和重启返回同一终态，异证据、部分/超量成交、重复成交编号或跨合同混合继续未解决。
随后单独以源账本 Paper modeled fills 检验 live permit；不利价格不撤销已证明的 `FILLED` 事实，但必须阻断下一根 bar。
委托页的成交价只作为第二份终态证据：必须为正数且位于成交页该合同所有明细价的 min/max 之间，
不得强行等于精确加权均价。比较 Paper 保守价时必须使用成交页 `sum(price×quantity)/sum(quantity)`，
不得回退到委托页显示价或委托限价。
跨日回归必须使用真实 SQLite 源账/outbox：昨日 durable `FILLED` 合同及其完整 canonical 成交证据允许
在今日委托表中缺席，permit 仍由源账 Paper fills 与持久成交明细独立重算；同一合同若今日出现则仍须
逐字段匹配。SUBMITTED/CANCELLING/AMBIGUOUS、删除事实、篡改证据、未知开放合同都必须 fail closed，
且审计前后源账 head 与 staged intent 不变、资金 UI 动作次数为零。
launchd 合同固定工作日 09:25、release binary、60 秒 stale、失败退出重启和成功日终不重启；新 outbox
才使用显式 sequence 0，已有绑定必须沿持久 cursor 续作并证明零重扫、零 UI 二次动作。
`prepare-form` 的 fake executor 必须证明写入脚本不包含点击“确定买入/卖出”，且第二次 probe 对
代码、Decimal 价格和数量逐字段回读；任一字段被客户端改写时立即失败，不能继续到后续阶段。
独立 outbox 必须永久绑定 32 位小写数据库 instance ID、run ID 和显式初始 sequence；游标、来源
或同 intent 请求漂移都要回滚整批。`ORDER_INTENT_CREATED` 只能进入 DISCOVERED，只有后续匹配
同一 order ID 的 `ORDER_SUBMITTED` 才能进入 ELIGIBLE；乱序、孤立 submit 和重启重扫不能产生
重复项。该阶段只允许读取源账本和写 outbox，不能调用 AppleScript。
委托核对 probe 必须只点击“委托”页签，只接受两种逐字受审表头：原十二列，以及同花顺 5.3.2
在“合同编号”和“委托属性”之间新增“申报编号”的十三列。申报编号只作为只读审计证据保留，
任何提交、对账或撤单仍只能绑定“合同编号”，不能把两个编号互换或回退到固定列号。缺失、重复、
乱序关键列以及任何未受审新增列都必须 fail closed；不得为了兼容任意未来列而按模糊名称或位置猜测。
每行使用稳定的 AX 层级列序号，
同时记录横坐标作诊断，不能只按横坐标排序：同花顺可能把“委托属性”以异常 x 坐标暴露在价格列前。
列序号缺失、重复、不连续，或缺列、改名、重复表、行数不符、非模拟页面都要拒绝。测试还必须逐字证明脚本不包含点击
“确定买入/卖出”。
同花顺会用成交价 `0` 或 `0.000` 表示“未报/未成交”或零成交撤单合同尚无成交价，解析后必须规范为 `None`，
不能误作真实零价成交。开放态及 remark 含“撤”允许该 sentinel；部分成交或全部成交必须携带严格正成交价，
零价、负价和非法 Decimal 一律在对账或撤单动作前拒绝。任意严格正的实际成交价必须按 Decimal
精确保留，不能因 sentinel 兼容而被抹成 `None`。
执行状态机必须在任何 UI 最终点击前原子写入 SUBMITTING，并冻结提交前已有合同编号集合；只有
唯一新增且与 symbol/direction/quantity/price 全匹配的合同才能转为 SUBMITTED。零个或多个匹配、
脚本超时或未知弹窗都转为终态 AMBIGUOUS；重启后 SUBMITTING/AMBIGUOUS 只能继续查询，不能回到
ELIGIBLE 或再次点击。合同编号、证据 JSON 和 request SHA 漂移必须原子拒绝。
准备与最终提交脚本必须先把三个可访问性输入框物化为本地列表，再按 y 坐标识别代码、价格和数量；
测试必须把 probe 行与控件枚举顺序打乱，并锁定脚本不对 `text fields of targetWindow` 的惰性结果直接取
`item 2/3`。最终点击脚本本身还要重验 bundle、版本、唯一模拟窗口、实盘控件为零和字段回读，
所有检查均位于唯一一次点击之前。
确认弹窗必须通过当前 focused `AXSheet` 的角色、警告描述、两个精确按钮和完整代码/方向/价格/数量
文案验证，不能假设自定义 sheet 会出现在 System Events 的 `windows` 集合中。撤单属于另一个高风险动作：
发布门禁必须用 fake executor 证明它在点击前第二次读取委托表，物化唯一可撤行的 static-text cells，
仅接受 outbox 已冻结的唯一 `remote_contract_id`，且撤单后同一合同必须为委托数=撤销数的完整零成交撤单。
当前自动撤单允许委托表同时存在多个可撤合同，但只能在先物化全部 rows/cells 后取得唯一包含冻结
`remote_contract_id` 的行，并且只能双击该行。零个或多个合同号匹配都必须在动作前停止；partial fill 仍必须标为
歧义。脚本中不得出现或使用“全撤/撤买/撤卖”批量按钮。
`cancel-contract` 只是 durable outbox 的另一个索引：CLI 只能接受 `--outbox + --contract-id`，必须先解析为唯一
已提交 intent，再进入与 `cancel-intent` 完全相同的持久取消状态机。未知/重复合同、非 SUBMITTED 状态在 UI
之前拒绝；已 CANCELLED 只返回旧证据，CANCELLING 只对账，AMBIGUOUS 终态拒绝，三者都不得再点击。
两个 durable intent 不得共用一个 remote contract；这一唯一性必须在 outbox 写入边界原子保持。
outbox schema 还必须把源账本 `ORDER_CANCEL_REQUESTED` 以唯一 sequence、order ID 和非空 reason
持久化；孤立、重复或乱序撤单请求必须连同 cursor 原子回滚。自动 worker 只能消费该事实触发精确合同
撤单，不能从当前选中行或临时 CLI 参数推断撤单。一次循环允许先提交后撤销同一源请求，但提交与撤单
各自最多点击一次；SUBMITTING/CANCELLING 重启只允许对账，任一 AMBIGUOUS 必须在所有后续 UI 动作前
阻断。worker CLI 只能接受数据库、run、outbox、历史边界和轮询/新鲜度参数，不得开放方向、代码、价格、
数量、intent ID 或合同 ID 等临时交易字段。
物理 outbox v1/v2 升级到 v3 必须是单个原子事务：以版本更新 trigger 注入失败时，schema version、
新增列、唯一索引和既有绑定必须全部保持原样；解除故障后可恢复升级，重复打开不得再次改写结构或游标。
当前 outbox v4 新增独立 `remote_execution_facts`，v3→v4 同样必须原子、可恢复且幂等：版本更新被
trigger 阻断时不得留下半张成交事实表，解除故障后只创建一次。旧 staged intent、源账本绑定和 cursor 必须原样保留。
同一 order 的第二条 `ORDER_CANCEL_REQUESTED` 与孤立 order 一样必须整批回滚，不能覆盖首次 sequence/reason。
测试还要用“一个取消已 AMBIGUOUS、另一个提交仍 ELIGIBLE”的组合证明 worker 在任何 orders/prepare/submit/
cancel UI 调用之前全局熔断，而不是只跳过出错的那一笔。
所有最终动作按钮（确定买入/卖出、确认、委托）都必须先把 AX 按钮集合物化，按精确名称证明唯一后
保留对象引用，最终只对该引用 `click` 一次；不得在审计后再用 `click button "…"` 重新解析活集合。撤单行的
static text 同样要先物化，再在冻结列表中查找唯一合同号，不能直接对可能重建的 `static texts of row` 按序取值。
“只显示可撤委托”标签与复选框也必须分别物化后再按标签文案与相对几何唯一绑定，不得对会在查询间
重建的活 `static texts`/`checkboxes` 集合反复取 item，否则同一标签可能被错计两次。
`windows` 同样是会在委托或辅助窗口消失时重建的活 AX 集合；单独保存 `count of windows` 不等于冻结对象。
probe、orders、prepare、submit、confirm 和 cancel 六条脚本都必须先把窗口对象物化到 `frozenWindows`，
随后只允许按冻结列表的长度和 item 迭代；测试必须用“count 后辅助窗口消失”反例拒绝再次对活 `windows` 取 item。
行情 refresh 与 final probe 还不得用 `every window whose subrole ...` 这一可在解析时重新枚举的过滤
specifier：必须先一次 `get every window` 得到未过滤快照，再对冻结对象读取 role/subrole 并筛选。
模拟标志 static texts 也必须先物化，禁止 `item n of static texts of w`；窗口 3 或动态文本在两次读取间
消失只能使本轮证据失败/重试，不能越过唯一窗口与实盘负证据门禁。
提交证券代码编辑的 Tab 会使此前取得的 window/codeField/priceField 引用整体失效；refresh 在 Tab
之后不得读取任何旧引用。20 次有界等待的每一轮都必须从新的未过滤 window 快照开始，重新证明唯一
模拟窗口、重新物化三个字段并按几何确定 code/price，再读取 symbol echo 与只读行情 static texts。
每轮仍须重新取得全部 window/field/static 引用。已证明 role 的动态非证据文本可在本轮局部跳过；
模拟 marker、证券名称、行情 anchor/price、三个字段及必要按钮的最终唯一性或数量只要不完整，本轮或
Rust `verify_simulation` 就必须失败，不能用局部跳过补成成功证据。其他错误原编号重抛。
随后 final probe 的 buttons/text fields 也必须先在同一受保护轮次内物化，禁止先对冻结 window 读取
活集合 count、再按 `item n of buttons/text fields of w` 解析；否则辅助窗口在取证中途消失仍会产生
`Can’t get window 3 (-1719)`。
同花顺动态时钟等 AXStaticText 的 `name/value as text` 以及普通 text-field value 可能抛 `-1700`，
这些明确的文本身份转换才允许把 `-1700` 当作不可读空值，且必须先成功读取相同引用的 role；
窗口/集合/role/position/size 和安全按钮名称一律不得吞 `-1700`，尤其不能把不可读
的“转账/退出”按钮降级成不存在。
targetWindow 上“转账/退出/账户设置”还必须用独立 direct `exists → error` 形成严格负证据，不得依赖
可能跳过动态节点的枚举计数，也不得用内层 catch 把该证明失败降级为控件不存在。
窗口身份不能假设“模拟练习”是 `AXStandardWindow` 的直接 static text；同花顺 5.3.2 会把该标志放在
嵌套 `AXScrollArea`。只读 orders probe 可用 AppleScript 在每个冻结窗口的后代 `AXStaticText` 中识别 marker，
仍要求恰好一个窗口命中；该扫描只是订单表定位，不再承担资金动作的安全证明。
所有提交、确认和精确撤单在运行任何点击 AppleScript 之前，必须另外调用原生 macOS AX 验证器：它从
`AXWindows` 开始有界递归 `AXChildren`，要求唯一标准窗口含“模拟练习”，并在同一完整树中拒绝
“账户设置/转账/退出”。唯一性、节点角色/属性读取、深度或节点预算任一失败均必须 fail closed；
不得退回 AppleScript 的 direct-marker 快速路径。测试要区分“点击前允许的一次只读字段回读”与“资金动作脚本”：
验证失败后后者调用数必须为 0。
每个冻结窗口引用在使用前必须通过读取 `role` 证明仍存活；快照捕获与该 liveness 块都只能将 `-1719/-1728`
视为瞬时对象消失并重试/跳过，权限、脚本或其他错误必须保留原编号立即重抛。不得用裸 `try ... end try` 把任意安全错误降级为死引用。
精确撤单将唯一合同 row 冻结后，必须读取该引用的 position/size，证明 row 尺寸为正且中心位于
唯一模拟 `targetWindow` 边界内。最终触发必须是一次原生 macOS 双击操作（同一中心点的 down/up click-state 1
与 down/up click-state 2），整个撤单边界只允许调用一次该 double-click。不得用两次 AppleScript
`click cancellableRow`、两次 `AXPress` 或 `click at cancellableRowClickPoint` 仿造：实机合同 6200000001 已证明，
即使两次 AXPress 间隔 0.2 秒，仍可以无弹窗地失败；对同一 AX row 执行单次 native double-click 才产生精确的
“您确定要撤销这1笔委托?”。
从 AppleScript 返回的 `CANCEL_READY x y` 只是候选坐标：严格解析必须拒绝缺列、多列、小数、非数字和 i32 溢出，
且任一失败都不得调用 native click 或确认脚本。在发送第一个 CGEvent 前，原生验证器还必须从当前 AX 树重新找到
唯一模拟窗口并证明 `(x,y)` 仍在当前 frame 内；只重验 marker/实盘负证据不足以关闭窗口移动的 TOCTOU。
双击的四个事件（down/up state1、down/up state2）必须在第一次 `CGEventPost` 前全部创建并配置成功；不得在发出
第一个 down 后才分配 up 事件，否则中途失败可留下未释放的鼠标按下状态。这两项红测未关闭前，ignored 硬件提交—撤单
只能用于发现问题，不能作为安全发布签章。
精确撤单的预检、原生动作前身份验证与确认 sheet 三段都不得保存指向活 AX collection 的按项
AppleScript specifier。窗口、按钮、复选框、滚动区、table、group/header、任意 `UI elements`、订单 row、
row 内 static text、模拟 marker/实盘负证据和确认文案都必须先一次 `get` 集合，再把每个对象的 `contents`
物化到本地冻结列表，之后只索引冻结列表。禁止 `count of static texts ...` 后再次执行
`item ... of static texts ...`，也禁止直接在 `scroll areas of targetWindow`、`UI elements of ...`、
`buttons/checkboxes of ...`、`rows of matchedTable` 或 `static texts of candidateRow` 上惰性循环；AppleScript 会把
这类循环重新解释为 `item N of every ...`，实机已在 scroll area 7 重建时以 `-1719` 证明。动态控件导致
`-1719/-1728` 时必须在任何 native double-click 之前安全停止；修复期间绝不可再点击或提交，修复签章后
也只能对既有唯一合同重试一次 exact cancel，绝不能重新提交订单。
同一合同也适用于只读 `orders-probe`：自动测试必须逐层锁定 target-window buttons、filter static texts、
checkboxes、scroll areas、scroll children、table children、group headers、rows 与 cells 都经 `get` 和
`contents` 物化后才遍历，同时继续解析精确的 12/13 列白名单。
`-1719/-1728` 可安全失败或从完整快照边界重试，任何其他错误必须原编号抛出；脚本始终不得包含买卖确认或撤单动作。
点击唯一“委托”tab 后，旧 `targetWindow` 必须视为失效；自动回归必须在读取 filter/table 前锁定第二次
unfiltered window 快照、role/liveness、唯一模拟 marker 和账户设置/转账/退出负证据，禁止从点击前引用产出部分结果。
最终 `PROBE_SCRIPT` 必须在每个 bounded attempt 内从零计数可读字段，并在发布 attempt-local receipt 前要求
恰好三个；瞬时 0/1/2 字段须丢弃整个 attempt 并重试，不能把部分证据交给 Rust 后才报错。自动测试还要锁定
该完整性 guard 早于 `set out`，`-1719/-1728` 走有界重试，其他错误原编号抛出，且脚本不包含资金点击。
实机 `14811:14820` 已把另一边界定位到 `ORDERS_PROBE_SCRIPT` 获取 AXTable children：测试必须令同一个
post-tab attempt 从 unfiltered windows 一直覆盖 filter、table/header、rows/cells 和完整输出发布；任一 descendant
失效都丢弃 attempt-local 结果并从窗口重取，禁止只重试窗口后在重试区外继续扫描 stale table 引用。
委托 tab 重建后的一轮快照可能暂时找不到窗口或“模拟练习”marker；恰好 0 个匹配必须转换为
`-1719` 等明确的可重试缺证据并丢弃整轮，最多重取 3 次，连续 3 次仍为 0 才以稳定快照不可用停止。
恰好 2 个或更多模拟窗口、任一实盘负证据以及所有未知错误仍须立即 fail closed，不能借重试等待歧义
自行消失；此兼容只影响只读取证，禁止产生提交、确认或撤单动作。
原生双击后，`cancel_contract_with` 必须再次调用原生 `verify_money_action_window`，证明恰好一个模拟窗口且
所有标准窗口均无“账户设置/转账/退出”，然后才允许运行确认脚本。该脚本只能有界等待当前 focused element
本身成为 `AXSheet`，并在同一 sheet 上验证“警告”、恰好两个按钮、唯一“确认/取消”和精确
“您确定要撤销这1笔委托?”；不得再沿 `AXParent` 找父窗、读取 `AXWindow` 或遍历父窗 `entire contents`。
这是刻意的安全分层：全局窗口身份由紧邻确认脚本之前的原生证明承担，确认脚本只证明刚由唯一合同 row
双击生成的精确 sheet。两次原生证明、一次双击和一次确认的顺序必须静态锁定；任何一步失败都不得再次双击、
不得创建新订单。该模式只适用于已人工核实的模拟账户恢复，不构成实盘执行授权。
双击后最多等待 20 次、每次向 `AXParent` 上溯最多 8 层寻找确认 `AXSheet`；除继续等待所需的
`-1719/-1728` 外不得吞错。找到的 sheet 还必须证明隶属于同一个 `targetWindow`，不能只凭通用的
“撤销这1笔委托”文案确认另一个窗口的弹窗。硬件精确撤单 smoke 必须显式提供
`GRIDEDGE_THS_SMOKE_CONTRACT_ID`，并在动作前后都证明该唯一合同为零成交、最终全撤；锁屏时只能
fail closed，ignored 用例不能算作通用 CI 或发布硬件签章。
确认窗口的 descendants 可合法包含无 `name`/`value` 的 `AXImage`；不得用对 `entire contents`
批量读取 `name`/`value` 的方式要求每个无关元素都携带身份文本。必须先逐元素读取可证明的 role，
只对 `AXStaticText`/`AXButton` 逐个读取 name/value。单个属性缺失 `-1728` 可尝试另一属性，但安全相关
元素的两个身份属性都不可读时必须拒绝；其他 AX 错误必须保留原编号立即重抛。这一兼容不得弱化唯一
“模拟练习”、“账户设置/转账/退出”负证据、sheet 所属窗口或精确“撤销这1笔委托?”文案。
实盘控件不存在属于安全负证据，不能通过裸 `try` 跳过不可读的 static text 后推断为不存在；撤单动作前
必须用不吞错的直接检查或完整可读证明排除“账户设置”，锁屏、权限或 AX 树重建导致的未知结果必须终止整次动作。

仿真 live 调度的 5 秒只是行情采样 cadence：quote 路径只能读取、验证新鲜度、持久化原始证据并推进
bar builder，没有完整 bar 时不得执行 orders probe、outbox UI、permit、bar log 或核心服务写入。实机已观测
单次完整委托表约 27–30 秒，所以慢速委托对账只允许出现在 completed-bar 或显式结算边界。
该边界的固定顺序为：一次不可变 orders snapshot → 未知合同预检 → outbox 对账 → 仅当 UI 确实改变时再读一次
→ 以同一快照铸造 terminal permit → 先写 append-only bar、再调用核心 service → 处理新 outbox。重启时从 quote log
重建出的 completed bar 也必须走这一路径，不得预先写 bar log 绕过 permit。午间结算必须有按 trade date
的成功 latch，同日后续 15 秒轮询必须在任何 orders probe 之前直接返回。

整份 BUY 机械容量必须有独立的 coordinator 回归：当更深层已部署数量达到或超过浅层 mechanical
cap，浅层 `capacity` 必须 fail-closed 为 0，且不能返回虚构的 carry/tranche ID；服务据此记录的
`BUY_NO_AVAILABLE_CAPACITY` 不能生成 right/tranche/order。该边界与下面“同 birth 已被消费”的
优先语义必须分开验证。

同族 birth-depth 门禁必须单独覆盖：浅层 birth tranche 先 Defer，随后 transfer 到更深 right 并在那里
实际 consumed；即使原浅层 right 的 `exercised_quantity=0`，rearm 后重触该 birth depth 也必须记录
`GRID_LEVEL_SKIPPED(reason=RIGHT_ALREADY_EXERCISED_AT_LEVEL)`。测试须断言零新 mint/right/order、
该 birth tranche 的 owner 已变化但 consumed 保持 Q，以及 hot/cold rights/tranches/orders/lots/账户一致。

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

## 平台二进制升级门禁

`tests/platform_upgrade.rs` 使用真实 SQLite journal、初始部署和模拟 outbox，而不是伪造一个空状态：
首个 3.51 行情必须先形成唯一 `27500` 股 opening Paper 成交；outbox 从 sequence 0 追到当前 head，
该 intent 必须依次成为 `SUBMITTING → SUBMITTED`，并保存合同、唯一 fill、数量和成交额完整一致的
`FILLED` 远端事实。只有这个无未决、同 source/run 的 outbox 才可作为离线授权证据。

专项测试必须至少覆盖：

- 新平台无授权恢复失败且零写；授权落账后旧平台恢复失败且 head 不变；
- 授权进程退出后由目标平台自动激活，`AUTHORIZED/ACTIVATED` 序号相邻、causation 绑定；
- 激活前后 opening `27500`、现金、Paper 投影和排除恢复审计计数的业务状态 SHA 完全一致；
- 认证报告、目标 binary 或已落授权中的相应 SHA 被篡改均 fail closed；
- 仅 platform 之外的算法 manifest 字段漂移不得借升级授权通过；
- 两个 pending 授权、从旧节点分叉、回到历史平台的降级和同一激活重放均无法重建或续写；
- 第二次目标恢复不重复授权/激活，也不重复任何订单、成交或 UI 动作。

定向命令为 `cargo test --test platform_upgrade`。该文件是恢复与身份链的 focused 门禁；发布签章仍须
执行全量 Rust/Clippy、THS outbox 专项和端到端 replay，不能用 focused 结果代替。

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

## 同花顺后台激活门禁

生产 worker 只能通过受审 bundle identifier 调用 `/usr/bin/open -b` 激活同花顺，禁止枚举或
点击动态 Dock 辅助功能树。激活仅用于让应用可见；后续每一次只读采样或资金动作仍必须独立
完成唯一“模拟练习”窗口、版本、bundle identity 与实盘控件负证据验证。普通自动化测试静态
锁定此边界，防止 LaunchAgent 再次因 Dock 辅助权限停滞。

发布必须先运行 `deploy/stage_ths_sim.sh`：它从当前静止源码构建 release，在独立临时文件上仅签名
一次，校验 Identifier/TeamIdentifier 后按完整文件 SHA-256 生成不可变、内容寻址的 staging 文件。
认证报告和 `authorize-platform-upgrade` 必须绑定该精确 staging 字节。随后安装只能通过
`deploy/install_ths_sim.sh`，并显式传入 `GRIDEDGE_SIGNED_BINARY` 与 `GRIDEDGE_SIGNED_SHA256`；安装阶段
禁止构建或重新签名，只能在 SHA、codesign 身份验证后把冻结字节复制到 Application Support，
且不得直接覆盖正式路径：必须先复制到目标目录临时文件，对该临时文件重新验证 SHA、签名、
Identifier、TeamIdentifier 与安全字符串，再原子替换正式文件，并再次断言正式文件 SHA 等于授权
SHA。用 `cmp` 与 SHA-256 锁定身份，并对最终安装二进制执行产物级检查，
拒绝任何残留 Dock AppleScript、要求 `/usr/bin/open` 与受审 bundle identifier 均真实存在。
源码门禁通过但安装产物身份不一致时，LaunchAgent 不得加载。

LaunchAgent 的资金/行情脚本依赖 macOS TCC。未签名或 linker ad-hoc 签名的可执行文件在每次
构建后 CDHash 都会变化，即使设置页仍显示旧条目为开启，后台 `osascript` 也会被系统以
`-25211` 拒绝。安装脚本必须先把 release 产物复制到独立 staging 文件，使用固定
`com.gridedge.ths-live` identifier 和受审 Apple Development Team ID 完整签名，再把该签名字节
安装到 Application Support；随后必须用 `codesign --verify --strict`、Identifier、TeamIdentifier、
`cmp` 与 SHA-256 同时验证。Apple Development 的 CMS 签名本身可使同一未签名程序在重复签名时
得到不同完整文件 SHA，因此授权后绝对不得再次 codesign；普通自动化测试静态锁定
“一次签名并冻结 → 精确认证/授权 → 原样安装”的阶段边界，禁止把 ad-hoc 或二次签名产物装入
LaunchAgent 路径。
## MQTT 5 行情数据面

`deploy/market_data/ingestor/test_market_ingestor.py` 必须锁定 canonical JSON、topic/identity 绑定、
多来源 identity、u64 边界和非法 payload 拒绝；`publisher/test_shadow_publisher.py` 必须锁定完整行读取、
日志截断/换 inode 时 fail closed，以及 ACK 丢失重启后逐字重投。旁路发布的公共事件不得含
`account_marker`，该字段只可用于 Mac 本地模拟页面准入。

群晖部署验收必须使用真实 MQTT 5/TLS/QoS 1 依次证明：同一证券的两个来源各保留一条；原消息重发
只增加 duplicate；复用 source sequence 的不同内容只进入 conflict；缺失 `application/json` content
type 只进入 rejection。重启 broker、ingestor 与 PostgreSQL 后重复同一原字节，unique event 数不得
变化。匿名 publish 必须失败，宿主 5432 不得监听局域网。Mac shadow 端到端测试必须以本地 durable
outbox 人为恢复一条未确认消息，群晖最终只能增加 duplicate，且 source cursor 不倒退。
