# 历史自动化基线归档

归档于2026-09-07。本文件仅用于查历史签名身份、回滚及取证路径；其中PID、Goal状态和正在运行的试验可能过时。现状必须重新核验，本周节奏以2026-09-11-paper-delivery.md为准。

## 原运维提示

在 Asia/Shanghai 按受审 A 股交易日历每 30 分钟无人值守巡检并在已有授权内恢复 GridEdge-T Android 同花顺模拟网格与东方财富 MQTT5 行情；周末或休市日只做只读状态核验，不启动交易会话。当前权威源码工作区是 /Users/rjwang/Documents/ChatGPT/GridEdge-T，安装根是 /Users/rjwang/Library/Application Support/GridEdge-T。正式 run=ths-002256-20260819-grid15-opening-v1，symbol=002256.SZ，grid_ratio=0.015，Q=5500。用户持续授权控制 Chrome、精确受审行情页、Android 模拟盘与 paper worker；绝不触碰实盘，不得测试下单、补历史订单、伪造行情、重置 breaker、代解 CAPTCHA 或削弱安全门。

历史基线必须保留：2026-09-03 因 14:55-15:00 缺失为 P0，缺失 bar 不得补造。2026-09-04 09:25 本地正式 raw 从 source sequence 23308 跳到 27521，而 PostgreSQL 对 source instance 8101d65c-bdba-4de3-83e0-8983506f159e 连续；已用精确 committed payload 27521..27768 重建并隔离 replay，候选 raw SHA-256=0b68501e8f7404709a3fa7e339732106cf5563232cabb5ac1db050c8a9de9e95，旧 raw 备份保留。09:38:17 恢复 RUNNING 但错过五分钟预算，2026-09-04 仍是 P0 运行失败。以后同类 gap 只能按 event/source/sequence/time/topic/payload SHA 精确核验 PostgreSQL committed bytes，隔离 replay PASS 后原子替换；不得伪造、清空或跳过。

2026-09-04 收盘基线：最后完整 bar 为 14:55，三阶段序列 1781/1782/1783，ledger head/outbox cursor=1783/1783。Paper cash available=103418.530、frozen=0、fees=28.97；position/sellable=27500、today_bought/frozen_sell=0；open terminal orders=0。outbox 有一条历史 raw cancel_state=AMBIGUOUS，但不可变 FILLED fact 覆盖全部 27500，unresolved ambiguity=0。旧 runner 在正常退出后错误把 breaker 从“2026-09-04 2\n”写成 0；缺陷已用红测复现并修复，合法基线已恢复为精确“2026-09-04 2\n”，SHA-256=7902facaf7bf29d46856397acd9378959bb41074c30033f79c16e198a95c0344。不得清零、编辑、绕过或扩大预算；新交易日只允许 reviewed date rollover，成功退出必须逐字节保留 breaker。

0.6.51 已通过 189/189 扩展测试、全 Rust gate（ths_live 61/61）、Python 47/47、配置校验、233-event replay、两轮 fresh UUID 各 900 秒 loopback-only READ_ONLY E2E 及独立复审。两轮均 money=false、formal ACL deny、strategy_evaluated=false、Android/order/money actions=0，归档在 runtime/e2e-audits/20260904-0651-r1 与 r2。签名 worker SHA=149ca55f6ae239695f22e132d56b6cfcfd14514f230b4fe349e3fc9bb50e8422；validator=c819a13080368311e896ba9a58a0447e291aeee4c057d93d8c6a94673b8ac8fb；certification=059b85505d6b857187fd03767405500e0d189f8e9053cc8fd89913e84612bbb2。

当前安装/effective identities：worker=149ca55f6ae239695f22e132d56b6cfcfd14514f230b4fe349e3fc9bb50e8422，runner=6f0df0290db7cc84a1c0b5977817c346008f42be81e37070fd2306338d32662c，trigger-free plist=30aba4c8d97e7b86b3cb0093ebcfb7b64267ab3d2e9909523598707cb181c28d，guard=b5efcd83220bb981b6b2df57054adbe766c8b7c38aa66bcb8c11689d6378c536。平台 upgrade id=61f3801e-9238-5f36-afff-b2c6eab9b02d，授权/激活序列 1495/1496。append-only execution binding rev15 identity=1be375fb951fd03a25f5f98100d43a746bea5f1ff7619f16ae73fba3b483bd53；与 rev14 相比只能 runner_sha256 不同。LaunchAgent label 必须 absent（rc113）；唯一启动/重启 owner 是 tmux -L gridedge_codex 的正式 guard。收盘后 guard/worker absent 是正常状态；不得在非交易时段为验证而启动正式 worker。

生产 Chrome 注册目录 /Users/rjwang/Documents/ChatGPT/GridEdge-T/build/gridedge-web-market-extension 已正常重载；popup 与 Secure Preferences 均核验 manifest_version=0.6.51，reviewed_build_id=collector-0.6.51-stale-trade-coverage-only-v1。唯一精确 URL https://quote.eastmoney.com/f1.html?newcode=0.002256 必须持续保留。heartbeat 先提交 status-only observation，只有精确 STALE_TRADE_COVERAGE 可在 session-persisted 20 秒冷却中触发一次受审 tab reload；INITIALIZING_HISTORY、delivery 失败、异常和 URL TOCTOU 都不得 reload。不得代解 CAPTCHA；若出现真实用户在场边界，保持订单 fail closed 并一次性报告所需动作。

下一受审交易日（当前预期 2026-09-08，最终以 reviewed calendar gate 为准）的执行基线：09:00 本巡检必须实际建立并核验唯一 app-independent guard，不能只写计划；09:25 前完成 AC/电量/屏幕、Chrome 精确 URL/实际 loaded runtime/build/倒序状态/CAPTCHA（若仍为0.6.51或仍有CAPTCHA，立即一次性通知用户本人完成CAPTCHA并按已审说明重载0.6.53，保持READ_ONLY）、MQTT 与 PostgreSQL committed ACK、Android 只读 identity/orders/fills/cancellable、installed/effective SHA、ledger/outbox、Paper/远端、未知合同与 breaker。09:00 heartbeat 只有在这些预检门全部提前通过时才可在09:25前结束；否则必须在同一轮用每次不超过60秒的有界检查保持活跃至09:25。用户在场边界可以立即通知且绝不代操作，但不能因此丢失09:25截止核验；若届时仍未就绪，立即标为P0 readiness breach并报告缺失事实、首次恢复动作及仍在执行的恢复，避免对同一用户动作重复通知。guard 在调用 runner 前独立验证唯一 ready emulator、Chrome 进程、MQTT TCP；普通依赖缺失只 defer 且 breaker 原字节不变。worker 在 Android 只读 startup preflight 后、market/service 前原子发布随机握手；runner 只对握手前真实 Android preflight 失败计 breaker，握手后行情故障不计。

09:30-11:30、13:00-15:00 worker 应可用。启动和 reconnect backlog 只能 READ_ONLY，不补历史订单；只有 fresh contiguous current trade、PostgreSQL exact committed ACK、terminal Paper account reconciliation 和当前完成五分钟 bar 的 MARKET_DATA_RECEIVED→MARKET_BAR_DECISIONS_COMMITTED→MARKET_BAR_PROCESSED 全成立才 RUNNING。用户激活后的生产 0.6.53 在页面 available 后 45 秒内必须产生首个 current reviewed source observation 和精确 PostgreSQL committed ACK；09:35 前必须处理首根完整 post-open 五分钟 bar。倒序状态、45 秒 ACK 和 09:35 bar 都属于下一真实窗口证据，不能用收盘后 status heartbeat 代替。

普通 Chrome/tab/MQTT/DB/emulator/guard 故障须在同一巡检自动恢复，单次检查不超过 60 秒；五分钟预算错过须报 P0 但继续恢复。只有错误/非模拟账户、实盘证据、future/gap/reorder、ledger/outbox 损坏、artifact 不一致、未知合同/未解决 ambiguity 或 Paper/远端不一致才保持停止。若 emulator 启动损坏，保留数据做受审 cold boot，绝不 wipe-data。候选验证必须用隔离 namespace/端口/状态且 formal topic ACL deny，不得替换健康正式 worker。

每轮核对本地时间、供电、collector 实际 runtime version/build/policy/provider、source sequence/watermark、PostgreSQL committed ACK、冲突/拒绝、最新完整 bar 与三阶段、cash/current observed minimum、position/sellable、open/unknown/unresolved ambiguous、ledger head/outbox cursor、installed/effective/runner/plist/guard/execution identity、breaker、guard/worker mode和实际恢复动作。交易时段结束前明确回答：worker 是否 RUNNING、是否处理当前完成 bar、若否是哪条 safety-critical fact 阻止恢复、还有什么动作正在执行。状态完全不变且无需动作时静默；发生 P0、恢复完成、失败或需要用户动作时通知。

2026-09-07追加安装基线：修复任务01a079b7-9292-7753-93ae-8b32104039e2仍active，审查工作树/Users/rjwang/.codex/worktrees/7e73/GridEdge-T，已审ops源码同步到正式源码目录deploy/ops。bar-mode复核版supervisor已安装（合法取消终态、每根bar按当时三阶段mode报告）；唯一tmux -L gridedge_codex session ths_session_supervisor，PID22058、父进程tmux server，09:00/12:55按受审日历自主恢复普通Chrome/AVD/guard及已证明本地raw committed缺口。manifest config/session-supervisor-manifest.json SHA=4b1b9ee226bd97a5704c4f6238836e63f7f689a49b5572c778dc5b5b84df6ae9；supervisor SHA=8df496ae16a57d43ef42f609ac2a2cfd747ac65314c5cdae9772ea96ac6addcd；launcher SHA=a5d25ed6402db57efa13973c0e764bb45369f9d733bfdfcc0f70e801067ac35d。先核验runtime/session-supervisor-heartbeat.json推进、manifest/installed一致及唯一owner，不得另建第二owner。正式worker50053自09:05:46未被替换，午休head/cursor1902/1902，rev15/breaker原bytes不变，AC100%。ops36/36，Rust483PASS/0FAIL/2原硬件资金smoke忽略。详情docs/reviews/2026-09-07-operational-supervisor-review.md。

raw修复必须worker=0、coordination+maintenance独占、guard停止；PG只读exact committed逐event/source/sequence/time/topic/SHA/QoS确认，只插入缺失中段且保留已知原行及合法重复。固定signed replay binary fe1ec2a7864f855f26eb3d282859ad32ee1ab0460bcac88b8cf21afc63f6d66b逐日语义PASS，原始证据/候选fsync、二次quiescence后atomic replace；仍由原guard READ_ONLY启动、不补历史订单。禁止对正式raw/AVD/订单/breaker作故障注入。

collector仍是正式0.6.51。0.6.53候选198测试PASS，最终R1已900秒PASS；独立PID90006/tmux closure_e2e_0653承载13:00 R2闭页恢复，进度/tmp/gridedge-20260907-closure-evidence/acceptance-progress.json。R2、最终review、正常生产加载及真实新bar通过前不许称0.6.53已上线；不得重复候选或抢占健康worker。先读取修复任务，再按已审证据续接。

开放生产缺陷：seq35808仅证明11:29:48 trade coverage（recv11:30:14），最后完成bar11:25；11:25–11:30桶无法合法完成、策略未评估，与Sep3/4最后桶同类。HTTPS Date/本地钟/午休到点/0.6.53/副守护发布均不能当完成证明。docs/incidents/2026-09-07-missing-segment-completion.md保留独立结论；已询问可用获准权威segment-end源。保持OPEN并报告可用性失败，不造11:30/15:00bar。副守护证明dependency supervision，不证明交易就绪；禁用扩展尚无已实现外部恢复路径。

12:41模式审计更正：上午23根processed bar仅19根在RUNNING接收，10:35/10:50/11:15/11:25四根走READ_ONLY恢复，禁止用事后RUNNING与三阶段齐全倒推strategy evaluated。必须报告latest_bar_stage_modes及latest_bar_strategy_evaluated；false或unknown不是完整交易可用。四次source observation均不足0.75秒，前序gap不足7.01秒，但trade coverage超过60秒，触发EASTMONEY_SOURCE_OBSERVATION_CATCHUP；原始source证据已归档，不能归因没机会，也不放宽门槛或补历史策略订单。

2026-09-07 13:18发布边界：两轮0.6.53各完整900秒按既有bar_count!=0合同PASS（R1=2bars，R2=1bar，R2闭页自动恢复、最大COMMITTED gap22.006824秒），独立最终复核中。正式registered build仍0.6.51，hash4881a04687535be542b99906a9314e4d5d057b10c932496e49c9592011f92a6b，未上线0.6.53。CUA打开Chrome扩展管理页被Browser URL security policy拒绝，并禁止alternate surface/raw CDP/命令绕过；不得重试其他通道加载/刷新扩展，等待用户手动正常重载。候选仅冻结准备，不能由PASS推称已发布。正式13:05/13:10三阶段全RUNNING；missing-segment completion仍OPEN。

13:24终审更新：独立collector候选SCOPED PASS已收到，详见docs/reviews/2026-09-07-collector-0653-final-review.md。R1真实900.469秒/2bars，R2真实900.163秒/1bar；既有门槛bar_count!=0不变。两轮结束后隔离进程/监听已退出；13:22事后只读PG查两候选source实例均0行，不能称同期tripwire快照。正式source代码已同步0.6.53，但Chrome registered build仍0.6.51未改，禁止通过build/文件替换/其他UI形成绕过Browser URL policy的间接激活。手动发布材料在安装根runtime/e2e-audits/20260907-recovery-closure/collector-0.6.53-manual-release/README.md，等用户手动复制并正常重载，再做只读核验。原worker50053持续处理13:20bar(1921/1922/1923全RUNNING)，head/cursor1923/1923；副守护22058唯一。一次13:16续接工作已完成验证、停用一次唤醒，后续由既有巡检与用户回复续接。Goal仍active，末桶权威completion问题仍OPEN。

2026-09-07 13:29历史Goal状态：旧修复任务01a079b7-9292-7753-93ae-8b32104039e2当时因同一外部阻塞连续三轮按规则标为BLOCKED，未完成；这只是历史记录，不得覆盖当前任务状态。需要用户手动激活已审0.6.53候选，以及可用权威末桶completion来源；此前active表述仅属历史。这不停止正式paper运行或本巡检：worker50053和副守护22058仍在，13:25bar全RUNNING、head/cursor1929/1929。保持既有巡检ACTIVE并恢复普通运行故障；不要重复发起无状态变化的发布尝试，不绕过Browser URL policy。用户完成手动操作或提供source后再续接实际验收。

2026-09-07 15:25 收盘权威基线：13:56:05 journal seq1957 因 EASTMONEY_SOURCE_OBSERVATION_TIMEOUT 进入 READ_ONLY；最后正式 committed source seq36653 仅覆盖到13:54:54。14:00巡检刷新并重建唯一受审页后出现滑块CAPTCHA，正式0.6.51仍未恢复投递；严禁代解或绕过。最后处理13:50 bar，三阶段1949/1950/1951均READ_ONLY、latest_bar_strategy_evaluated=false；15:05:06 worker正常退出。收盘ledger/outbox1957/1957，cash103418.530/frozen0/fees28.97，position/sellable27500，开放/未知/未解决订单0；breaker、rev15、installed/effective SHA不变。当天因后半段没有可执行行情判定为P0运行失败。精确页已mark handoff，用户仍需完成CAPTCHA并通过Chrome正常GUI重载已审0.6.53，Browser URL policy禁止任何命令/CDP/替代表面绕过。0.6.53已198/198与两轮900秒隔离PASS，但正式registered tree仍0.6.51；不得误报已发布。

下一受审交易日当前预期2026-09-08（仍以reviewed calendar gate为准）：长期tmux ths_session_supervisor继续作为app-independent 09:00/12:55 owner。09:00 heartbeat必须首先核验唯一supervisor、CAPTCHA与实际loaded collector；若仍需用户在场，立即一次性通知并保持READ_ONLY，不能等09:30。用户解除CAPTCHA并正常重载后，必须核验实际0.6.53 runtime/build、fresh V3 source、精确PG COMMITTED ACK、当前contiguous coverage、Android/Paper终态和一根三阶段全RUNNING的当前完整bar，才可报告恢复。若0.6.53实际验收失败，保持READ_ONLY并一次性通知用户按已审README从持久0.6.51回滚目录/Users/rjwang/Library/Application Support/GridEdge-T/runtime/release-backups/collector-0.6.51-4881a04687535be542b99906a9314e4d5d057b10c932496e49c9592011f92a6b通过同一Chrome扩展条目GUI回滚；不得以命令/CDP/替代表面重载，回滚后仍须通过实际runtime/build、行情ACK、账户终态和当前完整bar门禁。午休/收盘末桶权威completion缺口仍OPEN；不得用墙钟、HTTP Date、可见15:00 DOM行或收盘后历史补单关闭。

## 原盘后修复提示

在 Asia/Shanghai 的A股交易日收盘后，进入 /Users/rjwang/Documents/ChatGPT/GridEdge-T，修复当天自动化巡检、正式paper worker、Chrome东方财富采集、MQTT/PostgreSQL、Android同花顺模拟盘、账本/outbox、trusted guard或部署流程中发现的所有未解决问题。

开始工作时必须调用 Goal 工具创建一个明确目标：修复当天发现的全部问题，完成回归、审查、构建、签名/同run升级（若需要）和下一交易日预检准备。读取当天heartbeat、日志、incident/review/e2e证据和项目AGENTS.md/GOAL.md，先列出每个问题的影响、根因、当前缓解和完成条件。不要把“已报告”“候选已构建”“测试仍在运行”或“等待下次巡检”视为完成。

对每个可复现缺陷先新增独立红测，再做最小修复；执行cargo fmt --check、strict clippy、相关测试、全仓测试和端到端回放。需要review的安全、行情、执行、账本或发布变更必须取得规定的独立review。候选、回放和shadow必须物理隔离broker/topic/client/source/raw/ledger/outbox/profile，绝不能匹配正式topic、触碰实盘、创建测试订单、重置安全熔断或伪造盘后行情。健康正式artifact不得被候选验证替换。

收盘后可以完成确定性修复、回放、审查、build/sign、same-run发布准备和非资金只读检查。若真实交易窗口证据是唯一剩余条件，必须把精确证据、自动启动方式、fresh隔离环境和09:00/09:30执行动作写入次日可执行基线，并证明由app-independent guard或有效自动化承载；不能只留文字承诺。若安全允许并且无需交易窗口，可在当晚完成部署；正式资金worker保持收盘状态。

维护当天P0时间线和项目级回归/操作契约；相同类别第二次发生时必须补自动watchdog/recovery path。完成后核验账本/outbox、Paper/Android终态、未知/AMBIGUOUS订单、artifact身份和证据文件。只有所有可在收盘后完成的工作都已真正完成、没有遗漏的普通可恢复故障、剩余项仅为清晰记录且已自动安排的交易窗口证据时，才调用 Goal 工具将状态标记complete；否则继续工作，不要提前结束或把Goal虚假标记完成。最后用中文给出修复结果、验证证据、发布状态、未完成的live-only证据和次日自动动作。

