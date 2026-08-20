# GridEdge-T 核心目标

> 大道至简：机械网格负责授予权利，决策算法负责选择是否行权，账本负责如实记录每一份权利的来龙去脉。

## 1. 平台是什么

GridEdge-T 是量化决策算法的可靠运行平台。机械网格不是自动下单指令，而是在特定网格点产生一份有明确来源和数量的买入或卖出授权。

决策算法对每份授权只做一件核心事情：

- `Exercise(q)`：本次行使数量 `q`；
- `Defer(q)`：本次不行使数量 `q`，按规则递延到后续网格。

在一个决策点必须满足：

`授权数量 = 本次行使数量 + 本次未行使数量`

“未行使”是算法主动保留授权额度的决策，不等于订单未成交、成交失败、撤单或平台风控阻断。

平台使用自定义权利单位“份”。`1份` 必须等于配置中的固定股票数量 `standard_quantity`，且 `standard_quantity` 必须是交易 lot size 的整数倍。股价变化只改变成交金额，绝不能改变一份包含的股票数量。

例如 `standard_quantity = 6000` 时，任何买入或卖出网格的 `1份` 都是 6,000 股；两份是 12,000 股。递延、行权、撤销、订单和成交始终按股票数量守恒。金额只用于现金结算、费用、风险检查和收益计算，不得作为机械授权或算法行权的基本单位。

`份数 = 股票数量 / standard_quantity`

现行决策必须先把精确 lot/tranche 来源支持的股数 `C` 分成整份授权 `A` 与平台残余
`P`，再让算法和平台分别处置：

```text
C = A + P                 P = C mod standard_quantity
A = E + D                 I = E - B                 R = D + B
```

`A/E/D/B/I/R` 必须全部是 `standard_quantity` 的非负整数倍，且
`0 <= P < standard_quantity`。`E` 是算法主动行权，`D` 是算法主动递延，`B` 是算法
选择后被平台阻断的整份数量，`I` 是最终 OrderIntent 整份数量，`R` 是本次仍未形成意图的
整份数量。`P` 不是 Defer、Blocked、撤销或到期；它始终按股保留在原始 tranche 中，未来
只能与其他同源合格残余共同组成完整一份。OrderIntent 总量必须是完整份，具体 lot/tranche
分配和 Paper 部分成交仍可按 lot size 记录，例如 `700股 + 800股 = 1份1500股`。
算法审计字段 `action` 必须描述上述离散后的整份结果：只有 `E > 0` 才能记录
`EXECUTE`；即使连续比例 `alpha > 0`，若向下离散后 `E = 0`，也必须记录 `SKIP`。
现行合同以 typed `E/D` 为数量真值；`alpha` 只保留为算法解释信号，不能覆盖或放宽整份方程。
SELL 的平台批准还必须与不亏 lot 计划形成闭包：`I` 是 `0,Q,…,E` 中风险检查通过、且
同一次 canonical allocation 恰好覆盖全部 `I` 股的最大值。不得先规划较大数量、把其中
安全股数向下取整后再对较小数量重新规划；最低佣金等非线性费用可能使两次结论不同。
批准数量与 lot/tranche allocations 必须由同一 helper 同时返回并贯穿预留、订单、成交和账本复算。
BUY 的现金批准与预留也必须使用同一份订单级累计费用函数：预留金额等于整份订单在不利滑点
价格下的总成交额，加上 `max(累计成交额 × commission_rate, minimum_commission)`，最低佣金
对一个订单只计一次。Paper 即使把 `Q` 拆成 `700+800` 等多次部分成交，也只能在每次 fill
收取“累计应收佣金减已收佣金”的增量，绝不能按预计或实际 fill 数量重复预留、重复收费。
平台批准、订单预留、current-schema 账本复算和恢复必须共享这一口径；恰好够支付一次最低
佣金的现金必须批准，少一分钱必须整份阻断，不能由联合伪造 `B/I`、reservation 与 intent 绕过。
现行 Blocked/Reserved 处置使用 schema 4，OrderIntent 使用 schema 7；schema 7 必须以 typed
origin 区分普通 GridRight 与一次性 InitialDeployment，并把初始部署绑定到同批审计结果及尚未
Processed 的精确行情 event/hash。历史 schema 6 保留单最低佣金语义只读重放，schema 3 处置与
schema 5 intent 必须按当时“双最低佣金”的事实只读重放，不能用新口径改写，也不能继续新写。

Decision Contract v4 使用 `RESOURCE_AWARE_WHOLE_Q_V1`，把决策前已知的资金/库存不足与算法
主动递延分开。令 `Q=standard_quantity`、`C=P+A`、`P=C mod Q`，并定义资源最多支持 `M`
份，则：

```text
A = B0 + X                 X = min(A, M×Q)
X = E + D                  E = I + B1
R = D + B0 + B1            C = P + I + R
```

`B0` 是算法调用前已知的平台资源阻断，`B1` 是算法选择后重新审批产生的阻断；二者都不能
伪装为算法 `Defer`。`A/X/B0/E/D/I/B1/R` 全部是 `Q` 的非负整数倍。页面空心标记只显示
`D/Q`，`B0/B1`、T+1、底仓、冻结和不保本数量必须独立审计。

市场条件只使用当前机会之前已经 `Processed` 的最近 20 根行情。少于 20 根时不得行权。
固定参数为 `short_window=5`、`volatility_floor=0.005`、门槛 `0.60`，市场分数是规则分而非
概率：

```text
depth = abs(grid_index) / trade_levels
r5 = last_close / first_close_of_last_5 - 1
VWAP20 = sum(close×volume) / sum(volume)       # 总量为0时退回close简单均值
v20 = last_close / VWAP20 - 1
scale = max((max(close20)-min(close20))/first_close20, 0.005)

BUY:  trend=clamp(-r5/scale,0,1), location=clamp(-v20/scale,0,1)
SELL: trend=clamp(+r5/scale,0,1), location=clamp(+v20/scale,0,1)
m = 0.35×depth + 0.40×trend + 0.25×location
passed = m>=0.60 and (trend>0 or location>0)
```

资金和库存绝不能进入 `m` 或单独触发 BUY，只能在市场门槛通过后降低速度。资源节奏固定为：

```text
rho = M/(M+4)
g = 0.50 + 0.50×rho
pace = min(0.50, m×g)
e_units = min(X/Q, 2, max(1, floor((X/Q)×pace)))   # 仅在passed且X>0
E = e_units×Q, D = X-E
```

因此单个机会最多行权 2 份，`X=Q` 且市场通过时恰好行权 1 份；资源再多也不能让失败的市场
条件通过或一次用尽大量授权。`alpha` 仅作解释，必须同时记录整数 numerator/denominator，数量
只能以 typed `E/D` 为真值。

BUY 可买份数使用 `F=cash.available-cash.frozen`、不利价格、一次订单级累计佣金和仓位差值
精确求最大整份。仓位差值必须以 `position_exposure=position.total+pending_buy_quantity`
计算；所有未完成 BUY 的剩余数量均计入 exposure，target/max headroom 都不能因订单尚未成交而重复
授权连续接盘。SELL 必须逐候选份数调用同一个 canonical no-loss planner，允许多个合格 lot
（如 `700+800`）共同组成一份，不能假定费用线性。Decision v4 必须冻结完整资金/库存证据、
20 根行情身份和特征、`A/X/B0`、`rho/g/pace`、整数目标及算法身份，Ledger 从同一已处理前缀
独立重算。Decision v2/v3 保持原始 hash 和数量解释，只读重放，绝不能套用 v4 规则。

## 2. 账本的职责

账本必须记录**每一次机械网格机会**，不以是否最终产生交易为条件。每次进入网格点都形成一个具有稳定 opportunity ID 的追加式日志链：

`Touched → Granted(C) → Partition(A + P) → Decision(Exercise/Defer) → Blocked/Residual/Carry/Revoke/OrderIntent`

完全不交易的网格点同样必须留下 `Touched → Granted → Defer`；不足一份且算法未被调用的机会必须留下 `Touched → Granted → Residual(P)`；平台阻断的网格点必须留下独立的 `Blocked`。重复触碰但没有构成新 excursion 的网格点必须明确记为 `Skipped`，不能静默消失。状态是这些不可变事实的确定性投影，而不是可以覆盖历史的孤立快照。由此，页面看到的每个点、重启后的状态和离线审计使用同一事实来源。

SAFE 和 READ_ONLY 只禁止算法交易，不能关闭机械机会审计：这两种模式下仍须计算真实 crossing，并留下 `Touched → Skipped(SERVICE_MODE_*)`。STOPPED 不接收新行情。每根已接收行情只有在所有机械机会都得到唯一处置后，才能写入 `DecisionsCommitted → Processed`；同一机会、同一阶段和同一行情均只允许出现一次。

现代网页必须能够分页还原所选已处理前缀内的**全部**机械机会，而不是只显示最近事件或仅显示
产生权利的点。每个 `Touched` 都要以同一 stable opportunity ID 显示其唯一 `Granted` 或
`Skipped` 结论；Skipped 必须显示原因。页面总数必须满足
现行账的 `Touched = Granted + Skipped`，且 `Granted = Decision`。冻结 schema1 账若缺少稳定
correlation、无法把同一时刻同一网格的处置唯一绑定到 Touch，必须显示 `LEGACY_UNBOUND` 并满足
`Touched = Granted + Skipped + LegacyUnbound`，不能猜测或补造历史决策。`Granted` 只是授权阶段，
页面还必须显示唯一 `Reserved/Deferred/ResidualHeld/Blocked` 终态、原因、算法是否成功和 partial
block 的分类股数与 lot 来源。自动播放、刷新、服务重启和多 run 切换不得造成重漏，分页固定在
同一个 snapshot sequence，绝不能读到尚未 Processed 的行情机会。

每一份固定数量授权都必须有可追溯的来源。账本分别记录授权的铸造、转移、保留、预留、行使、撤销和到期，并保持逐数量守恒：

`minted = available + reserved + consumed + revoked + expired`

递延只改变权利归属，不凭空增加股票数量；价格反转只撤销本轮最深网格新增且尚未行使的边际份额，已经行使的份额永不回滚，较浅层递延份额不得被误撤销。

同一周期内，同一买入网格一旦有任何数量已经行权并形成持仓暴露，就不得因价格反弹后再次下探而重新铸造该网格的买入授权；必须等本周期持仓闭合并开启新周期。只有此前授权完全未行权、且边际份额已被反转撤销时，新的价格 excursion 才能生成新的 token。该规则必须由机器案例覆盖，防止同一个 `-1` 网格连续重复买入。

## 3. 决策与执行必须分开

算法选择行权、平台生成订单、经纪端接受、部分成交、完全成交、拒绝和撤单是不同事实，必须分别记账。

- 算法是否使用授权，看 `Exercise/Defer` 决策；
- 实际是否成交、成交多少，看订单与成交事件；
- 订单未成交不得被解释为算法未行权；
- 平台风控阻断不得被解释为算法主动递延。

网格交易底座的核心 case 边界止于“平台依据算法决定形成合法的订单意图”。订单提交后是否被经纪端接受、部分成交、完全成交、拒绝或撤单，属于独立执行模块的可靠性问题，不参与机械网格算法本身是否正确的判定，也不得反向改变已经记录的 `Exercise/Defer` 选择。执行模块仍保留自己的状态机与自动测试，但权利路径 case 不为成交概率组合做穷举。

## 4. 页面圆点的唯一语义

“行情与回放进度”中的每个圆点代表一个机械网格决策点，用来回答：机械网格授予额度后，算法最终选择了什么？

- 红色实心圆：算法选择行使的买入授权，数字是选择行使的“份”数；
- 绿色实心圆：算法选择行使的卖出授权，数字是选择行使的“份”数；
- 红色空心圆：买入授权中算法本次主动未行使的“份”数；
- 绿色空心圆：卖出授权中算法本次主动未行使的“份”数；
- 部分行权：同一决策点并列显示实心的已行使部分和空心的未行使部分。

每个决策点同时展示“本次行权份数”和“剩余可行权份数”，包括数值为 `0` 的一侧；行情线必须位于圆点下层，不得遮挡决策标记。

权利表必须与同一决策点数量事实一致：现行已决权利直接显示冻结的 `A / standard_quantity` 整份与 `P` 残余股，平台 `B` 和 T+1/风险/保本前置阻断分别标注；未决 SELL 读取 eligible 股数，未决 BUY 读取 available 股数。历史 BUY 的金额预算只能显示为货币，绝不能用字符串真假值把金额、BUY 股数和 SELL eligible 股数互相回退或把正常 SELL 容量显示成 0。

空心圆绝不表示订单未成交或成交失败。订单执行结果应在独立的订单/成交视图中表达。

## 5. 不可突破的交易原则

- 永远不卖出任何亏损的份额；保本判断必须逐 lot slice 计算并包含买入费用、卖出费用、税费和不利滑点。
- 决策不得使用当前时点之后的行情。
- 重启、回放、故障恢复和重复输入必须得到确定且幂等的业务结果。
- 账本是唯一事实源；任何派生状态都必须能够从账本重建和对账。
- 当前平台只用于研究、回测和 Paper 交易，不接入实盘经纪商。

macOS 同花顺连接只允许控制客户端内明确标记为“模拟练习”的 Paper 账户。页面上仅存在“模拟”
标签并不足以证明安全：每次读取或写入前必须同时锁定应用 bundle、已审版本、唯一模拟账户标志、
委托控件结构，并确认银证转账、券商退出和账户设置等实盘控件均不存在。任一标志、版本或布局
变化都必须 fail closed。只读 probe 和 dry-run 不得填写表单或点击提交；后续执行层只能消费已经
持久化的 GridEdge 订单意图，并必须具有本地 outbox、字段回读、单笔上限、委托结果核对与重启
幂等，不能把 GUI 超时当成未提交后盲目重发。实盘标签或账户永远不在允许范围内。
外部模拟执行还必须使用当前市场事实：源 `ORDER_INTENT_CREATED` 的 event_time 只能在明确的短
时限内使用，未来时间和十分钟以上的历史事件永远不能进入 UI，三年 replay 即使格式完全合法也
只能被记录或审计，不能成为当天委托。
同花顺页面行情采样只能在已审模拟窗口内重新写入证券代码以触发报价刷新；不得填写价格/数量或
点击订单动作。每个样本必须保存 UI 证据读取前后的本地时间、精确价格和 UI 证据哈希；证据读取
跨越五分钟桶或交易时段边界时整条样本拒绝，不能把边界后的价格倒记入前一根。样本只能在 bundle、
版本、模拟账户标志和规范哈希全部复核后进入聚合，再以已完成的交易时段桶
聚合 OHLC；不得编造成交量、午休 K 线或缺失桶。陈旧判定必须依据每次完整安全取证的
`observed_at` 严格推进，而不是要求价格发生变化：持续数分钟同价但证据时间推进仍是新行情；重复、
倒退或不推进的旧证据即使价格变化也必须停机，并从 append-only 历史恢复最后已接受证据边界，失败
重启不能重新获得宽限期。quote/bar 日志中的时间倒退、重复或同时间内容冲突必须在服务/下单前
fail closed。首日部署以 `minimum_free_cash` 作为一次性硬边界，至少保留
一半初始模拟现金；初始部署完成并追加唯一、完整绑定平台/初始部署/Paper/outbox 前缀的
`ONGOING_RESOURCE_POLICY_ACTIVATED` 后，`minimum_free_cash` 只作为报告指标，不再阻断持续 BUY。
持续 BUY 使用全部可用现金，只有足以覆盖一整份 `Q` 及费用才获批；持续 SELL 只由合格可卖库存决定，
不受现金余额影响。`max_position`/target 只保留数值安全上限，不得成为盈利后持仓增长的业务上限。
正式 002256 模拟运行在首个有效 3.51 元行情上只允许一次账本化初始部署：20 万元初始现金、10 万元
现金底线和 `Q=5500` 精确选择 `5Q=27500` 股，机械上即使开放 6 份也不得越过现金底线。该行为必须
先写 `INITIAL_DEPLOYMENT_EVALUATED`，再以 schema-7 typed origin 形成唯一 intent，经 Paper
成交后才可由 outbox 派生 UI 动作；拒单必须零 UI，部分成交必须守恒，重启不得重做。初始持仓 lot
不得污染 grid rights 或 `historically_executed_levels`，当日受 T+1 阻断，下一交易日才可成为 SELL
tranche 来源。
连续模拟执行必须以独立 durable outbox 的 cursor 追随源账本：只有配对的当前
`ORDER_INTENT_CREATED → ORDER_SUBMITTED` 可触发一次模拟提交，只有同一 order 的当前
`ORDER_CANCEL_REQUESTED` 可触发一次精确合同撤单。worker 重启只能从 SUBMITTING/CANCELLING
继续核对，不能再次点击；任何 AMBIGUOUS 都必须阻断后续自动 UI 动作，直到人工对账。
委托页中合同行消失不能被解释为撤单成功。若撤单已进入 AMBIGUOUS，只有读取已审模拟
账户的成交页，以唯一成交编号按合同号精确聚合，并证明 symbol/方向一致、累计数量恰好等于
该 durable intent 时，才可以把该合同以
独立的 `FILLED` 远程终态收口。该事实不得伪记为 `CANCELLED`，不得再点击撤单；相同证据重启
必须幂等。部分成交、超量、跨合同混合、重复成交编号或无法表示的价格仍必须保持未解决并 fail closed。
已完整成交但价格劣于 Paper 保守加权价时，客观远端事实仍记为 `FILLED`、永不再撤，但 live permit
必须继续阻断；两个状态不得混为一个。上述恢复和阻断全程保持零资金 UI 动作。
委托页的“成交价格”不可当作精确加权均价：它只需为严格正数并落在该合同成交明细的
`[min(price), max(price)]` 内。精确成交数量、成交额和加权均价只能由成交页每个唯一 fill 求和。
跨交易日使用“今天”筛选时，昨日合同可以不再出现在委托表，但该缺席只对已经持久化为独立
`FILLED` 终态、且完整规范成交证据仍能与源账 Paper modeled fill 对账的合同成立；缺少、损坏或
篡改证据，以及任何 SUBMITTED/CANCELLING/AMBIGUOUS 等非终态合同仍必须阻断。若昨日合同仍出现，
仍须逐字段匹配 durable intent。跨日缺席只是只读审计规则，绝不能触发重复提交、撤单或修改源账。
同花顺 5.3.2 委托表允许且只允许两个受审列布局：原十二列，或在“合同编号”与“委托属性”之间
增加只读“申报编号”的十三列。所有列都必须先物化并按精确表头名映射；申报编号必须保留用于审计，
但订单提交、远端成交对账与精确撤单的唯一身份始终是合同编号。缺失、重复、乱序关键列或出现任何
其他未知列都必须在资金动作前 fail closed，不能按固定 ordinal 把申报编号误作合同编号。
订单只读探测定位该 12/13 列表格时，委托 tab 按钮、筛选标签 static texts、checkboxes、scroll area、
逐层 UI element、table/group/header、row 和 cell 集合都必须先 `get` 并物化为冻结对象列表后才能遍历，
禁止让 AppleScript 将循环重新解析为活集合的
`item N of every ...`。动态树导致的 `-1719/-1728` 只能使本次探测安全失败或整轮重试，其他错误必须
原编号抛出；任一失败都不得产生提交、确认或撤单动作。
点击唯一“委托”tab 本身会重建 AX 树，因此点击前审出的 `targetWindow` 在 delay 后必须作废；读取筛选器
或表格前必须重新从 unfiltered windows 建立冻结快照，再次证明唯一模拟 marker、标准窗口仍存活且无
账户设置/转账/退出等实盘负证据。该重验证完成前不得输出任何部分订单证据。
最终通用 UI probe 的每次窗口快照也必须原子包含恰好三个可读订单字段；tab 重建期间暂时观测到 0、1
或 2 个字段时，必须在 AppleScript 内丢弃整轮证据并进入同一有界重试，不能先返回部分 receipt 再由
Rust 解析失败。只有完整一轮才能发布 bundle/version/marker/controls/fields，未知 AX 错误仍须原编号抛出。
订单探测的原子边界还必须覆盖 tab 后窗口重取、筛选器、table/group/header、rows/cells 直到完整序列化；
仅重试窗口列表而让后续冻结 descendant 在重建期间失效仍不够。该整段任何 `-1719/-1728` 都必须丢弃
attempt-local 输出并从 unfiltered windows 重来，不能发布半张委托表；其他错误继续原编号抛出。
连续部署在处理每根新行情前，必须先拒绝未知开放合同、完成 durable outbox 对账，并证明所有已绑定
合同已达到规范的“全部成交”或“全部撤单”终态；只有该严格审计产生的不可伪造 permit 才能进入
`process_bar`。这只是暂停后续市场处理，不能把远端未成交解释成算法 Defer。上午和下午最后一根可执行
五分钟 bar 分别在 11:25 和 14:55 结算，给新委托保留完整五分钟成交窗口；午休和收盘后不得产生新的
行情驱动 UI 点击。
当前同花顺模拟部署是“影子执行”：核心 Paper 仍以不利滑点形成保守事实，但远端合同必须完整成交
`Q`，且实际成交价不得劣于核心逐 fill 加权均价（BUY 不得更高，SELL 不得更低），否则不能取得
下一根 bar 的 permit。远端更优成交不会反写或美化核心收益。同花顺委托表无法证明实际费用，因此
该模式必须继续使用核心 Paper 的保守费用模型；10 万元只是一致展示的报告指标，不是持续交易硬门禁，
且绝不能扩展为实盘；未来实盘阶段必须让经纪端实际 fill/fee 直接
进入 Ledger，不能继续使用影子 Paper fill。
任何模拟提交都必须在最终点击的同一 AppleScript 内再次验证唯一模拟窗口、已审版本、
无实盘控件和三个字段回读，并且只能点击一次。输入框识别只能依据几何坐标，不能依赖 macOS
可访问性集合的枚举顺序或对惰性集合直接取 `item 3`。点击后必须以提交前合同号集合为基线，
仅接受唯一新增且方向、代码、价格、数量完全相同的合同。任何二次确认或撤单都必须重读并精确
绑定该合同号；不得按表格行号、当前选中行或“最新一笔”撤单。

既有运行的初始现金、总持仓、可卖持仓、symbol、anchor、配置版本、算法身份和估值/费用策略必须由账本中唯一且有序的 `RUN_STARTED → CONFIG_SNAPSHOTTED → ALGORITHM_REGISTERED` bootstrap 确定。CONFIG 必须保留可复核的规范内容哈希，算法 artifact/environment/platform 必须保留规范小写 SHA-256。当前配置文件只负责定位数据库，不能作为恢复种子覆盖旧运行事实。Web 快照、原生页面、CLI 只读查询、完整重放、snapshot 恢复和未完成 bar 的前缀重建都必须使用同一冻结运行上下文；即使没有 snapshot、snapshot 被删或损坏、或当前配置中的初始账户/symbol/anchor 已漂移，读结果仍必须保持账本事实。任何新 STEP/PLAY/FINISH 写入则必须在命令 claim 前拒绝运行身份漂移。bootstrap 任一事实缺失、乱序、不一致或被篡改时立即 fail closed，绝不能用合法 snapshot 或调用方当前配置“修复”。

承载算法的 Rust 平台二进制升级不能改写 `ALGORITHM_REGISTERED`。离线授权必须先验证目标二进制、
认证报告、冻结配置/算法合同，以及绑定同一 source/run 且 cursor 已到 journal head 的模拟 outbox；outbox
不得含未决状态，所有 staged intent 都必须已有 `SUBMITTED + FILLED` 远端终态。通过后只追加唯一
`PLATFORM_UPGRADE_AUTHORIZED`，其中 `from` 必须等于当前有效平台、`to` 必须是未使用的新 SHA。
目标平台只能在完整日志重建与 Paper 对账逐字一致后，紧邻授权追加唯一
`PLATFORM_UPGRADE_ACTIVATED`；授权与激活之间禁止任何业务事实。有效平台身份由这条 append-only 链
推导，Web durable command identity、CLI 和恢复均使用该 effective manifest，而不是初始 manifest 或
当前文件。无授权新平台、授权后的旧平台、非 platform manifest 漂移、重复 pending、分叉、降级、
认证证据或目标 SHA 篡改都必须在任何业务写入前拒绝。授权后进程退出不丢失许可；目标平台重启只可
幂等补同一激活，不得重复 opening deployment、Paper fill、outbox UI 动作或改变现金/持仓/业务状态哈希。

Web 的 liveness 与 readiness 必须分离：`/health` 只证明进程存活，不能代表数据库可读或可写。核心只能在数据库租约、一次性迁移、当前 schema、不可变数据库实例 UUID 与关键业务读取全部成功后对外 ready；启动后必须同时绑定文件和库内实例身份，运行中数据库丢失、替换、原位覆写或 schema 异常会永久撤销该进程的 readiness，所有业务读写返回不可用且不得创建或迁移替代账本。恢复只能显式重启。Launcher、BFF、浏览器和 CI 必须同时通过 `/ready` 与带内部令牌的业务读取，SIGINT/SIGTERM 都必须干净停止播放 worker 并释放租约。

网页控制也属于可靠性边界。每个 `start/step/play/pause/finish` 命令必须携带全局唯一且可稳定重试的 `request_id`、调用方看到的账本序号和控制版本。平台必须先持久化命令 claim，再执行业务，最后持久化完整回执；相同 ID 和相同请求在并发、响应丢失及进程重启后只执行一次并返回相同回执，相同 ID 改变任何字段必须原子拒绝。三年 FINISH 可能超过 BFF HTTP timeout；客户端超时后不得盲目再次 POST 原命令，只能用同一 request ID 查询/续作服务端持久回执。服务端的 per-run single-flight 生命周期不得依赖已断开的 handler future；相同 FINISH 的第二个入口必须加入同一在途执行，最终只产生一份完整 bar 事实和一个逐字相同的 COMPLETED 回执。PLAY 必须先完成持久回执才可启动 worker；遗留 PENDING PLAY 必须在快照或下一命令前确定性恢复。PLAY/PAUSE 使用持久化 generation，worker 每根 K 线都复核当前 generation，旧暂停绝不能中断新一代播放。同一数据库同时只允许一个 Web 核心进程持有运行租约。原生网页表单和 Python 网页层必须经过同一命令收件箱，不得保留直接写旁路。现代网页必须显式提供“运行至结束”控件，并通过同一 durable FINISH dispatcher；不能只在 Rust API 中保留一个用户无法触达的能力。

FINISH 的业务完成判据必须同时包含目标范围内每根行情的 `RECEIVED → DECISIONS_COMMITTED → PROCESSED` 三阶段事实，以及终态唯一的 `SERVICE_STOPPED`。最后一根已经 `PROCESSED` 但停止事件写入失败时，命令仍是可恢复的 `PENDING`；pending 列表、快照刷新或其他恢复入口都不得仅按 cursor 到达目标就提前生成 `COMPLETED` 回执。原 request ID 续作只能补齐唯一停止事实，再以不早于该事实序号的 `accepted_sequence` 完成原回执，不能重复任何 bar。

`SAFE` 和 `READ_ONLY` 是受保护但可审计的服务状态，不是“回放未完成”。FINISH 必须允许处于这些状态的当前账本把剩余行情完整记录到终点，并追加唯一停止事实和完成回执；它只能禁止新的不当订单，不能把安全模式误判为 Running 或强迫恢复交易。终态投影保留原安全状态及其原因。

所有数量、网格和结算算术都必须在写账本或 Paper 状态前证明有界。配置验证必须覆盖派生网格而不只检查输入符号：任何会让节点 Decimal 溢出、按价格精度舍入为零、或要求不可接受数量级节点枚举的 anchor/ratio/boundary 都以普通错误拒绝，绝不能 panic、卡死或先写 bootstrap。仓位上限使用差值/checked arithmetic；只剩 `Q-1` 股 headroom 时拒绝一份，恰剩 `Q` 时允许到 `i64::MAX`，不能通过加法回绕。成交造成现金、持仓、冻结量、累计成交、lot/tranche 或费用溢出时，Ledger 与 Paper 必须整笔原子拒绝且保持原投影、journal head、账户和 report 不变。

每个新 claim 还必须在同一事务保存规范原请求，以及绑定 accepted version、目标 cursor、数据集、配置和算法身份的计划摘要。业务尚未提交时，服务端不得因页面刷新而猜测执行；页面必须能够发现即使尚无任何 run event 的 PENDING 命令，并只用原 `run_id + request_id` 一键续作同一持久请求。此时当前数据、配置或算法身份只要变化就必须标为不可重试并返回 `PENDING_PLAN_CONFLICT`，账本保持零业务事件。已提交但回执未完成的命令必须以账本内冻结的数据 descriptor、配置和算法事实补原回执，即使重启时当前数据文件或配置已经变化也不得阻断、重新执行业务或改写 descriptor。迁移前缺少原请求或身份、请求/计划任一组成字段被篡改的 PENDING 记录必须可见但标为不可重试，任何续作都要 fail closed。

每个网页或 CLI 新回放在首根行情前必须冻结数据集 ID、完整 SHA-256、证券代码、总根数和首末时间。显式未知数据集必须在命令 claim 前拒绝，不得回退为默认数据。服务进程只使用启动时由同一份字节完成哈希与解析的不可变行情；重启时文件身份不符必须在任何新写入前拒绝。页面行情只能来自 Rust 核心按该运行身份返回的已处理前缀，绝不由 Python 或当前默认文件猜测；缺少数据身份的旧运行只显示账本指标和明确的“数据源未绑定”，不得冒充当前行情。行情使用成熟图表库绘制 OHLC/K 线并支持缩放，时间轴必须允许聚合桶内部的原始机会时刻拥有有限坐标，所有网格决策标记始终位于行情层上方且数量与账本事实一致。

性能与发布认证必须先证明“测的是谁”，再评价快慢。short/nightly 的 source config、processed data、run ID、采样次数、算法身份与确定性业务结果必须由仓库内版本化 manifest 固定，expected identity 不得由工作流或命令行覆盖；三年数据还必须绑定 raw 与 quality provenance。认证程序在启动任何子进程和创建数据库前校验输入字节，随后只使用同一次读取产生的 binary/config/data/manifest 冻结副本，并在每个阶段前后重验。回放结束后还必须从唯一的 `CONFIG_SNAPSHOTTED`、`ALGORITHM_REGISTERED`、`REPLAY_INITIALIZED` 反向验证 canonical config、完整算法、平台 binary 与数据身份。完整事件类型分布、逐 BUY/SELL 的 C/P/A/E/D/B/I/R 与 T+1/风险/保本前置阻断、intent/fill 数量、逐 SELL allocation 不亏、逐 tranche 非负守恒、Paper 与策略账的订单/fill/现金/持仓对账，以及终态费用和收益共同形成始终阻断的业务结果指纹。订单意图可以被拒绝、撤销或部分成交，因此成交量必须独立记录和对账，不能被错误规定为恒等于意图量；相同总事件数或 rebuild matched 也不能替代上述证明。只有资源阈值可在初始 nightly 观察期记为 WARN；输入、工作负载、账本、业务结果、正确性及 Linux 必测指标缺失在 short、nightly 和 release-candidate 中均必须 fail closed。

页面收益必须把“盯市”和“逐 lot 保守退出”明确分开，不能用一个含糊的“未实现收益”替代。盯市未实现收益按最新已完整处理的价格减每个策略 lot 的未摊销买入成本计算，不扣尚未发生的退出成本；逐 lot 保守退出估值使用账本冻结的保本策略，对每个剩余 lot 独立施加不利滑点、最低卖出佣金和印花税，允许如实显示负数，且不因 T+1、冻结或当前不可卖而删去该 lot。该估值不代表当前可成交或可卖。两种总收益都必须分别等于已实现收益加各自未实现收益；逐 lot 保守退出调整等于保守退出未实现减盯市未实现。没有最新已处理价格、存在未知成本 lot，或缺少账本冻结估值策略时，相应值必须明确显示不可用，绝不能默认为零。若已实现与未实现分量各自可表示、但二者之和超出 Decimal 数值域，总收益字段同样必须明确为不可用并保留两个可审计分量，不得 panic、回绕、截断或伪造总值。所有字段只能来自已处理账本前缀，并在热缓存、重启冷恢复和完整日志重建后保持一致。

## 6. 开发验收原则

## 5.1 独立行情数据面

行情采集、发布与长期存储必须和交易执行权限分离。群晖行情节点只允许承担经过 TLS 与账号鉴权的
MQTT 5 接入、原始事件持久化、精确重投去重和身份冲突留痕；不得获得同花顺 UI、交易账本、
订单 outbox 或任何下单/撤单能力。Mac 旁路发布器只能读取已经落盘并通过现有身份检查的行情
JSONL，自身使用 durable source sequence 与发送 outbox；网络失败或 ACK 丢失只能导致原始字节
重投，不能改变交易工作流。

行情公共模型必须按 `venue + symbol + source_id + source_instance_id + source_sequence` 保存来源事实，
同一证券、同一时刻来自网页逐笔、同花顺页面和未来商业数据源的记录可以并存，绝不能按证券代码
互相覆盖。全局 `event_id` 和单来源 `(source_id, source_instance_id, source_sequence)` 均是唯一边界：
原字节重投记为 duplicate，不同内容复用身份记为 conflict，非法 schema/topic/content-type 记为
rejection。事件时间、接收时间和来源序号在 wire 上都是 u64；时间单位固定为 Unix 微秒，数据库须
完整保存 u64 数值域。

`TRADE_TICK` 表示网页逐笔成交表中的真实单条成交；三秒采样只是一种采集节奏，不能把三秒内的
最新价误写成逐笔成交。只读最新价使用 `QUOTE_SNAPSHOT`；盘口使用 `BOOK_SNAPSHOT/BOOK_DELTA`；
K 线使用 `BAR`。公共行情事件只保存来源、证券、时间、载荷与证据哈希，`account_marker` 等交易
页面安全控件不是行情事实，只允许作为 Mac 本地准入检查，禁止传播到公共行情模型。

行情消息使用 MQTT 5 QoS 1、非 retained 数据事件及显式 `application/json` content type。首版 codec
是 canonical JSON，但数据库必须同时保留原始字节与 payload format，以便未来增加 Protobuf 而不
破坏来源身份和关系索引。数据库不得直接暴露到局域网；匿名 MQTT、明文 1883 和越权 topic 发布
均必须拒绝。容器、数据库与发布器重启后，唯一事件数、重复投递数、冲突/拒绝记录和 source cursor
必须保持一致。

所有产品文案、状态模型、接口、图形和测试都必须符合本文件。若实现与本文件冲突，以本文件为准，并先补充机器可执行案例，再修改代码。

测试完整性是产品能力，不是开发收尾工作。网格路径必须由版本化、机器可读的 case catalog 描述，并由同一套 Rust runner 自动执行。不能只覆盖顺畅的单边行情；至少必须系统覆盖：

- 连续逐格下跌、连续逐格上涨；
- 下跌一格或多格后逐格反弹；
- 下跌途中反弹一格、再次下探并形成新 excursion；
- 多层递延后在深层全部行权、部分行权或继续递延；
- 已行权网格反弹后再次下探，禁止同周期重复铸造；
- 完全未行权的边际份额被反转撤销后，允许新 epoch token，但绝不复活旧 token；
- 买卖两侧镜像、T+1、底仓、保本和下单前风控阻断；
- 同一 OHLC 内无法确定先后顺序的深探/反弹路径，必须保守判定歧义；
- 相邻行情之间发生向上或向下跳空时，上一收盘到当前价格路径越过的每一层都必须逐层记录机械机会；
- 每一根 bar、每个持久化故障点的重启恢复，与不中断运行得到相同业务结果；
- 旧 schema 只读重放、新 schema 降级写入拒绝、重复输入幂等和多写者栅栏。

每个网格 case 必须明确行情路径、机械触点、算法精确 `exercise_quantity/defer_quantity`、下单前平台阻断、是否形成订单意图、决策时的 right/tranche 处置以及数量守恒；不得用成交后的 right 状态、现金、持仓或 PnL 作为网格算法 oracle。修复任何网格语义 bug 时，必须先或同时增加能稳定复现该 bug 的自动 case；只增加普通单元测试而没有业务路径案例，不视为完成。
