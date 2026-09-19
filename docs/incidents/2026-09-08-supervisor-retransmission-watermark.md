# 2026-09-08 supervisor 重投尾部水位误报

状态：10:33已由接管owner正式发布、10:34独立现场验收通过，监控误报已修复。完整交易恢复事故仍OPEN；见2026-09-08-recovery-owner.md。以下候选阶段记录为历史。

## 事实与时间线（Asia/Shanghai）

- 09:10 正式 supervisor 报告 seq36653、昨日观察、READ_ONLY。该报告不能证明 raw 真正的最高序号。
- 09:15 正式运维核对：raw 9394 行，唯一序号27521..36657连续，gap0、冲突0、完全重复257；物理尾部重投36650..36653。PG唯一1..36657连续，36654..36657 exact committed 存在。全局既有 conflicts/rejections 为1/5，本轮未清除或改写。
- 根因：`market_health` 读取最后1 MiB并以 `decoded[-1]` 作为最新事件，反向物理行寻找最新观察。完全合法的旧记录重投导致监控水位和观察时间倒退。只取尾部最大值仍不能覆盖超过1 MiB的重复尾部。
- 09:19 开始独立复现夹具；旧实现7个subcase失败，含普通/大尾部重投、冲突、身份复用/漂移、gap和未见倒序。
- 09:19–09:22 候选最小修复后23项supervisor测试通过。
- 同一09:19–09:22区间，候选只读正式raw及PG：seq36657，event `ece4a3a223ad835661042156200253b9a36e3d07c576df2952f39e9512680500`，payload SHA `bd759f161b7a3fc5dade2a9c2767cc2227775dc56d40bd62e579d6045a0afcac`，exact committed=true，source_fresh=false；全扫描加远程查询0.984秒。纠正水位不等于获得今天的行情许可。
- 截至09:22:57实际时钟复核，`python3 -m unittest discover -s deploy/ops -p 'test_*.py'`：38项通过，有一个测试夹具的未关闭lock文件ResourceWarning；未以此宣称生产运行恢复。证据与审查请求已交同一运维任务，未建立第二运行owner。草稿中09:23/09:24为错误估时，已以实际时钟上界纠正，非准确动作时间。

## 候选边界

读取打开时固定文件长度内的完整换行前缀；并发追加留给下一轮。保存每个唯一sequence的topic/payload哈希与event_id索引，完全一致重投跳过；新事件必须连续推进，冲突/身份漂移/未见倒序/缺号拒绝。最新观察按有效新序列推进，不按文件物理尾部或最大墙钟时间猜测。

不改变行情完整性规则、市场准入、Paper/outbox、账本、策略、交易门禁或Android账户。未整理/截断/覆写正式raw。不是新行情adapter，不能由本修复宣称OHLC可执行或策略已经评估。

候选 `deploy/ops/session_supervisor.py` SHA:
`c5e8fea5c5d904fde70dd6983f42a3afff1ceb124f50b612adde1fb3f1f73c42`

测试 `deploy/ops/test_supervisor_independent.py` SHA:
`ccf915c05ca3cbab37c4a169fef97bbebfadd0d6a86b5590c4d61be64f6fce4f`

正式安装仍是 `8df496ae16a57d43ef42f609ac2a2cfd747ac65314c5cdae9772ea96ac6addcd`。
待审项包括完整前缀扫描的增长成本（当前31 MiB实测小于1秒，含SSH）、独立拒绝用例及既有发布路径。
未运行完整supervisor候选（它有恢复动作），只调用只读`market_health`。Rust无新增变更，复用09-07 23:22对应身份的format/Clippy/496测试/build/隔离replay证据，不把Python修复当Rust重认证。

## 恢复责任

唯一正式运维任务 `01a00079-b837-7750-9e60-1dbe3f803685` 继续09:25和09:35现场验收。
正式监控仍可能报告物理尾行旧值；以只读raw/PG精确绑定结果补充诊断，不能手工伪造正式健康回执。

## 09:28 独立审查与新的现场阻塞

已直接读取独立审查任务 `01a07e9b-4504-76b3-a13a-c16a81ced631` 的完成回执，结论PASS，仅允许候选进入正式发布流程。审查者复核两个SHA、相关35项/全部38项测试、Python编译、自建fstat后并发追加夹具；大重投尾部实际1,980,000 bytes。独立正式只读探针得到36657/exact committed=true/fresh=false，耗时0.962秒。未改文件或操作正式运行环境。发布与恢复验收仍未完成。

另外，正式运维09:26:14确认首观察45秒门限已错过，立即刷新并保留受审页面；09:27实际画面出现滑块验证码，虽显示09:25逐笔，PG仍停36657/昨日。运维已通知用户亲自完成当前Chrome滑块，未代做或绕过。该人机边界是当前正式行情阻塞，不是本监控修复可消除的故障，也不能称为无交易机会。worker继续READ_ONLY，开盘验收仍由同一运维活任务承担。

## 09:35:33 截止验收回执

唯一正式运维回报P0 deadline breach：PG仍36657/09-07 13:56:48，guard/worker/Android各1，ledger/outbox1958/1958，READ_ONLY，无今日current bar，策略未评估。09:27确认的验证码截至09:35仍未解除；没有下单、绕过或重复点击。开盘可用性验收失败，未恢复。监控候选仍仅审查PASS、未发布；本监控缺陷和当前验证码导致的行情不可用分别保持未关闭。完整时间线已追加交付status。
