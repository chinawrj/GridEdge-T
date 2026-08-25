# GridEdge Web Market Collector

这是一个可独立运行的 Chrome MV3 行情采集扩展：

```text
东方财富逐笔页
  -> provider adapter
  -> Chrome IndexedDB（原始证据、去重、source sequence、MQTT outbox）
  -> MQTT 5 / WebSocket / QoS 1
  -> 群晖 Mosquitto
  -> PostgreSQL ingestor
```

运行时不需要本机 companion，也不连接交易账本或订单 outbox。MQTT
publisher 用户名和密码保存在本机 Chrome 扩展存储中；数据库密码不会进入扩展。

## 运行语义

- 当前 provider 是东方财富 `f1.html?newcode=...` 的网页逐笔成交。
- content collector 会在采集前自动选择页面受审的“倒序”控件，先有界遍历当日全部分页，
  回到第一页复核首尾重叠与页面哈希后一次性提交；随后保持第一页实时采集。
- provider 按成交时间升序合并完整分页并分配连续 `source_sequence`；完整历史最后发布
  `SESSION_HISTORY_COMPLETE`，实时新增逐笔必须与上次水位有重叠，最后发布
  `LIVE_CONTIGUOUS`。交易侧只有收到覆盖桶末端的水位后才形成五分钟 K 线。
- 网页显示的每一行形成一个 `TRADE_TICK`，`手数`精确换算成股数。
- 相同外观的网页行由 occurrence ordinal 保持为不同源事实。
- 原始行和 canonical market event 先在同一个 IndexedDB 提交，再进入 MQTT outbox。
- MQTT QoS 1 PUBACK 只证明 broker 已收到传输，记录仍保持 `PENDING`。
- 只有 ingestor 在 PostgreSQL 事务提交后返回与 `event_id`、source identity 和 sequence 完全一致的
  `COMMITTED` 应用回执，才把 outbox 标记为 `ACKNOWLEDGED`。
- PUBACK 或应用回执丢失、网络断开或 service worker 重启时，记录保持 `PENDING` 并可重复发布；
  PostgreSQL 以 `event_id` 和 source sequence 幂等接收。
- 同一 source-row identity 的完全相同市场事实是 duplicate；成交时间、价格、数量等
  内容变化是 durable conflict，不覆盖已接收事实。表格位置以及价格箭头/空白属于可变
  展示证据，不参与事实冲突判定；完整原始 DOM 行仍保留在 capture batch 摘要中。
- Chrome、行情标签页或电脑停止工作时，实时采集也明确停止。恢复时先重发已经采集的
  `PENDING` 记录；若第一页无法与旧水位重叠，则重新遍历当日全部分页，绝不把断层
  声称为连续实时行情。
- 当前受审 provider 身份为 `eastmoney-time-sales-dom-v6`。只有完整分页证明、连续
  source sequence、完成水位和交易侧精确 OHLCV 重建全部通过，事件才可进入策略恢复链。
- `account_marker` 等交易页面身份不是行情字段，后台会直接拒绝。

## 开发与加载

依赖和 MQTT.js 浏览器 bundle 固定在仓库中：

```sh
cd extensions/gridedge-web-market
npm install --ignore-scripts
./scripts/test.sh
./scripts/build.sh
```

打开 `chrome://extensions`，启用开发者模式，选择“加载已解压的扩展程序”，加载：

```text
build/gridedge-web-market-extension
```

开发阶段统一加载构建目录。修改后重新执行 `./scripts/build.sh`，在扩展管理页点击该开发版的
“重新加载”，再刷新东方财富标签页；不要删除 IndexedDB，因为它保存 source sequence、完成水位
和未确认 outbox。

## MQTT 设置

群晖 listener 固定为：

```text
ws://192.168.1.201:9001/mqtt
username: gridedge-publisher
MQTT version: 5
QoS: 1
```

密码使用现有
`$HOME/Library/Application Support/GridEdge-T/market-mqtt/publisher.password`，仅保存在
Chrome 本机扩展存储。Mosquitto ACL 只允许它读写 `gridedge/market/v1/#`；读权限仅供同一
内网凭据的纸面交易行情消费者使用，不授予订单、账本或 UI 控制能力。

## 测试边界

普通测试覆盖 provider、多表页面、自动最新优先、批内时间正序、滚动展示变化、背景二次验证、
IndexedDB 重启、duplicate/conflict、source sequence、canonical event、broker PUBACK 后仍 pending、
数据库 `COMMITTED` 回执、伪造/超时回执和 MV3 权限/CSP。远端 E2E 还必须证明同一个 fixture 从
WebSocket MQTT 到达群晖 PostgreSQL，并收到绑定精确 `event_id` 和 source sequence 的应用回执。

```sh
node scripts/certify_synology_websocket.js
```

正式接入仍须通过浏览器到 PostgreSQL 的真实 E2E、五分钟 K 线重建、账户对账、全仓测试、
签名产物和同 run 平台升级链；任何一步未通过都保持只读恢复，不允许静默跳过行情缺口。
