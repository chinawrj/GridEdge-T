# Web snapshot 热缓存合同

`GET /api/v1/runs/{run_id}` 的热缓存只优化重复的账本投影和 JSON 序列化，不能改变
append-only journal、durable command inbox、回放控制或无未来数据语义。无缓存的账本
重建始终是权威基线；缓存内容必须可以随时丢弃并由同一账本重新得到逐字段相同的结果。

## 一致性边界

一次 snapshot 必须在同一个 SQLite read transaction 中观察以下版本向量：

- run 的 journal head sequence；
- duplicate event 计数；
- 已完成 bar 数；
- replay descriptor 的 dataset id、完整 SHA-256 和时间范围身份；
- `web_playback_control` 的 command version、active 和 interval；
- 是否存在 PENDING receipt。

同一 run、同一版本向量才允许命中。STEP/PLAY/PAUSE、worker 推进、receipt/control
变化、dataset identity 变化或 duplicate 计数变化必须得到新版本。不同 run 永不共享缓存项；
相同文件名但 SHA 不同的数据集也不能共享。

PENDING receipt 必须先按 durable inbox 规则恢复。仍有 PENDING 时不得命中或写入缓存；
失败响应、反序列化错误、账本校验错误、dataset 校验错误以及不完整投影都不得写入缓存。
snapshot 只能公开完整 committed prefix，不能把 bar 内部中间态或未来 OHLC 带入 DTO。
对已绑定 `ReplayDescriptor` 的回放，若存在没有同 symbol、同 event time
`MARKET_BAR_PROCESSED` 配对的 `MARKET_DATA_RECEIVED`，说明该 bar 尚未完成：此时必须
绕过热缓存，并从 journal 0 精确
重建至这条 market fact 之前；若同时存在 PENDING receipt，则进一步截到
`min(receipt.expected_sequence, market_sequence - 1)`，从而保持整个命令的公开原子边界。
该规则不依赖 snapshots 派生表是否已有 checkpoint。API sequence、state、performance、
progress 和 last price 都不得前移。故障移除后只能由同一个 durable STEP receipt 补完该
bar，完成后新版本才可缓存。

同一 run 的 miss 使用 single-flight：一个调用执行投影，其余调用等待并复用完全相同的
序列化字节。不同 run 使用不同锁，避免一个长 run 阻塞其他 run。服务重启后内存缓存为空，
第一次读取必须冷算；后续同版本才可复用。

缓存是有界的派生数据，不是第二账本：最多 16 个 run、总计 16 MiB、单项 2 MiB，按 LRU
淘汰。超过单项预算的合法 snapshot 直接返回但不缓存。淘汰、进程退出或缓存丢失不能影响
任何业务结果。

## 自动化矩阵

黑盒测试位于 `tests/web_command_inbox.rs`：

| 合同 | 自动测试 |
|---|---|
| 同 head 连续五次及四并发响应字节完全一致 | `snapshot_same_head_is_byte_identical_for_sequential_and_concurrent_readers` |
| STEP、PLAY、PAUSE 和独立 control 更新立即可见 | `snapshot_refreshes_after_step_play_pause_and_direct_control_changes` |
| 两个 run、两个不同 SHA 的 dataset 不串缓存 | `snapshot_cache_contract_isolated_by_run_and_frozen_dataset_identity` |
| PENDING PLAY 完成失败不前进、不缓存错误；移除故障后同入口恢复 | `snapshot_pending_play_failure_is_not_cached_and_never_exposes_partial_progress` |
| STEP 已写 market 但 bar completion 失败时仅返回前一 checkpoint；同 receipt 恢复后才前进 | `snapshot_hides_partial_market_bar_until_the_same_step_receipt_completes` |
| 重启冷读与独立 `SqliteStore::rebuild`、progress/control 原始事实逐字段相同 | `snapshot_after_restart_matches_an_independent_ledger_recovery_field_for_field` |
| 未处理 bar 的特殊未来价格不出现在 snapshot，处理后才出现 | `snapshot_cache_never_exposes_future_ohlc_before_its_bar_is_processed` |

“实际投影次数”不能通过稳定的 HTTP 黑盒信号推断：SQLite trigger 不统计 SELECT，耗时、
文件时间和直接篡改 journal 都不是可靠证据。精确计数必须由 `web.rs` 私有缓存组件的同模块
测试使用计数 closure 完成，不增加 debug endpoint、public trait 或生产逃生参数。该测试必须
锁定：同版本连续/四并发只投影一次；错误后下一次重新投影；restart 后第一次重新投影；
失效后的新版本也只投影一次。

## 性能合同

三年 34,944 bars / 110,532 events 数据仍是 nightly/release 权威样本。缓存上线后的测量
应把 cold 和 warm 分开，不能用预热结果冒充恢复性能：

| 指标 | 初始预算 | 门禁性质 |
|---|---:|---|
| restart 后首次 cold snapshot | ≤ 0.75 s | 沿用现有 snapshot 预算 |
| 同版本 warm sequential snapshot p95 | ≤ 0.10 s | 前 3–5 次 warn-only |
| 同版本 warm concurrency-4 snapshot p95 | ≤ 0.25 s | 前 3–5 次 warn-only |
| 同版本连续请求实际投影数 | 1 | 始终 blocking 的单元合同 |
| 同版本四并发实际投影数 | 1 | 始终 blocking 的单元合同 |
| cached 与 uncached JSON | 逐字段及序列化字节相同 | 始终 blocking |

性能 artifact 后续应分别记录 `snapshot_cold_seconds`、
`snapshot_hot_sequential_p95_seconds`、`snapshot_hot_concurrency_4_p95_seconds`、响应字节数
和 cache 是否可用；不能只保留混合 cold/warm 的单一 p95。生产 API 不暴露内部投影计数，
精确次数只属于自动化单元合同。
