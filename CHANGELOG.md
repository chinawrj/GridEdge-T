# Changelog

## v0.1.0 — 2026-08-17

GridEdge-T 的首个可发布研究版本，提供：

- 固定股票数量“份”的 BUY/SELL 权利、整份决策与 lot/tranche 守恒；
- 追加式 SQLite 账本、确定性重放、快照恢复与 Paper 执行；
- 逐 lot 不亏卖出证明、订单级累计佣金、T+1 与平台风控分区；
- 可分页的完整机械机会历史，以及单步、自动播放、暂停和运行至结束；
- durable Web 命令回执、single-flight FINISH、数据库身份与 readiness 边界；
- 明确区分盯市收益和逐 lot 保守退出估值；
- 有界网格、数量与 Decimal 算术，所有越界在账本或 Paper 副作用前失败；
- short 与三年固定输入的发布认证、业务结果指纹和真实浏览器门禁。

本版本仅用于研究、回测和 Paper 交易，不包含实盘经纪商连接。
