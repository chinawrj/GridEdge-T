# GridEdge-T v6 三年认证报告

- 认证运行：`certification-3y-v6-0815`
- 标的：`002256.SZ`（兆新股份）
- 数据周期：2023-08-14 09:35:00 至 2026-08-14 15:00:00
- 5 分钟 K 线：34,944 根
- 发布二进制 SHA-256：`256b34ab4f11149fd6caea381fd6e9427ece7c8288682217fb30c376934ac71b`

## 账本完整性

- SQLite integrity：ok
- 事件：111,615 条
- 序号：1..111615，连续且无重复
- MARKET_DATA_RECEIVED：34,944
- MARKET_BAR_DECISIONS_COMMITTED：34,944
- MARKET_BAR_PROCESSED：34,944
- duplicate_events：0
- 快照重建与完整日志重建：matched=true，均到 111615

## 交易与数学不变量

- 订单：87；未完成：0
- 新增交易份额（lot）：88；未关闭：0
- 亏损卖出份额：0
- 逐 lot 已实现收益合计：77,837.56
- 策略状态已实现收益：77,837.56
- 权利 tranche：163
- `minted = available + reserved + consumed + revoked + expired` 违规：0

## 独立执行账户对账

- broker source：paper-broker-sqlite-v1
- matched：true
- differences：[]
- broker snapshot SHA-256：`30ad4412af7c32182c8931037a6fb18eaf2f45698fef2bd441ec20cb77557657`
- 策略与独立账户一致：可用现金 377,837.56、冻结现金 0、总费用 1,648.44、持仓 20,000、可卖 20,000、冻结卖出 0、未完成订单 0

此报告描述研究、回测与 Paper 阶段的认证结果，不代表实盘收益承诺。
