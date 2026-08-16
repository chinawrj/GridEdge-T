# 性能门禁与基线

性能门禁分为两个互不混淆的层次：PR 使用 `configs/default.yaml` 对应的 21 根非空
sample bar 做快速硬门禁；nightly、正式
release 和手动调度使用固定的兆新股份三年 5 分钟数据做长周期观测。长周期工作流不会
用 synthetic 数据替代历史数据，也不会修改业务实现来迎合测量。

两种模式的输入身份由仓库内版本化 `configs/performance-inputs-v1.json` 唯一冻结。manifest
至少记录 source config 与 CSV 的原始字节 SHA-256、根数、固定 run ID、固定采样次数和预期
算法 name/version/artifact/environment；nightly 还绑定 raw CSV 与 quality report 的来源路径和
SHA-256。expected hash 或工作负载身份不接受
CLI、环境变量或 workflow_dispatch 覆盖。相同根数但 volume/amount 不同的行情、或任一行为
配置字段变化，都必须在生成运行配置、创建 SQLite、启动首个子进程之前阻断。复制文件或符号
链接只要字节完全相同仍可使用，因为合同绑定内容而非调用方路径。
进程入口只读取 manifest 一次：run/workload、输入、durable ledger、业务结果期望和 artifact
中的 `manifest_sha256` 必须全部来自这一次读到的同一字节快照。即使原 manifest 路径随后被
连续替换，冻结副本仍保持入口字节，禁止在一次测量中混用新旧身份。

每次运行先写 `input-identity.json`，逐项记录 expected/actual/pass，包括工作负载、算法与
nightly provenance；
`summary.md` 必须显示同样证据，成功运行的 `metrics.json` 还要嵌入相同身份对象。输入身份失败
即使在 nightly warn-only 阶段也始终是 blocking `FAIL`，但仍留下可上传的 identity/summary，
且不得留下生成配置或数据库。release binary 本轮不设仓库 golden，但必须记录启动时实际
SHA-256，并冻结这份字节供 validate、replay、status、rebuild 与 Web 全程共用；调用方在运行
中替换原 binary 路径不能改变被测程序。
validate 完成后到任一后续阶段开始前，冻结 binary 或生效配置的任一字节变化都会以
`stage_identity` 硬失败，且该后续子进程不会启动。摘要必须区分阶段冻结输入篡改、业务结果
指纹漂移与 durable ledger 身份不一致：三者都不能显示 PASS，也不能用错误类别掩盖真实故障。

nightly 的 Web RSS 是 Linux runner 上的必需观测，不是可选性能值。若 `/proc` 采样在 Linux
返回空值，必须记为 blocking instrumentation failure，即使阈值仍处 warn-only 期也不能降级为
`N/A/PASS`；非 Linux 本地运行才可明确记录 unsupported 而不阻断。

## 工作流

- `.github/workflows/ci.yml`：每个 PR/main push 使用优化构建运行 `short` 模式。replay
  wall time 必须不超过 1 秒，峰值 RSS 不超过 64 MiB，SQLite 文件不超过 2 MiB；超限
  立即阻断。
- `.github/workflows/performance.yml`：每天 UTC 18:30（北京时间次日 02:30）、GitHub
  Release 发布时及 `workflow_dispatch` 手动触发。固定输入为
  `configs/zhaoxin_5m_quantity_v10.yaml` 和
  `data/processed/002256.SZ_5m_raw_20230814_20260814.csv`，使用 release binary。
- `v*` release-candidate tag 与 GitHub Release 事件始终启用性能阈值硬门禁；定时 nightly
  在基线观察期仅把性能资源超限记为 WARN，不能启用或覆盖 release/tag 的 `--enforce` 规则。
  输入身份、账本身份、业务结果指纹、正确性及 Linux
  必测指标缺失在所有模式下始终阻断，不能被 warn-only 或手动参数降级。
- 三年门禁上线后的前 3–5 次有效运行是 warn-only：所有超限写入 summary 和 artifact，
  但不使工作流失败。基线稳定且量化/架构席复核后，手动调度时勾选
  `enforce_thresholds` 验证阻断模式，再将其提升为发布硬门禁。该开关不改变测量方法。

长周期 job 的 timeout 为 50 分钟；replay 本身的发布预算为 180 秒，并设 240 秒硬截止，
避免索引退化时再次空耗二十多分钟。正常完成后仍继续数据库、恢复和 API 诊断并上传证据。
工作流不自动取消已开始的长测，避免新提交销毁一次昂贵测量。

## 已测基线和初始预算

固定数据集基线是 34,944 根 bar、110,532 个 ledger event。以下是量化席的实测结果和
前 3–5 次 warn-only 使用的上限：

| 指标 | 实测基线 | 初始上限 |
|---|---:|---:|
| replay wall time | 30.345818 s | 180 s |
| replay peak RSS | 25.140625 MiB | 64 MiB |
| SQLite size | 87.609375 MiB | 100 MiB |
| cold `status` p95 | 0.509206 s | 0.75 s |
| full rebuild wall time | 4.017797 s | 8 s |
| full rebuild peak RSS | 159.203125 MiB | 256 MiB |
| sequential snapshot p95 | 0.533831 s | 0.75 s |
| bars(1000) p95 | 4.206 ms | 25 ms |
| events(1000) p95 | 4.441 ms | 25 ms |
| concurrency-4 snapshot p95 | 0.232526 s | 1 s |
| replay throughput | 1151.526047 bars/s | 不低于 190 bars/s |
| Web peak RSS | 待 Linux artifact 积累 | 96 MiB |
| sequential snapshot body | 138,420 B | 256 KiB |
| bars(1000) body | 164,652 B | 256 KiB |
| events(1000) body | 630,117 B | 1 MiB |

三项响应体积从第一阶段即按上表纳入 warn-only 性能检查。Web RSS 通过 Linux `/proc`
采集；Linux nightly 采不到 RSS 属于 instrumentation failure 并始终阻断。本地非 Linux
平台不提供 `/proc` 时明确记录 `N/A` 且不阻断，不以进程外估算值冒充实测。
Cold status RSS 尚未进入首轮合同，待建立独立进程稳定采样后再增加 32 MiB 预算；不得用
当前 replay/rebuild 的累计 child RSS 代替。

### 三年账 Web 超时可观测性

2026-08-17 的第二次三年账已完整写入 110,532 个事件，终态事件和快照 sequence 都是
110,532，快照 JSON 约 137 KiB。相同 Web 进程中，bars(1000) 与 events(1000) 均约 5 ms，
但首个 `GET /api/v1/runs/perf-three-year` snapshot 超过 30 秒且进程持续占满单核。因此该故障
定位为大账 snapshot 冷投影/cache 路径，而不是行情分页、事件分页或终态快照缺失。

性能 harness 的超时必须保留原始 `TimeoutError` 类型并附带完整 endpoint；Web context 清理
即使随后 SIGINT 超时并被迫 kill，也只能作为附加诊断，不能用 cleanup `RuntimeError` 覆盖
原始 snapshot 超时。这样 artifact 才能直接说明哪个端点、哪个阶段首次失败。

M11 为精确的 unfinished-bar `NOT EXISTS` 内层查询增加
`(run_id,event_type,symbol,event_time,sequence_number)` 复合索引；自动化必须用
`EXPLAIN QUERY PLAN` 证明内层按 run/type/symbol/time 全键命中该 covering index，同时继续
验证完整 bar 公开、半 bar 停在前一公开前缀，不能用数量快捷判断替代精确身份匹配。
M12 是只向前执行的 schema 迁移，只删除已被唯一约束或 M10 覆盖的三个冗余索引：`idx_events_run_sequence`、
`idx_events_market_run`、`idx_events_processed_bar_run`。迁移测试必须从物理 M8/M10/M11 分别
升级到当前 schema 13（先执行 M12，再创建 M13 database identity），证明 M10/M11、correlation index 和 `UNIQUE(run_id,sequence_number)` 自动索引仍在，
`load_after` 改用该唯一自动索引、unfinished-bar 内外层仍分别命中 M10/M11，且重复启动不恢复
已删除索引、不改变事件、快照或 durable receipt。`DROP INDEX` 释放页进入 freelist，但不承诺
在未执行安全 `VACUUM` 时立即缩小 SQLite 文件字节数。性能门禁和启动迁移都不得自动执行
`VACUUM`；回滚或压缩数据库属于独立、显式、可备份验证的运维流程。

在上述 110,532-event 数据库副本上的修复后实测：首次执行 M10→M11 建索引并请求 snapshot
合计 0.973 秒；迁移完成后重启，冷 snapshot 为 0.612 秒，随后三次热请求为
0.057/0.056/0.057 秒。四个响应均为 138,420 字节且 SHA-256 完全一致。由此迁移后冷请求
重新满足 0.75 秒预算，热 cache 也未改变业务 JSON；一次性索引创建耗时应独立记录，不冒充
稳定态 snapshot 延迟。

正确性与性能告警严格分离。PR 必须精确得到 21 bars/233 events；nightly 必须精确得到
34,944 bars/110,532 events；两者都必须 `rebuild matched=true` 且 full/snapshot sequence
等于事件数。这些正确性条件即使处于 nightly warn-only 阶段也始终阻断。量化阶段曾出现的
285 events 来自不同入口，不属于当前冻结的 `configs/default.yaml` PR 合同。
三年 `110,532` 是 `run_id=perf-three-year` 下当前 Paper 固定抽样轨迹：38 个 intent 中
16 个产生部分成交（BUY 9、SELL 7）。run ID 会进入 intent UUID 和确定性 Paper draw，旧的
110,571 不是当前固定工作负载，不能继续作为 golden。总事件数相同仍不足以认证，因此还需
同时匹配完整 business-result v2 SHA `1b624dd87114ffe3270c66e91ac31a62d6e94ff00f692bd1a34616001a3c082b`；short v2 SHA 为
`d7e3644ba66965104998770b9ebf795f4cec5838f2f101adf9076423c7da9e1d`。当前三年终态为 cash `323502.44`、position/sellable
`20000/20000`、fees `497.56`、realized `23502.44`，且 Q 数量满足
`C=A=E=I=228000`、`P=D=B=R=0`，预决策 T+1 阻断量为 `12000`。

## 可复现测量

`scripts/perf_gate.py` 只使用 Python 标准库，并调用真实 CLI、SQLite 和 HTTP API：

1. 从仓库内置的 `configs/performance-inputs-v1.json` 读取唯一的 short/nightly 合同；它固定
   source config、processed data、run ID、采样次数、算法身份和业务结果 golden，命令行不能
   覆盖 expected hash。nightly 还固定 BaoStock raw 与 quality report provenance；
2. 在启动任何子进程和创建数据库前校验字节 SHA。失败仍写 `input-identity.json` 与摘要，
   但不生成有效配置或数据库；通过后把 binary/config/data/manifest 写入隔离冻结副本，后续
   每个阶段前后重验，所有命令只使用这些副本；
3. 一次性 replay 后记录 wall/RSS、bar/event 数和 SQLite 页面/字节数，并从账本中唯一读取
   `CONFIG_SNAPSHOTTED`、`ALGORITHM_REGISTERED`、`REPLAY_INITIALIZED`，反向验证 canonical
   config SHA、完整算法清单、平台 binary SHA 和数据 SHA；
4. 计算稳定业务结果 v2 指纹：验证终态 snapshot 的 run/head/checksum，完整 event-type
   histogram，以及每份 right 的 grant、request right/context、response 与唯一 terminal；按 BUY/SELL 独立闭合
   `C/P/A/E/D/B/I/R`。合法 partial `Blocked` 可与最终 `Reserved` 并存，但缺 terminal、重复
   terminal 或跨 right 正负抵消均 fail closed。随后认证
   决策/意图/部分与最终 fill 数量，以及终态现金、持仓、费用、两种估值、
   lot/order 和 level 集合。v2 还从终态 strategy snapshot 独立证明每个 lot 无负已实现收益或
   负剩余成本、每个 tranche 的数量与预算双重守恒，并将 Paper 现金、仓位、开放订单、order/fill
   ID、accepted/rejected/cancelled report 与累计成交量逐项对账。SELL allocation 必须携带成交价、
   commission/tax、cost basis、worst fill、maximum fees 与 worst-case profit；费用必须全部归属到
   allocation，逐 slice 实际/最坏收益非负，且 sold+remaining cost 精确守恒 allocated cost。
   rejected/cancelled 必须为零 fill。时间戳、wall/RSS、数据库
   大小不进入该指纹；部分成交与最终成交事实均按各自 fill 数量累计，typed `DEFER` 即使省略
   `exercise_quantity` 也按零处理，并验证 `C=A+P`、`A=E+D`、`I=E-B`、`R=D+B` 以及
   BUY/SELL 意图/成交总量闭包。Decimal 文本由全局 canonical helper 生成，`1` 与 `10` 不得因
   去零发生哈希碰撞。方向错配、坏 snapshot checksum、费用残余、负 slice/lot、成本或 tranche
   不守恒、Paper 不一致都必须在
   生成 Q hash 前 fail closed；任何业务指纹漂移始终阻断；
5. 以独立 CLI 进程采集 cold status p95，并完整 rebuild；
6. 启动真实 Rust Web core，采集 sequential snapshot、bars(1000)、events(1000) 和四并发
   snapshot 的 nearest-rank p95；
7. 停止服务并评估同一版本化阈值表。nightly 固定 7 次采样，nearest-rank p95 在 7 个
   样本上等价于取最大值；不允许用 `iterations=1` 冒充该合同。

本地只运行短模式，不重复消耗约 23 分钟做三年 replay：

```sh
cargo build --locked --release --bin gridedge
work="$(mktemp -d /tmp/gridedge-perf.XXXXXX)"
python3 scripts/perf_gate.py \
  --mode short \
  --binary target/release/gridedge \
  --config configs/default.yaml \
  --data tests/fixtures/sample.csv \
  --output-dir "$work" \
  --run-id perf-short
python3 -m unittest -q tests/test_perf_gate.py
```

每次 CI 都上传 `input-identity.json`、`metrics.json`、`database-size.json` 和
`summary.md`。`input-identity.json` 记录 expected/actual SHA、字节数、冻结阶段重验、
workload/threshold/harness/manifest identity、账本反证和业务结果。`metrics.json` schema 2
嵌入同一身份并包含环境、历史基线、全部原始测量、每项阈值和 blocking/warn 判定；
`database-size.json` 包含文件字节数、MiB、page size/count、freelist count、live bytes/MiB
和冗余 event index 数量。artifact 不
上传包含完整交易日志的 SQLite 文件，既保留体积证据，也避免把研究账本数据复制到构建
产物。nightly artifact 保留 30 天，PR sample artifact 保留 14 天。
