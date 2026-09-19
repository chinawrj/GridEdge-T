# 2026-08-28 shadow MQTT topic collision

- Detection: 14:36 CST. The formal worker received candidate source instance sequence 1 and exited on its source-identity continuity gate.
- First recovery action: 14:36 CST. Stopped the candidate publisher and its isolated Chrome process, then closed the formal reviewed market page to freeze both streams.
- Root cause: the candidate had an isolated source instance, MQTT client, raw log and ledger, but published to an MQTT topic matched by the formal worker's subscription.
- Evidence preservation: retained the pre-collision formal raw log, the candidate E2E log and `shadow-topic-collision-recovery-20260828-1436.json` under the installed runtime directory.
- Mitigation: identified formal afternoon anchor sequence 11666, atomically rebuilt the formal raw stream as production-instance sequences 11666 through 13173, replayed 1508 events into 15 bars successfully, and reset the formal persistent MQTT session only after the raw rebuild.
- Validation start: 14:45 CST. Started the installed signed worker directly in `READ_ONLY`, then reopened and handed off the sole reviewed Eastmoney page.
- Restoration: 14:47 CST. A current 14:45 bar traversed `MARKET_DATA_RECEIVED -> MARKET_BAR_DECISIONS_COMMITTED -> MARKET_BAR_PROCESSED`; terminal Paper/Android reconciliation passed; the worker changed to `RUNNING`; ledger/outbox reached 862/862.
- Release decision: candidate extension 0.6.29 was not promoted. Its real E2E evidence is invalidated by the topic collision and must be repeated behind a physically isolated broker/topic namespace or a verified formal-side topic filter fence.
- Prevention: project instructions now explicitly prohibit candidate or shadow publication on any topic matched by a formal subscriber. Unique source/client/state identities alone do not establish publication isolation.
