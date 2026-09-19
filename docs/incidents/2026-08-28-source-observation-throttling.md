# 2026-08-28 source-observation throttling

- Detection: 14:53:39 CST. The formal worker changed from `RUNNING` to `READ_ONLY` after a 92.98-second `SOURCE_OBSERVED_CURRENT` gap, while trade rows and the reviewed page remained available.
- First recovery action: 14:54 CST. Refreshed the exact reviewed Eastmoney page and renewed its cross-patrol handoff marker.
- Root cause: production extension 0.6.28 still drives source-observation heartbeat from a content-page interval that Chrome can throttle while the page is backgrounded.
- Mitigation: page refresh restored approximately 12-second observations; terminal account reconciliation passed and the worker returned to `RUNNING` at 14:54:54 CST.
- Validation: the worker processed the completed 14:55 bar through `MARKET_DATA_RECEIVED -> MARKET_BAR_DECISIONS_COMMITTED -> MARKET_BAR_PROCESSED`; ledger/outbox reached 871/871.
- Recurrence: at 14:58:02 CST the worker correctly returned to `READ_ONLY` for observation catch-up and remained non-ordering through the 15:00 close. Final terminal reconciliation at 15:05 synchronized ledger/outbox to 872/872 and the worker exited normally.
- Operational result: the day is unsuccessful under the availability contract because an ordinary liveness fault removed order-submission availability during the final minutes, despite safe final reconciliation and no unknown orders.
- Pending release: candidate extension 0.6.29 moves the heartbeat to an MV3 alarm-backed lane and has deterministic tests plus test-expert READ_ONLY approval, but its prior real E2E evidence is invalid because the shadow publisher collided with the formal MQTT topic. It must be revalidated behind a physically isolated topic path before promotion.
