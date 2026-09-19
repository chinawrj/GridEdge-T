# 2026-08-31 source-observation catch-up availability incident

## Outcome

The paper-trading day is operationally unsuccessful. Five grid touches during
the morning were evaluated while the service was `READ_ONLY`, so none was
eligible to create an order. The installed 0.6.30 collector later processed
current bars, but recurrent `SOURCE_OBSERVATION_CATCHUP` transitions meant the
order path was not continuously available. No order or fill was created.

The deterministic repair is complete as candidate 0.6.38. Production
promotion remains blocked only on two fresh physical-isolation live E2E rounds
in the next reviewed market window; post-close data must not satisfy that gate.

## Timeline (Asia/Shanghai)

- 09:00: formal pre-open recovery began.
- 09:30–11:23: the formal worker was not `RUNNING`; this exceeded the P0
  availability budget.
- 11:23: the formal worker first recovered to `RUNNING` after read-only catch-up
  and terminal Android/Paper reconciliation.
- 13:01: the afternoon formal path recovered to `RUNNING`.
- 13:03–14:58: recurrent source-observation catch-up transitions continued.
  The installed worker still processed current completed bars, including the
  14:55 three-stage bar, but did not provide continuous order availability.
- 14:42: fresh physical-isolation E2E R1 for 0.6.36 started; this was the first
  concrete live validation of the candidate chain.
- 14:51: R1 exposed a 48.653-second source-observation interval while the
  regular collector waited for an exact unchanged high-tick rowset.
- 14:58: R1 failed the 45-second operating target and was stopped. Its nonce
  root and evidence were preserved; all owned listeners were cleaned.
- 15:00: the reviewed market window ended. No second live round was fabricated.
- Post-close: an independent regression was added first. The collector was
  changed to prove rolling suffix-prefix continuity across two sequential
  reviewed snapshots, while atomic heartbeats remained status-only.
- Post-close: test-expert review found one further production-semantic gap:
  rolling continuity and durable deduplication needed one shared evidence
  projection. A real-provider regression was added red, then the shared helper
  was implemented and the regression turned green.
- Post-close: extension 148/148, deployment/ingestor 23/23, `cargo fmt`, strict
  Clippy and all Rust targets/features passed. The test expert approved 0.6.38
  for the two live READ_ONLY E2E rounds, not for production installation.

## Root cause and repair

0.6.36 required an exact unchanged time-sales rowset before regular ingestion.
During active trading the latest-first DOM can roll continuously, so the proof
could wait too long even though each individual page was valid.

0.6.38 instead accepts only a proven maximum non-empty suffix-prefix overlap
between two sequential reviewed snapshots. It rejects gaps, conflicts,
reordering and identity drift. The durable layer still independently requires
prior-watermark overlap and accepts only forward unseen facts. Atomic
heartbeats remain status-only and cannot ingest unseen trades or advance trade
coverage.

`stableMarketRowEvidence` is the single shared evidence projection for both
rolling continuity and durable event/deduplication identity. It excludes only
the reviewed presentation fields (`source_table_ordinal`,
`source_row_ordinal`, `source_same_second_ordinal`, `raw_cells`); future fields
are included by default.

## Frozen candidate and remaining live-only evidence

- Version: 0.6.38
- Candidate bundle:
  `/tmp/gridedge-market-e2e-bundle-e2e-0629-b21e73fe-1609-4465-a5ba-26007bcade48`
- Bundle manifest SHA-256:
  `e96d2e7ebc1d1c9f90057a0a3e0395033d15b90358e7ee7a3ef06e946ba4cba9`
- Source snapshot SHA-256:
  `ba81f6321868722b4e79a47b0b9ba6e11dde9ace09d95189a0379a74386858d5`
- Extension tree SHA-256:
  `9159a684e9e73a4e53baf9f28589bf1b2b5ee617fde09a15222b1771669e3245`
- Release binary SHA-256:
  `9ecace099fa65f01347a69c026cafa79d1474f5a1dde327f792f00227268989a`

At the next trading window, run two serial fresh-nonce physical-isolation
READ_ONLY rounds of 900 seconds each. Both must satisfy the source/committed
45-second target, sequence and delivery invariants, background high-tick
rolling coverage, complete local bar triplets and zero formal-topic, Android,
order or money-action effects. Only after both rounds and production review may
the candidate be signed/activated, followed by formal READ_ONLY catch-up,
fresh current evidence, terminal reconciliation, a current bar triplet and
stable `RUNNING`.
