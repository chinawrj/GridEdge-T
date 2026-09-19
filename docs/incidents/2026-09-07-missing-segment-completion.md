# Open: no authoritative completion for the last five-minute bucket

Status: OPEN production availability defect. Neither collector 0.6.53 nor the
additive operational supervisor fixes or certifies this data-source gap.

On 2026-09-07, the reviewed source's last morning trade was 11:29:48. The exact
PostgreSQL-committed `LIVE_CONTIGUOUS` source sequence 35808 carried that coverage
and was received at 11:30:14. The preceding source observation 35806 had a fresh
HTTPS source clock of 11:29:56 and trade coverage only through 11:29:42. At lunch
the ledger/outbox were 1902/1902 and the last complete processed bar was 11:25,
with received/decisions/processed events 1895/1896/1897. The strategy did not
evaluate the 11:25–11:30 bucket because it had no executable completed bar.
The same class was already visible at the September 3/4 final trading segment.

A read-only examination of the exact production page confirmed latest-first,
page 1/8, latest row 11:29:48, and unavailable header totals (displayed as `-`).
The page exposed no usable source-native segment-complete marker. The CAPTCHA
was not interacted with and must not be bypassed. No synthetic 11:30 or 15:00
trade, status, watermark or bar was appended.

Independent review verified the existing contract: `LIVE_CONTIGUOUS` and
`SESSION_HISTORY_COMPLETE` release a bucket only when trade coverage reaches its
end. `SOURCE_OBSERVED_CURRENT` is status-only; a fresh HTTP Date or wall clock
does not expand trade coverage. A partial/discontinuity boundary discards unsafe
buckets and cannot be relabeled as completion.

## Evidence required for a real repair

The current source must supply an authoritative completion marker, or a separately
reviewed source must supply equivalent evidence for the exact instrument,
trading day and segment. A source's completed bar or explicit terminal state
must have a documented meaning and immutable provenance; elapsed time alone is
insufficient. If completeness is proved through source totals, exact volume and
amount reconciliation must cover the complete relevant source history. Prices,
amounts and quantities retain Decimal/integer representation.

Any new path needs a separate source identity and isolated capture, committed-ACK
and replay validation before integration. It must reject stale, future, cross-day,
wrong-instrument and unbound claims, and must not turn reconnect history into
orders. The existing explicit lot/tranche/no-loss and mathematical-rights gates
remain unchanged. No alternative endpoint or publisher has been installed.

A request for an available authorized source/documentation entry was sent while
the existing deterministic recovery work continued. Until real source evidence
is available and independently reviewed, the last bucket remains unexecuted and
the overall Goal remains active. A later live window can collect evidence; it
cannot retroactively erase the missed completion or justify a fabricated bar.


## Public protocol reference investigated; no feed connected

The official Shenzhen Stock Exchange Binary specification Ver1.17 (March 2025),
sections 3.3, 4.4.1, 4.4.4 and 4.5.4, describes channel sequence numbers and
retransmission, channel-heartbeat last sequence/EndOfChannel, aggregate channel
close state, and instrument snapshots with OrigTime, trading phase, trade count,
total volume and total amount. For cash-equity snapshots it defines B as break
and E as closed. These definitions were checked in the complete relevant text and
the printed pages 16/17 phase tables.
[Official SZSE specification](https://www.szse.cn/marketServices/technicalservice/interface/P020250328368568358456.pdf).

Engineering inference: a phase flag by itself still does not prove this local
DOM stream received every trade. A candidate would need authenticated source
provenance, exact day/instrument/segment binding, sequence or aggregate
reconciliation, and successful independent replay. A channel's daily end marker
must not be assumed to certify a midday break. No feed access, credential,
provider adapter or production source identity was added during this research.

Research artifact SHA-256: PDF e15a70e356420c7aca00abfc29750717a35c5213a521c24e402ff52ea8885c6d; extracted text 0f4091e84c9638ee09924b92f6abf4063ac2e9c817971c5ca2de1269256c92ba. These are research identities, not production admission evidence.

A read-only inventory of the existing market database found no BAR events.
The current provider has TRADE_TICK and SOURCE_STATUS; other quote/tick source
IDs are legacy simulation/probe/certification records and are not substituted
for current authoritative completion evidence. No database row was altered.
The inventory is preserved as available-market-event-types.txt.

Mode audit correction: 11:25 was processed in READ_ONLY recovery, followed by RUNNING reconciliation. The morning contained 19 received-in-RUNNING and 4 received-in-READ_ONLY bars (10:35, 10:50, 11:15, 11:25). Current RUNNING plus a three-stage chain is not proof of normal strategy evaluation for that bar. No historical strategy/order evaluation is replayed to fill this gap.

Read-only source evidence ties these four transitions to stale trade coverage,
despite fresh page observations. At journal events 1840/1853/1879/1894,
source sequences 34682/35048/35545/35715 had observation ages below 0.75 seconds
and consecutive observation gaps below 7.01 seconds, but coverage ages of
60.758/63.747/61.695/62.607 seconds. The existing core therefore entered
EASTMONEY_SOURCE_OBSERVATION_CATCHUP through its trade-coverage gate. This is
neither an ACK timeout nor evidence that the strategy found no opportunity.
The source records cannot distinguish a genuinely quiet tape from an incomplete
display with sufficient authority to advance coverage. The 60-second gate stays
intact; fresh HTTP time alone does not repair this evidence limitation.
Exact event IDs and timestamps are retained in catchup-cause-audit.json and
catchup-source-status.json in the closure evidence archive.

## 2026-09-08 same-source aggregate route rejected

A post-close read-only probe inspected the aggregate snapshot endpoint already
loaded by the reviewed Eastmoney page. It returned the exact instrument code and
name plus numeric cumulative-volume (`f47`), cumulative-amount (`f48`) and trade
time (`f86`) fields; the observed `f86` mapped to 15:34 Asia/Shanghai. This is a
real same-source aggregate sample, not a production admission.

The page's own current script also establishes the missing half of the proof:
the time-sales history request contains only `f51..f55`, rendered as time,
price, quantity and direction. It has no exact per-row amount and no native
complete/revision marker. Consequently the current time-sales bytes cannot be
reconciled exactly to both aggregate volume and aggregate amount as required
above. A source clock later than a bucket and a matching aggregate snapshot do
not fill that gap. No aggregate-derived watermark or bar was implemented.

The exact sample, response hash, inspected field sets and negative admission
decision are recorded in
`../plans/evidence/2026-09-08-eastmoney-aggregate-probe.json`. Any future
same-source route must first provide exact amount-bearing history and documented
bucketing/completion/revision semantics, then pass the existing isolated
capture/ACK/replay and independent review gates. Repeating the current endpoint
probe is not the next action.
