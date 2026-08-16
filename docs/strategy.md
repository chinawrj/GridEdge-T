# Strategy and grid rights

The grid grants a right; it does not force an order. Each genuine crossing creates one deterministic `right_id` scoped by run, cycle, level and crossing time. Its lifecycle is `GRANTED → RESIDUAL | DEFERRED | BLOCKED | RESERVED → PARTIALLY_EXERCISED | EXERCISED | RELEASED`, with price-invalidated rights becoming `REVOKED` and remaining rights becoming `EXPIRED` when the cycle ends.

Every mechanical opportunity is an append-only fact whether or not it produces an order. A touch must resolve to exactly one grant or one explicit skip; the two facts are mutually exclusive. A grant must record exactly one typed decision. A fully deferred point therefore remains visible as `TOUCHED → GRANTED → DECISION(DEFER)`, while a platform denial records `TOUCHED → GRANTED → BLOCKED`. `SKIPPED` is reserved for a touch that creates no right, such as a same-excursion re-touch. Derived state and the chart are projections of this opportunity log, never substitutes for it.

`RESIDUAL` means the exact eligible capacity is smaller than one standard quantity and is retained only as shares (`P`); the algorithm was not asked to decide it. `DEFERRED` means the algorithm deliberately declined whole-unit authorization. `BLOCKED` means it requested whole-unit exercise but platform risk controls denied it, for example cash, maximum position, T+1, sellable stock or base-position protection. Broker rejection produces `RELEASED`; it does not consume mechanical capacity.

`RESIDUAL` means the exact eligible capacity is smaller than one algorithm unit after applying `P=C mod Q`, or consists solely of that remainder. It is a platform-held share balance, not an algorithm `Defer`, not a risk `Blocked`, and never an order intent. The residual remains attached to its source tranche, is displayed in shares, participates in conservation and must be revoked, transferred or expired with its owning cycle rather than leaking into the next cycle.

For a buy at negative level `k`, cumulative mechanical capacity is `abs(k) × standard_quantity` shares. `standard_quantity` (`Q`) is the algorithm's indivisible unit and is itself a fixed board-lot multiple: price changes alter cash settlement, never the number of shares in one unit. Current decisions use the following exact integer partition:

```text
C = A + P                 P = C mod Q, 0 <= P < Q
A = E + D
I = E - B                 R = D + B
```

`C` is the exact eligible tranche capacity, `P` is the platform residual retained as shares, `A` is the whole-unit quantity offered to the algorithm, `E/D` are the algorithm's exercise/defer choice, `B` is the whole-unit quantity blocked after that choice, `I` is the order-intent quantity and `R` is the whole-unit remaining right. `A/E/D/B/I/R` must all be non-negative multiples of `Q`. `lot_size` remains the execution and accounting granularity only: individual tranche allocations, partial fills, releases and residual lot slices may be board lots smaller than `Q`, but a current algorithm decision and its total order intent may not be fractional units. Historical decision schemas retain their exact recorded quantities for read-only replay and must never be rendered as current whole-unit decisions.

The audited gate `action` describes the typed discrete result, not a pre-discretization target: it is `EXECUTE` only when `E > 0`. Current contracts treat typed `E/D` as the quantity truth; `alpha` remains an explanatory algorithm signal and cannot override or relax the whole-unit equations. A positive fractional `alpha` with typed `E = 0` therefore records `SKIP` and a Defer outcome.

SELL platform approval is closed over the exact no-loss lot plan. `I` is the largest candidate in `0,Q,…,E` that passes platform risk and whose single canonical allocation covers exactly `I` shares with every slice non-negative after fees. The platform must not plan a larger quantity, floor its partially safe sum, and then independently re-plan the smaller quantity: minimum commissions make that transformation non-monotonic. The approval helper therefore returns quantity and allocations together, and the same result drives reservation, intent, fill accounting and ledger validation.

BUY platform approval is closed over the exact order-level reservation. At the adverse fill price it reserves total notional plus one cumulative commission, `max(total notional × rate, minimum commission)`. A possible 700+800 partial-fill path does not create two orders and therefore cannot reserve or charge two minimum commissions; each fill charges only the delta between cumulative commission now due and commission already charged. Current schema 6 records this rule, while schema 5 preserves its historical two-minimum reservation for read-only replay. The service and Ledger independently recompute the current rule, so exact cash approves one whole unit and a one-cent shortage blocks that unit even if disposition, tranche reservation and intent are forged together.

Slippage and fees affect cash accounting but never inflate or shrink the mechanical right. Historical ledgers created before the quantity contract retain their recorded `standard_budget` semantics for replay only and cannot start a new run.

Unrealized valuation has two distinct, audited views. Mark-to-market uses the latest fully processed market price for every remaining strategy lot and subtracts its effective remaining buy cost; it deliberately excludes future exit charges. `LOT_CONSERVATIVE_EXIT_V1` instead prices every remaining lot independently through the same adverse fill-price and fee helper as the no-loss proof, including one minimum commission per lot plus sell tax. It retains negative results rather than filtering them through the sell eligibility guard. T+1, frozen and currently unsellable quantities are still valued because this is economics, not an executable sell plan; base inventory outside strategy lots is excluded. Missing mark, any unknown remaining lot cost, or a missing audited profit policy makes the affected valuation unavailable rather than zero. The identities are exact:

```text
total_mark_to_market = realized + mark_to_market_unrealized
total_conservative_exit = realized + conservative_exit_unrealized
conservative_exit_adjustment = conservative_exit_unrealized - mark_to_market_unrealized
```

The compatibility fields `unrealized_grid_pnl` and `total_grid_pnl` are exact
aliases of the corresponding mark-to-market fields. They contain identical
Decimal text when valuation is available and are both null when the mark or
cost basis is unavailable; they never fall back to a different calculation.

Every projection is seeded from a durable run context, never from the caller's current configuration. For a current run, `RUN_STARTED` supplies exact initial cash/position/sellable values and must agree with the hash-verified `CONFIG_SNAPSHOTTED` event on those values, symbol, cycle, correlation and configuration version. The same context seeds full replay, snapshot replay, incomplete-bar prefix replay, Web/native views and CLI queries. A missing or corrupt snapshot therefore changes only the recovery path, not the initial account or valuation. Runtime identity drift is tolerated for read-only inspection because the ledger remains authoritative, but it is rejected before claiming any new write command.

For a sell at positive level `k`, capacity is share-based. It contains eligible strategy-lot slices opened at depths `-1…-k`, including slices whose earlier sell rights were deferred. Each slice retains its lot source; the selection order is `DEEPEST_FIRST_V1`, then opening date and lot ID. T+1, base position, sellable quantity and per-lot break-even are classified before `C`; excluded quantities remain independently audited in shares and are never folded into `C`, `P`, `D` or `B`. Only the resulting eligible exact slices form `C`, and only `C mod Q` forms `P`.

The rights machine is a discrete crossing automaton. A downward `Enter(k)` adds exactly one new BUY tranche of `standard_quantity`; carry only changes the owner of surviving shallower tranches. An upward `Exit(k)` revokes only the available tranche born at `k`. Reserved or consumed quantities never roll back. Within one monotone leg carry is strictly deeper. After a genuine reversal, re-entry may re-offer surviving shallow authority at a previously visited depth, but it creates a new epoch token and never revives a revoked token. Any BUY depth with consumed quantity remains locked for the rest of that cycle.

The core grid-case boundary ends when a valid order intent has (or has not) been formed. Acceptance, fills, rejection and cancellation belong to the independent execution state machine; those outcomes cannot rewrite the already audited `Exercise/Defer` choice.

The first bar only establishes prior price. A crossing decision sees only earlier completed closes plus the touched grid price; it cannot see the current bar's final close, VWAP or high-low range. Rearming happens after all decisions for that bar, preventing impossible same-bar rearm/retrigger ordering. Opposing intrabar crossings remain conservatively ambiguous by default. Levels ±5 are observation boundaries and never create risk.
