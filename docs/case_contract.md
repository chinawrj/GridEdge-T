# Executable case contract

Business semantics are specified before implementation as strict YAML fixtures and executed by Rust tests. YAML amounts are decimal strings; loaders reject unknown fields. Rust property tests supplement these cases but do not replace them as the human-readable product contract.

Current catalogs:

- `tests/fixtures/rights_cases.yaml` (`version: 4`): 24 directed crossing paths covering monotone movement, whole-unit partial/full exercise, multi-level reversal, repeated excursions, same-level lockout, deterministic gaps, ambiguity, platform blockers, exact position/base boundaries and buy/sell source symmetry. Every current decision and order-intent oracle is an integer multiple of the catalog's `standard_quantity`; lot/tranche/fill residuals remain share-denominated.
- `tests/fixtures/no_loss_cases.yaml`: exact break-even, one-tick-below rejection, buy commission, minimum sell commission, tax, slippage and partial-slice boundaries.
- `tests/fixtures/case_coverage.yaml`: required case IDs and direction/path/decision/blocker/equivalence dimensions. CI fails if a required regression disappears even when the total test count remains high.

Every rights case identifies the market path, exact typed `exercise_quantity/defer_quantity`, exact mechanical touches, required and forbidden opportunity events, the complete right set, every decision-time tranche disposition and every order-intent quantity. Every `GridLevelTouched` must resolve to exactly one grant or explicit skip; every grant must have exactly one decision log. Unscripted algorithm calls fail the case. The same case runs continuously, with recovery before every bar and with every input bar duplicated; normalized **opportunity traces** and grid-semantic projections must agree.

Catalog v4 treats `standard_quantity` as the algorithm's indivisible unit. A “partial exercise” means an integer number of whole units selected from a larger whole-unit authorization, such as `E=Q,D=Q` when `A=2Q`; it never means a board-lot fraction of one unit. Residual tranche or fill shares are covered by execution/accounting cases and cannot appear as current catalog decision or order-intent quantities.

The catalog's scope ends at valid order-intent formation. A deterministic Paper adapter may prepare prerequisite lots for a later SELL opportunity, but acceptance/fill events, cash, positions, realized PnL and post-fill right states are excluded from the rights-case oracle. Broker acceptance, fills, rejection and cancellation remain independently tested execution concerns and are not combinatorially enumerated as grid-algorithm cases.

An independent property test generates random adjacent BUY-depth walks and compares the platform against a minimal layer-stack oracle. This covers histories beyond the curated goldens; it supplements rather than replaces named cases. A required case ID cannot be replaced by increasing the number of unrelated random examples.

## Fixed review seats

Two domain seats are permanent release gates:

- Mathematics expert: proves conservation, state transitions, reversal/carry semantics, rounding and boundary monotonicity.
- Quantitative expert: validates market microstructure, fee and tax treatment, T+1, lot selection, PnL attribution and absence of forward-looking data.

Both seats re-review every change to rights, allocation, execution price, fees or PnL semantics. Architecture and reliability reviews remain independent additional gates.
