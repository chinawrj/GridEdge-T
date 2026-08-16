# Ledger

An order intent reserves a grid right but changes no cash, position or lot. Submission freezes worst-case buy cash (adverse slippage plus commission) or sell quantity. Rejection and cancellation release both asset and right reservations. Only a unique, validated fill changes accounting.

The order aggregate rejects direction mismatches, overfills, inconsistent final flags, foreign target lots and fills beyond reserved resources before the event is committed. Partial fills cannot collectively exceed the intent. Buy fills open independent lots; sell fills close only named, prior-day lots while respecting sellable quantity and the base position.

Commission is `max(cumulative order notional × rate, minimum commission)` and each partial fill charges only the delta from commission already charged. A BUY cash reservation applies that same cumulative function once to the whole order at its adverse fill price; it must never multiply the minimum commission by an assumed number of fills. Thus a 1,500-share order filled as 700+800 shares still has one cumulative minimum commission, with the second fill charged only the remaining delta. Stamp tax applies to sell fills. Realized P&L deducts allocated buy cost, including buy commission, and all sell fees. Prices, money and rates are `Decimal`; share quantities are integers.

The independent Paper Broker persists its own account and execution report atomically. The strategy ledger never overwrites it. Reconciliation checks cash, frozen cash, total/sellable/frozen shares, unfinished orders and cumulative fills.
