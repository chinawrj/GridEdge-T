# Market data acquisition and provenance

## Primary source

GridEdge-T uses BaoStock's anonymous TCP service as the free historical minute-bar source. The pure Rust client implements the published client protocol directly; no Python runtime or Python data library is used by the project.

- Endpoint: `public-api.baostock.com:10030`
- API method: `query_history_k_data_plus`
- Native fields: `date,time,code,open,high,low,close,volume,amount,adjustflag`
- Supported minute frequencies used here: 5, 15, 30, and 60 minutes
- Adjustment flags: backward (`1`), forward (`2`), unadjusted (`3`)

The downloader splits requests into non-overlapping 90-day windows. Each window uses a fresh authenticated anonymous session, bounded retries with exponential backoff, and a configurable inter-window delay. The response is retained in source-native CSV before transformation.

## Reproducible snapshot

The 兆新股份 snapshot was requested for `002256.SZ` from 2023-08-14 through 2026-08-14 at 5-minute frequency with no price adjustment. It returned:

- 34,944 bars
- 728 observed trading days
- 48 bars on every observed day
- first bar: 2023-08-14 09:35:00
- last bar: 2026-08-14 15:00:00
- no duplicate timestamps, invalid OHLC relationships, negative volume, or out-of-session bars

The processed file SHA-256 is `f68e60309c7af91a3805f7144f6b7fec07fab4274bd5eb6102c051151ce19e7b`.

## Completeness boundary

The quality report verifies every day returned by the source. It does not treat an absent whole day as an error because a stock can be suspended while the exchange is open. A future enhancement should join an exchange trading calendar with authoritative suspension records before claiming exchange-calendar completeness.

Five-minute bars are interval aggregates and cannot reveal the price path inside an interval. GridEdge-T therefore records ambiguous multi-level bars and applies the configured conservative policy. Higher precision than five minutes requires a different licensed source and must enter through the same normalized CSV contract.

## Adjustment policy

Unadjusted bars represent traded prices and are the default for order and fee simulation. Forward-adjusted prices may be downloaded separately for return-based features or statistical models. Adjusted and unadjusted files must remain separate, and a replay configuration must use a single adjustment convention throughout a run.

## Source limitations

Data is obtained for research and simulation. Availability and vendor terms can change. Before redistribution or commercial use, verify the current source terms. Tushare's historical minute endpoint is a possible licensed fallback; its access tier and usage limits must be checked at acquisition time.
