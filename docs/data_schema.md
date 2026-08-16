# Replay CSV schema

Required columns: `timestamp,symbol,open,high,low,close,volume`; `amount` is optional. Timestamps use `YYYY-MM-DD HH:MM:SS`. Rows must be strictly increasing, unique, for the configured symbol, within trading hours, and satisfy positive prices, nonnegative volume and `low <= open/close <= high`.
