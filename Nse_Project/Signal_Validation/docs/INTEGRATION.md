# Integration

## Read-only bridge

The system consumes three Redis artifacts produced by the existing live feed
(unchanged):

| Artifact | Key / channel | Used for |
|---|---|---|
| Publish notification | `gamma:live_options_updated` | capture trigger |
| Option-chain snapshot | `live_options_data` | spot, expiry, ATM CE/PE LTP |
| Market metrics | `market_state:{IST-date}` | bias, momentum, score, gamma_boost, iv_skew |

The collector SUBSCRIBEs to the channel and GETs the two keys on each publish.
It never SETs, PUBLISHes, or DELETEs. `ReadOnlyRedis` in `logger/collector.py`
blocks every write method with `RedisReadOnlyError` (proven by
`tests/test_no_write.py`).

## Data flow

```
feed (unchanged) -> Redis -> collector -> extractor -> ring buffer -> enricher
                 -> writer (raw + enriched JSONL) -> evaluator -> reports
```

## Storage isolation

Code and configs live in `Signal_Validation/`; data/logs/reports default to
`D:/Signal_Validation_Data/{raw,processed,reports,logs}` and relocate together
via the `SIGVAL_DATA_DIR` environment variable.

## No-schema-change guarantee

`docs/SCHEMA.md` is frozen. Phase-5 refinements only added derived report
columns (`edge_delta`, `max_drawdown_cooldown`); the raw/enriched JSONL schemas
are unchanged, so ongoing daily captures remain compatible.
