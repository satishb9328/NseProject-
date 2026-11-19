# Signal Validation System — JSONL Schema (Contract)

> Phase 1 deliverable. This document is the single source of truth for the
> on-disk record format. Every field name and unit below is normative; the
> logger writes exactly these fields and the evaluator reads exactly these
> fields. Horizons are `[15, 30, 60, 120]` seconds everywhere.

## 1. Purpose

Two lossless, append-only, newline-delimited JSON (JSONL) artifacts:

| Artifact | File | Written by | Consumed by |
|---|---|---|---|
| **Raw capture** | `signal_{date}.jsonl` | logger (immediately, on every deduped publish) | enricher + evaluator self-join |
| **Processed enriched** | `signal_{date}.enriched.jsonl` | enricher (after forward horizons elapse) | evaluator |

`{date}` is the IST trading date (`YYYY-MM-DD`). The raw capture is the
**ground truth** and is written *before* enrichment, so a crash can never lose
a signal — only forward values, which the evaluator rebuilds by self-join.

## 2. Common rules

1. **Encoding** — UTF-8, LF line endings, one JSON object per line, no trailing
   comma, no pretty-printing.
2. **Null vs zero** — a missing/unobserved value is `null`, never `0`. A zero
   would masquerade as a real move and corrupt labels.
3. **No non-finite** — `NaN`/`Infinity` are forbidden; write `null` instead.
4. **Time** — all timestamps are ISO-8601 strings with the `+05:30` offset
   (`YYYY-MM-DDTHH:MM:SS.mmm+05:30`). See §5 for the three distinct time fields.
5. **Sequence** — `seq` is a monotonic per-process counter (starts at 1,
   resets on process restart). Uniqueness of a row is `(session_date, seq)`;
   `captured_at` disambiguates across restarts.

## 3. Raw capture row (`signal_{date}.jsonl`)

One object per deduplicated publish snapshot. Mandatory fields unless marked
"nullable".

| Field | Type | Req | Units | Meaning / source |
|---|---|---|---|---|
| `row_type` | string | ✅ | — | constant `"capture"` |
| `ts` | string | ✅ | ISO-8601 | canonical record time; equals snapshot `updated_at` normalized to IST, else `captured_at` |
| `updated_at` | string\|null | ✅ | — | feed snapshot `updated_at` preserved verbatim (provenance) |
| `captured_at` | string | ✅ | ISO-8601 | validator receive wall-clock (gap detection) |
| `seq` | int | ✅ | — | monotonic per-process capture counter |
| `session_date` | string | ✅ | `YYYY-MM-DD` | IST trading date; file partitioning key |
| `spot` | float | ✅ | index points | `live_options_data.spot_price`, must be `> 0` |
| `active_expiry` | string | ✅ | `YYYY-MM-DD` | `live_options_data.expiry_date` |
| `atm_strike` | float | ✅ | index points | pinned ATM strike (nearest `|strike - spot|`); pinned for forward CE/PE |
| `bias` | float | ✅ | % | `market_state.bias` |
| `momentum` | float | ✅ | % | `market_state.momentum` |
| `gamma_boost` | float | ✅ | score points `[-25, 25]` | `market_state.gamma_boost_val` |
| `score` | float | ✅ | `[0, 100]` | `market_state.score` |
| `iv_skew` | float | ✅ | IV percentage points | `market_state.iv_skew_est` |
| `atm_ce_ltp` | float\|null | — | ₹ | active-expiry chain `call_ltp` at `atm_strike` |
| `atm_pe_ltp` | float\|null | — | ₹ | active-expiry chain `put_ltp` at `atm_strike` |
| `data_source` | string\|null | — | — | `"redis"` \| `"cold_json"` \| `null` |
| `feed_connected` | bool | ✅ | — | `live_options_data.feed_connected` |
| `stale` | bool | ✅ | — | `live_options_data.stale` |
| `seconds_since_last_tick` | float\|null | — | s | `live_options_data.seconds_since_last_tick` |

## 4. Processed enriched row (`signal_{date}.enriched.jsonl`)

Denormalized: **every raw-capture field above** (same names/types) is repeated,
then `row_type` becomes `"enriched"`, and the following forward fields are
added. Forward values are looked up at the **pinned** `atm_strike` / same
`active_expiry` — never a re-derived ATM strike.

| Field | Type | Units | Meaning |
|---|---|---|---|
| `spot_15s`, `spot_30s`, `spot_60s`, `spot_120s` | float\|null | index points | spot at `ts + h` |
| `ce_15s`, `ce_30s`, `ce_60s`, `ce_120s` | float\|null | ₹ | pinned ATM CE LTP at `ts + h` |
| `pe_15s`, `pe_30s`, `pe_60s`, `pe_120s` | float\|null | ₹ | pinned ATM PE LTP at `ts + h` |
| `complete` | bool | — | `true` iff all 12 forward fields are non-null |
| `reason` | string\|null | — | why incomplete: `"warmup"`, `"feed_gap"`, `"restart"`, `"expiry_rollover"`, `"strike_unavailable"`; `null` when complete |
| `missing_horizons` | list[string]\|null | — | horizons not filled, e.g. `["30", "120"]`; `null` when complete |

## 5. Derived forward-return columns (evaluator, not persisted)

The enriched JSONL stores **lossless forward prices**, never pre-computed
returns. The evaluator derives returns on load (so a return-formula change
never requires re-capturing data). Column names and formulas:

| Column | Formula | Units |
|---|---|---|
| `spot_ret_{h}_bp` | `10000 * ln(spot_{h} / spot)` | basis points |
| `ce_ret_{h}_pct` | `100 * (ce_{h} - atm_ce_ltp) / atm_ce_ltp` | % |
| `pe_ret_{h}_pct` | `100 * (pe_{h} - atm_pe_ltp) / atm_pe_ltp` | % |

where `{h} ∈ {15, 30, 60, 120}`. A return is `null` when either the base or the
forward price is `null` or the base price is `<= 0`.

## 6. Units reference

| Quantity | Unit | Example |
|---|---|---|
| spot / strike | index points | `24780.45` |
| option LTP | ₹ | `212.40` |
| bias / momentum | % | `3.2` |
| score | 0–100 | `58.3` |
| gamma_boost | score points, `[-25, 25]` | `0.7` |
| iv_skew | IV percentage points | `1.1` |
| seconds_since_last_tick | seconds | `0.4` |
| spot forward return | basis points | `7.1` |
| option forward return | % | `-1.2` |

## 7. Quality flag semantics

| Flag | Meaning | Effect on evaluation |
|---|---|---|
| `stale = true` | feed has not delivered a fresh tick recently | row excluded from labels |
| `feed_connected = false` | websocket down | row excluded from labels |
| `data_source = "cold_json"` | Redis unavailable; on-disk fallback served | row flagged; excluded by default |
| `atm_ce_ltp` / `atm_pe_ltp` = `null` | no quoted LTP at pinned strike | option labels unavailable (spot labels still valid) |

## 8. Completeness & nullability rules

1. `complete = true` **iff** all 12 forward fields (`spot/ce/pe × 15/30/60/120`)
   are non-null.
2. `reason` is `null` when complete, otherwise names the cause.
3. `missing_horizons` lists only the unfilled horizon labels.
4. A forward field is left `null` (never zero) when the future value was not
   observed — due to warm-up, a feed gap `> max_gap_seconds`, a process restart,
   an `active_expiry` change, or the pinned strike no longer being quoted.
5. The evaluator self-join can reconstruct `null` forward values from the raw
   capture file when the same-day future rows exist.

## 9. Example rows

Raw capture:

```json
{"row_type":"capture","ts":"2026-11-18T09:30:01.000+05:30","updated_at":"2026-11-18T09:30:01.000+05:30","captured_at":"2026-11-18T09:30:01.120+05:30","seq":4821,"session_date":"2026-11-18","spot":24780.45,"active_expiry":"2026-11-26","atm_strike":24800.0,"bias":3.2,"momentum":1.8,"gamma_boost":0.7,"score":58.3,"iv_skew":1.1,"atm_ce_ltp":212.4,"atm_pe_ltp":198.6,"data_source":"redis","feed_connected":true,"stale":false,"seconds_since_last_tick":0.4}
```

Processed enriched (same row, after horizons elapse):

```json
{"row_type":"enriched","ts":"2026-11-18T09:30:01.000+05:30","updated_at":"2026-11-18T09:30:01.000+05:30","captured_at":"2026-11-18T09:30:01.120+05:30","seq":4821,"session_date":"2026-11-18","spot":24780.45,"active_expiry":"2026-11-26","atm_strike":24800.0,"bias":3.2,"momentum":1.8,"gamma_boost":0.7,"score":58.3,"iv_skew":1.1,"atm_ce_ltp":212.4,"atm_pe_ltp":198.6,"data_source":"redis","feed_connected":true,"stale":false,"seconds_since_last_tick":0.4,"spot_15s":24781.10,"spot_30s":24782.10,"spot_60s":24779.80,"spot_120s":24788.20,"ce_15s":211.0,"ce_30s":210.0,"ce_60s":208.5,"ce_120s":215.9,"pe_15s":199.5,"pe_30s":200.1,"pe_60s":201.8,"pe_120s":193.4,"complete":true,"reason":null,"missing_horizons":null}
```

