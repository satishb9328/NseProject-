# Live Sniper Scan Diagnosis

## Executive summary

The repeated zero counts are not evidence that Upstox is returning valid but
pattern-free data. The saved 5-minute cache is stale, and the old pipeline
could silently turn an API, parsing, import, or per-symbol exception into an
empty result. The old 2-hour resampling was also aligned to midnight rather
than the NSE session, and it evaluated a still-forming 2-hour candle.

The code now logs API status and payload quality, reports per-symbol scanner
stages and rejection reasons, converts API timestamps to IST, builds session-
aligned 2-hour candles, and excludes incomplete candles. A failed or empty
live scan is no longer treated as a successful zero-result run that clears
existing badges.

## Evidence collected

- The scanner's configured VIP universe contains **26 symbols**. All 26 have a
  matching cached 5-minute parquet file.
- Every one of those parquet files ends on **2026-09-21**. The cache directory
  was last modified on 2026-09-23; the master setup CSV was modified on
  2026-10-20. The current environment date is 2026-10-22, so the cached prices
  are not current.
- Across the 26 VIP parquet files, the observed data had **0 rows with missing
  OHLC values** and **0 rows with missing volume**. This only validates the
  cached data, not the live Upstox response.
- Re-evaluating the latest closed, session-aligned 2-hour candle from that
  stale cache produced no patterns for all 26 symbols: **12** latest lows were
  outside the 1.5% EMA50 support band and **14** had no supported reversal
  candle structure. These old candles cannot establish whether today's market
  should produce a signal.
- No authenticated live Upstox request was made during this diagnosis.
  Therefore, HTTP status, current candle count, live OHLC/volume validity, and
  current-token validity remain unverified. Previously, the fetch helper
  suppressed request exceptions, non-200 responses, invalid JSON, and empty
  candle responses, so the old run log did not provide evidence either way.
- The access header is already constructed as a Bearer token header; the
  redacted display of that expression should not be mistaken for a literal
  mask or an authorization defect.

## Findings by requested area

### 1. Upstox data retrieval

The scanner requests **1-minute** Upstox historical candles, then resamples
them to 5-minute candles and combines those with the cached 5-minute parquet
history. It does not request 2-hour candles directly.

The previous fetch helper returned an empty dataframe without explaining
whether that meant an HTTP error, timeout, empty response, bad JSON, or invalid
rows. The management command also swallowed scanner exceptions, which could
make a failed call look like a successful run with zero signals.

The updated fetch path now prints request failures and HTTP status, records
received and valid candle counts, checks timestamp/OHLC/volume validity, sorts
and deduplicates timestamps, and reports the newest returned candle. It
normalizes numeric values and rejects rows missing a timestamp or OHLC while
reporting the number rejected. Missing volume is counted and reported rather
than silently hiding the row.

**Action on the next run:** check each `[DATA] <symbol>` line. HTTP 401 means
refresh the Upstox token using the existing login flow; other non-200 statuses
and response text identify endpoint, key, or entitlement issues. HTTP 200 with
zero candles or a last timestamp before today means the live data is still
unavailable/stale. The next run is required to determine which condition
applies.

### 2. Strategy criteria and evaluation

The sniper detector requires all of the following on the latest completed
2-hour candle:

1. At least 50 2-hour closes to initialize EMA50.
2. The candle low to be within **±1.5%** of EMA50.
3. One of five exact bullish structures: hammer, bullish engulfing, piercing
   line, morning star, or marubozu.

These are restrictive conditions, but the local historical sample cannot
justify loosening them. In the cached sample, 12 of 26 latest candles failed
the EMA proximity filter and the other 14 failed the candle-structure tests.
The new `[SCAN]` log reports the EMA distance, proximity result, each
structure's boolean result, and the reason for rejecting that symbol. Use
fresh live candles before adjusting the 1.5% band or pattern definitions.

Mega Reversal is independent of the sniper candle patterns. It requires at
least 20 completed weekly closes, the previous session's close below the
20-week simple moving average, and a same-day live price at or above that
average but no more than 3% above it. `load_weekly_bb20_midline` computes a
rolling 20-week **SMA**; it does not calculate a Bollinger band with
standard-deviation bounds. The `[MEGA]` line now states which precondition
failed for each symbol.

### 3. Timeframe and timestamp alignment

The cached parquet timestamps are naive datetimes with sample session data
starting at 09:15. The old resampling used pandas' default midnight anchor,
which formed bars on 00:00-based 2-hour boundaries rather than NSE's 09:15
session boundary. It also explicitly allowed the latest forming candle to be
evaluated.

The updated code converts timezone-aware API times through UTC into IST and
then stores them as IST wall-clock-naive values, matching the cached history.
Two-hour bars are now anchored at 09:15 IST, and a bar is eligible only after
its full two-hour interval has ended. Thus, during a 12:19 IST run, the
11:15–13:15 candle is excluded; the latest eligible candle is the one ending
at 11:15. This prevents intrabar shapes from appearing/disappearing between
15-minute scans.

### 4. Parsing and silent filtering

Several silent failure paths existed:

- Upstox request and JSON/parsing errors returned an empty dataframe.
- Weekly-history read/column/row problems returned `None` without a reason.
- Per-symbol scanner exceptions were ignored.
- Scanner import/runtime failures in the Django command became `{}`, after
  which the command printed zero counts and a success-shaped result.

Those paths now emit explicit messages and scan counters. Fatal scanner
failures are raised as a Django `CommandError`, and scans without any valid
same-day live candles are marked failed so the command does not clear existing
patterns as though a valid empty scan had completed.

## Changes made

- Added API response, timestamp, OHLC, volume, history freshness, merge, and
  per-symbol strategy diagnostics.
- Added timeout and visible errors for failed Upstox requests and malformed
  responses.
- Added robust timestamp/numeric parsing for live 1-minute candles.
- Anchored 2-hour bars at NSE's 09:15 session open and excluded forming bars.
- Added explicit skip/failure status when no same-day live candle is available.
- Changed the management command to surface import/runtime failures rather
  than reporting them as zero signals.
- Restricted database badge updates to symbols whose live 2-hour evaluation
  completed, preserving existing badges for symbols skipped due to missing or
  invalid data.
- Added focused tests for IST conversion and closed, session-aligned 2-hour
  candles.

## Next-run checklist

1. Run `automation\live_sniper_update.bat` during an active market session.
2. Confirm `[DATA]` lines show HTTP 200 and valid same-day candle timestamps.
3. Check `[SUMMARY]` for nonzero live candles and zero unexpected history
   errors; investigate any HTTP or parsing errors before interpreting zero
   signals.
4. For symbols with live data, use `[SCAN]` and `[MEGA]` rejection reasons to
   decide whether a strategy threshold is responsible. Do not tune thresholds
   from the stale September cache.
5. Confirm the cache refresh process is separately scheduled if the project
   expects the on-disk 5-minute parquet files themselves to stay current; this
   live scanner only merges Upstox candles in memory and does not persist them
   back to parquet.
