# VOLUME_FLOW_ENGINE and EXHAUSTION_ENGINE Design

## 1. Scope and design constraints

This design adds two downstream engines for NIFTY weekly-options scalping:

1. **VOLUME_FLOW_ENGINE** — estimates call/put option volume velocity and
   signed aggressive flow from Upstox V3 tick-updated data.
2. **EXHAUSTION_ENGINE** — combines recent spot movement, spot acceleration,
   volume/flow behavior, and optional published sentiment to score Bull
   Exhaustion and Bear Exhaustion.

The engines do not change the existing Bias, Momentum, Delta Wt, Score, IV
Skew, or Gamma Boost formulas. Those remain the authoritative existing
metrics described in
[`MARKET_METRICS_TECHNICAL_AUDIT.md`](MARKET_METRICS_TECHNICAL_AUDIT.md) and
[`REVERSAL_ENGINE_DESIGN.md`](REVERSAL_ENGINE_DESIGN.md).

All thresholds below are **initial design values**, not established trading
rules. They require replay/backtest and paper-trading calibration, including
fees, slippage, expiry-day behavior, and time-of-day effects. Exhaustion is a
warning that the current move may be tiring; it is not, by itself, a trade
entry.

## 2. Relevant existing data path and limitations

The current live worker subscribes to Upstox V3 `FULL` data and routes decoded
messages through `update_live_state_from_message`. Option entries carry
`option_type`, `strike`, `expiry`, cumulative `volume` (`vtt`), and live
option OI/IV. The current `compute_volume_velocity` calculates a per-contract
three-second rate from cumulative VTT. It is not a 20/40/60-second
flow-history engine and its UI/API field should not be overloaded.

The existing `CVDEngine` parses V3 option/future LTP, VTT, and the first
best-bid/best-ask quote. It derives a tick volume from consecutive cumulative
VTT values and signs that volume only when LTP is at/through the ask or bid.
Ticks inside the spread or without quotes are unclassified (zero signed
delta). The primary CVD worker forms two-minute candles; that output is too
slow to be the sole input for 20–60 second exhaustion detection.

Accordingly:

- The new volume engine should consume the **raw decoded feed updates** or
  their equivalent normalized tick records before the two-minute CVD candle
  aggregation.
- Upstox V3 updates and cumulative `vtt` are not necessarily an exchange
  print-for-print audit trail. Describe outputs as tick-derived estimates, not
  guaranteed complete TBT executions.
- The NIFTY index feed provides spot LTP but not index traded volume or
  bid/ask. Use live index LTP for price velocity; use the NIFTY future's
  volume and bid/ask for signed underlying flow.
- Current option volume velocity uses the raw `vtt` units. Keep the new
  engine in the same units; do not multiply or divide volume by lot size.
  Lot-size conversion used for OI/COI is separate.
- Call volume and put volume are **not intrinsically directional**. Call
  buying can be hedging; put selling can hedge another position. Unsigned
  call/put activity should be a participation feature, while aggressor-side
  signed flow is a separate, noisy confirmation.

## 3. VOLUME_FLOW_ENGINE

### 3.1 Responsibilities and inputs

Maintain bounded, timestamped per-instrument histories for:

- NIFTY CE/PE option contracts in the active weekly expiry;
- the selected active NIFTY index future;
- canonical NIFTY index spot ticks for timestamp alignment (spot itself has no
  volume).

For option legs, consume:

```text
instrument_key, expiry, strike, option_type, ltp, vtt,
best_bid, best_ask, exchange_event_time, local_receive_monotonic_time
```

For the future, consume the same price, cumulative volume, and quote fields.
Use exchange event time for ordering/alignment when valid; retain monotonic
receive time for detecting local gaps and for safe elapsed-time calculations.
Deduplicate repeated event samples and reject or separately quarantine
out-of-order events.

Only active-expiry contracts in the configured near-ATM band participate.
Recommended initial band: ATM ±4 strike intervals. Use a Gaussian moneyness
weight:

```text
w_i = exp(-0.5 * ((K_i - S) / sigma_K)^2)
sigma_K = 2 * strike_interval
```

where `K_i` is option strike and `S` is the contemporaneous index spot. If
strike interval cannot be resolved, use an explicitly configured NIFTY
fallback and expose that fallback in diagnostics. Do not mix expiries.

### 3.2 Call Volume Velocity and Put Volume Velocity

Let `V_i(t)` be cumulative VTT for option leg `i`. For horizon
`h ∈ {20,40,60}` seconds, calculate a nonnegative volume increment over the
horizon and convert it to feed-quantity units per second:

```text
DeltaV_i(h) = V_i(t) - V_i(t-h)
q_i(h) = max(DeltaV_i(h), 0) / h

CallVolumeVelocity_h = sum(w_i * q_i(h) for CE legs)
PutVolumeVelocity_h  = sum(w_i * q_i(h) for PE legs)
```

If a cumulative-volume reset is detected (`V_i(t) < V_i(previous)`), discard
that leg's affected window and reseed its history; do not emit a negative
velocity or a volume spike. A leg with no observation within the freshness
limit is excluded from that calculation, not treated as a fresh zero.

The raw summed values depend on the number of active strikes and feed volume
units. Also publish normalized comparisons:

```text
TotalOptionVolumeVelocity_h = CallVolumeVelocity_h + PutVolumeVelocity_h
CallPutVolumeImbalance_h =
    (CallVolumeVelocity_h - PutVolumeVelocity_h)
    / (CallVolumeVelocity_h + PutVolumeVelocity_h + epsilon)
```

`CallPutVolumeImbalance` is an activity-composition measure, **not** a
bull/bear signal. Do not label call-heavy volume bullish or put-heavy volume
bearish without quote-side classification and corroborating price response.

### 3.3 Volume-relative activity

Compare current activity to a time-of-day baseline. For each horizon and
side, maintain a baseline from prior comparable sessions, preferably matching
the same time bucket and expiry class:

```text
RelativeVolume_h =
    (TotalOptionVolumeVelocity_h + epsilon)
    / (MedianHistoricalTotalOptionVolumeVelocity_h + epsilon)
```

Use the median rather than the mean to reduce sensitivity to isolated spikes.
If there is not enough baseline history, report the ratio unavailable and
do not treat unavailable as 1.0 or as confirmation.

Initial interpretation thresholds:

| Relative activity | Interpretation |
|---:|---|
| <1.25 | Ordinary / no activity confirmation |
| ≥1.50 | Elevated participation |
| ≥2.00 | High activity |
| ≥3.00 | Candidate climax activity, only with price/flow evidence |

These are not absolute trade triggers. A volume surge without directional
flow or price response is not exhaustion by itself.

### 3.4 Signed aggressive flow

For a print/update with positive incremental volume `dv`, classify only where
valid best quotes exist:

```text
buy_aggressor_i = dv if LTP >= best_ask else 0
sell_aggressor_i = dv if LTP <= best_bid else 0
unclassified_i = dv otherwise
```

Do not double-classify when a crossed/locked quote would satisfy both
conditions; reject invalid quotes or classify the event as unknown.

For the NIFTY future, define:

```text
FuturesAggressorImbalance_h =
    (sum(buy_aggressor) - sum(sell_aggressor))
    / (sum(buy_aggressor) + sum(sell_aggressor) + epsilon)

ClassifiedCoverage_h =
    (sum(buy_aggressor) + sum(sell_aggressor))
    / (sum(buy_aggressor) + sum(sell_aggressor) + sum(unclassified) + epsilon)
```

For options, also preserve separate CE/PE buyer-initiated and
seller-initiated totals. An optional option-flow directional proxy is:

```text
OptionDirectionalFlow_h =
    CE_buy - CE_sell - PE_buy + PE_sell
    ------------------------------------
    CE_buy + CE_sell + PE_buy + PE_sell + epsilon
```

This convention treats aggressive CE buying and PE selling as bullish, and
aggressive PE buying and CE selling as bearish. It remains a weak proxy:
multi-leg trades, hedges, market-maker inventory, and quote changes can
reverse its interpretation. Keep futures signed flow as the primary
underlying-direction confirmation; use option-side activity as secondary
context.

Initial quality gates:

- At least **60% of the configured ATM-band weight** must have fresh option
  VTT observations for a side's volume velocity to be considered complete.
- Require at least **50% classified quote-side volume** for aggressor
  imbalance to be eligible as confirmation.
- Treat index/future inputs older than **3 seconds** and option inputs older
  than **5 seconds** as stale for the fast signal. Mark the feature unavailable
  when these limits are exceeded.
- Use a minimum valid option-leg count on both sides; do not let one
  illiquid contract dominate the chain aggregate.

## 4. Spot Velocity and Spot Acceleration

### 4.1 Input and sampling

Use canonical live NIFTY index `ltpc.ltp` ticks from `update_index_spot`.
Do not use the index close, OHLC candle fields, an option's `underlying_price`,
or a stale persisted spot as a current observation. Store `(event_time,
receive_monotonic_time, spot)` in a bounded 60-second history.

For timestamp irregularity, compute on timestamped observations or a
one-second resampled series with a strict maximum carry-forward age; do not
assume every tick is one second apart.

### 4.2 Velocity

Use log returns in basis points for scale independence:

```text
r_h_bp = 10,000 * ln(S_t / S_(t-h))
SpotVelocity_h = r_h_bp / h
```

Report `SpotVelocity_20`, `SpotVelocity_40`, and `SpotVelocity_60`, in
basis-points per second. Positive means NIFTY spot rising; negative means
falling.

### 4.3 Acceleration

Use the change between short- and medium-horizon velocities. The 20-second
window's midpoint is about 10 seconds newer than the 40-second window's
midpoint:

```text
SpotAcceleration_20_40 =
    (SpotVelocity_20 - SpotVelocity_40) / 10
```

Units are basis-points per second squared. Positive acceleration means price
movement is becoming more upward or less downward; negative acceleration
means movement is becoming more downward or less upward. This is a
multiscale estimate, not a tick-by-tick second derivative.

Optionally calculate `SpotAcceleration_40_60` similarly using the 40/60
velocity difference divided by approximately 10 seconds. Keep the 60-second
context separate from the faster exhaustion trigger.

### 4.4 Initial spot thresholds

Initial thresholds are point-of-reference values; validate against realized
volatility and time of day:

| Feature | Initial threshold |
|---|---:|
| Meaningful 20s spot move | `abs(r_20_bp) >= 3 bp` |
| Fast directional velocity | `abs(SpotVelocity_20) >= 0.15 bp/s` |
| Medium directional velocity | `abs(SpotVelocity_40) >= 0.10 bp/s` |
| Meaningful short/medium acceleration | `abs(SpotAcceleration_20_40) >= 0.01 bp/s²` |
| Stretched 60s move for exhaustion eligibility | `abs(r_60_bp) >= max(5 bp, 1.25 * RV_60_bp)` |

`RV_60_bp` should be realized volatility calculated from valid one-second
log-return observations over the prior 60 seconds. If the short-horizon
realized-volatility estimate is unstable, require the absolute 5 bp floor
alone and mark the volatility context unavailable. These thresholds should
ultimately be replaced or calibrated by time-of-day empirical distributions.

## 5. EXHAUSTION_ENGINE

### 5.1 Definition

- **Bull Exhaustion**: evidence that a sufficiently stretched **down move** is
  losing downside force, with opposing buying/flow or a price-flow divergence.
- **Bear Exhaustion**: evidence that a sufficiently stretched **up move** is
  losing upside force, with opposing selling/flow or a price-flow divergence.

Do not call every fast counter-move exhaustion. Require a pre-existing
directional move, deceleration, and at least one independent flow or
divergence confirmation.

### 5.2 Component definitions

Let:

```text
R60 = 60-second spot return in basis points
A = SpotAcceleration_20_40
F = FuturesAggressorImbalance_20
RelVol = RelativeVolume_20
```

Use `clamp(x,0,1) = min(1,max(0,x))`. Define stretch:

```text
Stretch = clamp(
    (abs(R60) - max(5, 1.25*RV_60_bp)) / max(5, RV_60_bp),
    0, 1
)
```

This is zero until the 60-second move exceeds the stretch eligibility level.

Define trend deceleration, only in the relevant trend:

```text
BullDeceleration = clamp(A / 0.02, 0, 1)     when R60 < 0
BearDeceleration = clamp(-A / 0.02, 0, 1)    when R60 > 0
```

Define opposing futures flow:

```text
BullOpposingFlow = clamp(F / 0.30, 0, 1)     when R60 < 0
BearOpposingFlow = clamp(-F / 0.30, 0, 1)    when R60 > 0
```

`0.30` is an initial meaningful imbalance level, and this feature is eligible
only when `ClassifiedCoverage_20 >= 0.50`.

Define volume climax from the same-time-of-day relative volume:

```text
VolumeClimax = clamp((RelVol - 1.5) / 1.5, 0, 1)
```

It ranges from zero at 1.5x baseline to one at 3.0x. It contributes only when
volume quality/coverage is valid; otherwise omit it and renormalize the
remaining valid component weights. It is not directional.

Define optional price-flow divergence over a common 20-second interval:

```text
BullDivergence =
    clamp(max(-R20_bp, 0) / 3, 0, 1)
    * clamp(max(F, 0) / 0.30, 0, 1)

BearDivergence =
    clamp(max(R20_bp, 0) / 3, 0, 1)
    * clamp(max(-F, 0) / 0.30, 0, 1)
```

This simple form captures price moving down while aggressive futures flow
turns net buying, or price moving up while flow turns net selling. A separate
swing-pivot divergence can be used as slower confirmation: lower price low
with higher flow low for bullish; higher price high with lower flow high for
bearish. Swing pivots must not be used as the earliest trigger because they
require confirmation and introduce lag.

### 5.3 Exhaustion scores

Initial component weights:

```text
BullExhaustion =
    100 * Stretch * (
        0.30 * BullDeceleration
      + 0.30 * BullOpposingFlow
      + 0.25 * BullDivergence
      + 0.15 * VolumeClimax
    )

BearExhaustion =
    100 * Stretch * (
        0.30 * BearDeceleration
      + 0.30 * BearOpposingFlow
      + 0.25 * BearDivergence
      + 0.15 * VolumeClimax
    )
```

Apply the following eligibility gates:

- Bull score is zero/not eligible unless `R60 < 0`; Bear score is zero/not
  eligible unless `R60 > 0`.
- Require stretch eligibility as defined above.
- Require either opposing-flow or price-flow-divergence confirmation. Volume
  climax alone cannot activate an exhaustion score.
- Require valid spot freshness and valid volume/flow coverage. If a component
  is unavailable, renormalize the valid component weights; if both flow and
  divergence are unavailable, return the score as unavailable/not ready rather
  than interpreting it as zero evidence.
- A score **≥65** is a high-exhaustion watch; **≥80** is a strong watch. These
  are alert thresholds, not orders.
- Require persistence on **two consecutive valid one-second evaluations** or
  equivalent distinct data updates before changing a watch to an alert.
- Cancel/decay the watch if the price resumes trend with acceleration beyond
  `0.02 bp/s²` in the trend direction, the flow confirmation reverses, data
  becomes stale, or the session/expiry changes.

The explicit trend/stretch precondition avoids scoring exhaustion in a flat
market. Flow confirmation and persistence reduce single-tick noise.

### 5.4 Optional published-sentiment confirmation

The existing published Bias/Momentum/Score/IV Skew/Gamma Boost can be read as
an **optional contextual confirmation** after the base scores are computed:

- For Bull Exhaustion, prefer improving 20/40-second directional Momentum or
  Gamma velocity and falling IV Skew, if those are already exposed by a
  separate reversal calculation.
- For Bear Exhaustion, prefer the inverse.
- Do not add these features as independent full-weight votes alongside
  Momentum/Gamma-derived signals: they share OI/COI sources and would
  double-count correlated evidence.
- Do not alter the six underlying values, their formulas, or the Score.

If these derivatives are not yet available, the engines work solely from
Upstox volume, quote-side flow, and spot price.

## 6. Architecture and data flow

```text
Upstox V3 FULL websocket callback (decoded feed updates)
  ├─ Option CE/PE: strike, expiry, vtt, ltp, best bid/ask, event time
  │      └─ VOLUME_FLOW_ENGINE
  │           ├─ per-contract cumulative-VTT history
  │           ├─ weighted CE/PE 20/40/60s volume velocities
  │           ├─ relative volume vs same-time-of-day baseline
  │           └─ option quote-side flow and data-quality coverage
  ├─ NIFTY future: vtt, ltp, best bid/ask, event time
  │      └─ VOLUME_FLOW_ENGINE: signed underlying aggressor flow
  └─ NIFTY index ltpc.ltp
         └─ spot history: returns, velocities, acceleration, realized volatility

VOLUME_FLOW_ENGINE snapshot + spot features
  └─ EXHAUSTION_ENGINE
       ├─ stretch and trend direction
       ├─ deceleration
       ├─ opposing flow
       ├─ price-flow divergence
       └─ Bull Exhaustion / Bear Exhaustion scores and watch state

Immutable engine snapshot
  └─ existing publish/state path (new fields only)
       └─ date-scoped Redis state -> API -> optional dashboard panel
```

Separate responsibilities: the Volume Flow engine owns raw VTT/quote
histories and flow quality; the Exhaustion engine consumes its snapshots and
owns score/watch state. Neither engine should import the feed worker or
calculate existing sentiment metrics.

## 7. Integration points

1. **Raw tick ingestion:** in the Upstox websocket `on_message` path in
   `build_streamer`, route decoded messages to the new volume-flow ingestion
   boundary before the existing two-minute CVD aggregation. A bounded
   thread-safe queue is preferable to doing per-contract history work in the
   websocket callback.
2. **Option metadata:** normalize each option tick using the already-known
   instrument metadata (`option_type`, `strike`, `expiry`). Apply active
   expiry and near-ATM filtering. Keep call and put volume histories separate.
3. **Spot history:** feed every valid canonical index `ltpc.ltp` in
   `update_index_spot` to the spot sampler. Do not wait for the throttled
   market-state publish to capture spot ticks.
4. **Future aggressor flow:** subscribe to/retain the selected NIFTY future
   tick with VTT and best bid/ask. Use it as the primary signed directional
   flow proxy. The index instrument itself has no quote-side volume.
5. **Calculation hook:** on each new flow/spot snapshot, evaluate the
   Exhaustion engine using synchronized event-time horizons. Alternatively,
   evaluate once per existing `_refresh_market_state` publish, but retain the
   higher-frequency raw histories and avoid pretending that the resulting
   cadence is raw-TBT. Do not insert any calculation into
   `_compute_live_sentiment_full`.
6. **State and publication:** add new, separate fields for the three-horizon
   call/put volume rates, spot velocity/acceleration, relative volume,
   coverage/freshness, and Bull/Bear Exhaustion outputs to market-state
   persistence and the API response. Include explicit availability/readiness
   flags; never map missing/stale inputs to neutral zeros without a status.
7. **Reset and concurrency:** reset per-instrument volume baselines on VTT
   rollback, active-expiry change, and trading-date rollover. Reset spot and
   confirmation histories on session/date change. Protect mutable state with
   one serialized worker or locks; do not assume the asyncio state lock covers
   worker-thread updates.
8. **Dashboard:** display the outputs in a separate panel with a data-age and
   coverage indicator. Label scores as exhaustion watches, not buy/sell
   commands.

## 8. Required validation before live use

- Replay representative normal, trend, reversal, opening, lunch, and expiry-day
  sessions. Compare 20/40/60-second outputs to raw event-time samples.
- Verify VTT increments across duplicate updates, out-of-order events,
  reconnects, counter rollbacks, expiry changes, and sessions.
- Measure quote classification coverage; investigate feed periods where bid,
  ask, or volume is absent.
- Calibrate baselines by time-of-day and expiry regime; evaluate false
  exhaustion alerts, missed reversals, alert delay, and post-alert excursion.
- Include realistic option spread, slippage, brokerage, and execution delay.
- Keep thresholds and scores in shadow mode until they are stable out of
  sample. A high exhaustion score is a risk/attention signal, not proof of
  reversal or a standalone entry.
