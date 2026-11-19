# Live Market Metrics: Technical Audit

## Scope and evidence

This report traces the six Math Console values on the live-signals page: **Bias,
Momentum, Delta Wt, Score, IV Skew, and Gamma Boost**. It follows the active
Upstox-to-dashboard path, rather than similarly named metrics in unrelated
backtests or the other GAMMA dashboard.

The authoritative calculations are in
[`upstox_live_feed.py`](Nse_Project/Live_dashboard/upstox_live_feed.py#L1739).
The dashboard explicitly treats `bias`, `momentum`, and `score` as backend
values; it renders `delta_wt_impact`, `iv_skew_est`, and `gamma_boost_val` from
that same payload. It does not recalculate these formulas in JavaScript.

The active chain is a full-mode Upstox V3 websocket subscription created by
`build_streamer`; decoded messages are routed through
`update_live_state_from_message`. The separately present `tbt.py` and
`TbT_Collector` collectors are not called from this dashboard calculation path.
They should not be conflated with the live-signals feed worker.

### Principal source files and symbols

| Responsibility | Source |
|---|---|
| Upstox V3 subscription, tick extraction, OI/COI state, sentiment formulas, Greek blend, publish cadence | [`Nse_Project/Live_dashboard/upstox_live_feed.py`](Nse_Project/Live_dashboard/upstox_live_feed.py): `extract_live_values`, `update_instrument_state`, `update_index_spot`, `update_live_state_from_message`, `_compute_live_sentiment_full`, `_real_net_gex`, `_refresh_market_state`, `build_streamer` |
| Black–Scholes Greeks, option-chain normalization, day GEX/Vanna/Charm aggregates, gamma-flip search | [`Nse_Project/GAMMA/gamma_blast_engine.py`](Nse_Project/GAMMA/gamma_blast_engine.py): `OptionStrike`, `GammaMetrics`, `GammaProfile`, `GammaProcessor.process_payload`, `GammaProcessor._to_strike`, `GammaProcessor._repair_iv_surface`, `GammaProcessor._build_metrics`, `GammaProcessor._metrics_for`, `GammaProcessor._find_gamma_flip`, `bs_greeks`, `normalize_live_payload` |
| Date-scoped market-state persistence | [`Nse_Project/Live_dashboard/market_state.py`](Nse_Project/Live_dashboard/market_state.py): `load_market_state`, `save_market_state`, `normalize_market_state` |
| API payload and Redis-backed metric read | [`Nse_Project/Live_dashboard/views.py`](Nse_Project/Live_dashboard/views.py): `_read_market_state_for_today`, `live_option_chain_api`, `_build_live_option_chain_payload` |
| Display, rounding, peaks, bars, 1-second polling | [`Nse_Project/Master_Temp/live_signals.html`](Nse_Project/Master_Temp/live_signals.html): `refreshOptions`, `updateSentiment`, `renderAdvancedMetrics`, `renderMathHwm`, `bumpPeak`, `renderBar` |

There is no metric-calculation class in the feed module: its calculations are
module-level functions. `GammaProcessor` and the Greek data classes are the
calculation classes in the gamma engine.

## Raw data and execution path

### Feed fields and conversion

1. `build_streamer` subscribes to the active NIFTY option instruments, the
   NIFTY index instrument, and (where configured) the NIFTY future in
   Upstox V3 `FULL` mode.
2. The websocket callback sends decoded feed messages to
   `update_live_state_from_message`. `extract_live_values` recursively extracts
   exact fields including `ltp`, `cp`, `oi`, optional `poi`, `iv`, `vtt`, and
   `atp`.
3. Option `oi` is converted from Upstox contracts/lots to the internal
   share-based scale by multiplying by `LOT_SIZE` (default 65, configurable
   with `GAMMA_LOT_SIZE`). The code computes:

   ```text
   COI_i = live_OI_i - previous_OI_i
   COI_pct_i = 100 * COI_i / previous_OI_i
   ```

   Previous OI is taken from a tick `poi` if present; otherwise it is sourced
   from `market_data.prev_oi` in the Upstox option-chain REST response, scaled
   by the same lot size. The refresh interval defaults to 900 seconds. A
   previously held valid POI can be retained when a refresh is unavailable.
   Missing/zero POI makes that leg's COI `None`, which contributes as zero to
   sentiment aggregation; the implementation does not invent a POI.
4. Upstox V3 IV is treated as a fraction and multiplied by 100 before being
   stored, so `0.1928` becomes `19.28`. The gamma engine converts percent IV
   back to a volatility fraction with `/100`.
5. Index spot is read from the index tick's live `ltpc.ltp` path. It does not
   use the index close (`cp`) or a candle's OHLC value. The feed snapshots the
   active expiry and filters out instruments provably belonging to another
   expiry before sentiment and Greek processing.

The six values do **not** use option LTP, `cp`, volume/`vtt`, future `atp`, or
Upstox-provided option Greeks directly. Volume and `atp` feed other parts of the
application. For Gamma Boost, Greeks are recalculated locally by the
Black–Scholes implementation.

### Publication and display path

`update_instrument_state` and `update_index_spot` invoke the publish path after
ticks. `REDIS_PUBLISH_MIN_INTERVAL` defaults to 1.0 second, so the ordinary tick path
is throttled to no more than one calculation/publish per second, not one per
raw websocket event. While the feed is live, a separate 5-second loop can
force-publish the current snapshot if ticks go quiet (it does not normally
increase the rate above the 1-second tick-path floor). `_refresh_market_state`
calculates the metrics from the published active-chain snapshot;
`_persist_market_state` saves the state under the IST-date Redis key with a
24-hour TTL. The API in `views.py` returns these values; the page polls that
API every 1 second and renders the values.

The feed also passes ticks to candle/CVD engines, but those engines' candles
are **not inputs to these six formulas**. No OHLC candle close is used by this
metric set.

## 1. Bias

**Meaning in this implementation:** the put-versus-call open-interest
imbalance, expressed as a percentage, with a Gaussian distance-to-spot weight.
This is not option delta weighting despite the nearby “Distance/Delta
weighting” description.

**Files, classes, functions:** `upstox_live_feed.py`:
`_compute_live_sentiment_full`, `update_instrument_state`; gamma-engine classes
are not involved in Bias. `OptionStrike` is not used to calculate it.

### Step-by-step

**STEP 1 — Raw inputs**

- Each active-expiry option leg's `option_type` (`CE`/`PE`), live `oi`, and
  `strike`.
- Canonical current index `spot_price`, taken from the live index `ltpc.ltp`.
- `oi` is converted to the internal share scale. POI and COI are not used in
  Bias.

**STEP 2 — Intermediate calculations**

For each leg `i`, with spot `S`, strike `K_i`, and OI `OI_i`:

```text
sigma = max(0.01 * S, 1.0)
w_i = exp(-0.5 * ((K_i - S) / sigma)^2)
```

Accumulate raw and weighted sides:

```text
call_OI       = sum(OI_i for CE legs)
put_OI        = sum(OI_i for PE legs)
weighted_call = sum(OI_i * w_i for CE legs)
weighted_put  = sum(OI_i * w_i for PE legs)
total_OI      = call_OI + put_OI
```

**STEP 3 — Transformation**

```text
unweighted_bias = 100 * (put_OI - call_OI) / total_OI
weighted_bias   = 100 * (weighted_put - weighted_call)
                  / (weighted_put + weighted_call)
```

If spot/strike data are absent, or the weighted denominator is not positive,
`weighted_bias` falls back to `unweighted_bias`. When one or both OI sides are
missing/empty (`total_OI <= 0`, `call_OI <= 0`, or `put_OI <= 0`), the overall
sentiment routine returns its neutral fallback rather than a partial Bias.

**STEP 4 — Normalization, windows, limits**

- Multiplied by 100; no moving average, rolling window, rate-of-change, or
  time smoothing is applied.
- No backend min/max clamp is applied to Bias. The percentage is not
  mathematically guaranteed to stay within ±100 if feed OI/weight inputs are
  anomalous, though ordinary nonnegative OI makes the weighted imbalance
  bounded by ±100.
- The Math Console bar clamps its *visual position* to `[-20,+20]`; the
  displayed number remains the backend number and is formatted to one decimal
  place with `%`.

**STEP 5 — Final displayed value**

`market_state["bias"] = weighted_bias`; API `bias`; Math Console text and bar.
The frontend bar is bipolar around zero and clips only the bar fill/peak marker
to its `[-20,+20]` scale.

**Exact formula in plain English:** subtract total call OI from total put OI
after weighting each option leg by a Gaussian of its distance from spot,
divide by total weighted OI, and multiply by 100.

**Pseudocode**

```text
sigma = max(spot * 0.01, 1)
for each option leg:
    w = exp(-0.5 * ((strike - spot) / sigma)^2)
    if side == CE: weighted_call += oi * w
    if side == PE: weighted_put  += oi * w
bias = 100 * (weighted_put - weighted_call) / (weighted_put + weighted_call)
```

## 2. Momentum

**Meaning in this implementation:** directional change-in-open-interest
(COI), normalized by total live OI, plus a bounded first-difference boost. The
sign convention is `put_COI - call_COI`; thus call writing/adding COI pushes
Momentum negative, and put adding COI pushes it positive.

**Files, classes, functions:** `upstox_live_feed.py`:
`update_instrument_state`, `_compute_live_sentiment_full`,
`_previous_directional_coi`; no candle or gamma-engine class is used directly.

### Step-by-step

**STEP 1 — Raw inputs**

- Per active option leg: live `oi` and previous-day `poi` (option-chain REST
  baseline unless a tick has POI).
- The previous directional-COI aggregate stored in the process-global
  `_previous_directional_coi`.
- Total live call and put OI for the denominator.

**STEP 2 — Intermediate calculations**

For each leg:

```text
COI_i = live_OI_i - previous_OI_i
call_COI = sum(COI_i for CE legs)
put_COI  = sum(COI_i for PE legs)
directional_COI_t = put_COI - call_COI
```

The previous directional COI is updated on each sentiment computation:

```text
acceleration_t = directional_COI_t - directional_COI_(previous computation)
```

For the first computation after process start, `acceleration = 0`.

**STEP 3 — Transformation**

```text
base_momentum = 100 * directional_COI_t / total_OI
accel_boost   = clamp(100 * acceleration_t / total_OI * 0.5, -15, +15)
momentum      = base_momentum + accel_boost
```

Here `clamp(x, lo, hi) = max(lo, min(hi, x))`.

**STEP 4 — Normalization, windows, limits**

- There is no rolling COI window or moving average in this metric.
- The acceleration term is a **first difference**, not a true time-normalized
  derivative: the code does not divide by elapsed seconds. Since the previous
  sample is the previous metric computation, the value depends on the
  effective publish interval.
- The acceleration *boost only* is clipped to `[-15,+15]`; base Momentum and
  final Momentum are not clipped.
- There is no second derivative or acceleration-of-acceleration.
- Empty/partially initialized OI chains trigger the common neutral fallback
  (`momentum=0`, `acceleration` still reports the computed difference).
- The displayed number is one decimal and suffixed `%`; the bar alone clips to
  `[-30,+30]`. The UI shows “EXPLOSIVE!” when `abs(momentum) >= 30`.

**STEP 5 — Final displayed value**

`market_state["momentum"] = base_momentum + accel_boost`; the backend also
publishes `acceleration` separately. The API's `momentum` is displayed in the
Math Console and its bar.

**Exact formula in plain English:** divide net directional COI (puts added
minus calls added) by total live OI and multiply by 100; add half of the same
percentage normalization of the one-sample COI change, limited to ±15 points.

**Pseudocode**

```text
directional_coi = sum(put_coi) - sum(call_coi)
acceleration = directional_coi - previous_directional_coi
base = 100 * directional_coi / total_live_oi
boost = clamp(100 * acceleration / total_live_oi * 0.5, -15, +15)
momentum = base + boost
previous_directional_coi = directional_coi
```

## 3. Delta Wt

**Meaning in this implementation:** the change in the Bias contribution to
the Score input caused by distance-weighting OI. It is not option delta,
delta exposure, or a separately measured Greek.

**Files, classes, functions:** `upstox_live_feed.py`:
`_compute_live_sentiment_full`; UI rendering in `live_signals.html` via
`renderAdvancedMetrics` and `renderMathHwm`. No class is used.

### Step-by-step

**STEP 1 — Raw inputs**

- Same live `oi`, option side, strike, and index spot as Bias.
- Both the unweighted and Gaussian-weighted Bias values from the same
  calculation pass.

**STEP 2 — Intermediate calculations**

```text
unweighted_bias = 100 * (put_OI - call_OI) / total_OI
weighted_bias   = 100 * (weighted_put_OI - weighted_call_OI)
                  / (weighted_put_OI + weighted_call_OI)
```

**STEP 3 — Transformation**

```text
delta_wt_impact = 0.5 * (weighted_bias - unweighted_bias)
```

**STEP 4 — Normalization, windows, limits**

- The factor `0.5` is the Score's Bias weight. The resulting number is in
  Score-point units, despite the UI's bipolar number/bar presentation.
- No smoothing, rolling window, backend clamp, or additional normalization.
- The Math Console bar clips visually to `[-20,+20]`; text is rounded to one
  decimal. The peak marker has the same display-range clipping.

**STEP 5 — Final displayed value**

`market_state["delta_wt_impact"]`; API `delta_wt_impact`; Math Console “Delta
Wt” value. This diagnostic is not separately added to Score. Rather, the
weighted Bias used by Score already reflects this adjustment.

**Exact formula in plain English:** take weighted Bias minus unweighted Bias,
then multiply the difference by one half.

**Pseudocode**

```text
delta_wt_impact = (weighted_bias - unweighted_bias) * 0.5
```

## 4. Score

**Meaning in this implementation:** a logistic transform of a composite
directional input. The Score is not a weighted average of all six displayed
metrics.

**Files, classes, functions:** `upstox_live_feed.py`:
`_compute_live_sentiment_full`, `_real_net_gex`; gamma dependency in
`gamma_blast_engine.py` (`GammaProcessor` and `bs_greeks`).

### Step-by-step

**STEP 1 — Raw inputs**

- `weighted_bias` (live OI and distance to live spot).
- `momentum` (live COI versus prior-day OI, plus first-difference boost).
- `gamma_boost_val` (day COI-based Black–Scholes GEX/Vanna/Charm blend).
- IV, spot, strike, and active expiry are indirect inputs through Gamma Boost.
- IV Skew and Delta Wt are **not independently** added to Score.

**STEP 2 — Intermediate calculations**

```text
raw_delta = 0.5 * weighted_bias + momentum + gamma_boost_val
```

Since `delta_wt_impact = 0.5 * (weighted_bias - unweighted_bias)`, an
algebraically equivalent decomposition is:

```text
raw_delta = 0.5 * unweighted_bias + delta_wt_impact
            + momentum + gamma_boost_val
```

This identity does not mean the code adds Delta Wt as an independent signal;
the actual code uses `weighted_bias * 0.5`.

**STEP 3 — Transformation**

```text
score = 100 / (1 + exp(-0.025 * raw_delta))
```

**STEP 4 — Normalization, windows, limits**

- Logistic normalization maps finite inputs into the open interval `(0,100)`;
  `raw_delta=0` gives exactly 50. There is no explicit backend clamp.
- The frontend rounds the value to an integer for text and clamps the bar to
  `[0,100]` for layout.
- The displayed Score's high-water mark is measured from neutral 50 and
  decays toward 50 with a 300-second half-life in `_refresh_market_state`.
  This peak decay does not smooth or alter the current Score.
- Missing/empty/one-sided OI invokes the neutral fallback `score=50`.

**STEP 5 — Final displayed value**

`market_state["score"]`; API `score`; rendered as `round(score)` and a
0–100 bar. A distinct decision layer elsewhere uses thresholds (for example
`score > 55`, `<45`) to select trade direction; those thresholds do not alter
the Score formula.

**Exact formula in plain English:** add half the weighted Bias, Momentum, and
Gamma Boost; multiply that sum by 0.025, apply the logistic sigmoid, and
multiply by 100.

**Pseudocode**

```text
raw_delta = 0.5 * weighted_bias + momentum + gamma_boost
score = 100 / (1 + exp(-0.025 * raw_delta))
```

## 5. IV Skew

**Meaning in this implementation:** the difference between the unweighted
arithmetic mean of positive put IVs and the unweighted arithmetic mean of
positive call IVs across the active chain. This is a chain-wide mean
difference, not an ATM-only skew curve or regression.

**Files, classes, functions:** `upstox_live_feed.py`:
`extract_live_values`, `update_instrument_state`,
`_compute_live_sentiment_full`; no class is used.

### Step-by-step

**STEP 1 — Raw inputs**

- Latest `iv` on every CE/PE leg, from the Upstox V3 option tick.
- Values are stored in percentage units after multiplying the wire fraction
  by 100. Only values greater than zero are included.

**STEP 2 — Intermediate calculations**

```text
call_iv_mean = sum(valid call IVs) / number of valid call IVs
put_iv_mean  = sum(valid put IVs) / number of valid put IVs
```

**STEP 3 — Transformation**

```text
iv_skew_est = put_iv_mean - call_iv_mean
```

**STEP 4 — Normalization, windows, limits**

- No strike-distance weighting, OI weighting, smoothing, rolling window, or
  backend clipping.
- This is a difference in **IV percentage points** (for example, 20.0 minus
  18.8 equals 1.2 points). The frontend appends `%`, which can be read as a
  percent sign but is not a relative percent change.
- It is set to zero if either side has no valid IVs or the shared sentiment
  routine returns its neutral fallback.
- The UI bar clips visually to `[-10,+10]`; text is rounded to one decimal.

**STEP 5 — Final displayed value**

`market_state["iv_skew_est"]`; API `iv_skew_est`; Math Console “IV Skew”
readout.

**Exact formula in plain English:** average all valid put IVs, average all
valid call IVs, then subtract the call mean from the put mean.

**Pseudocode**

```text
call_ivs = [iv for CE legs if iv > 0]
put_ivs  = [iv for PE legs if iv > 0]
iv_skew = mean(put_ivs) - mean(call_ivs) if both lists nonempty else 0
```

## 6. Gamma Boost

**Meaning in this implementation:** a score-point blend of normalized
day-change GEX, Vanna, and Charm, amplified near the day-gamma flip, then
clipped to ±25. It is not simply a gamma value or gamma/OI ratio.

**Files, classes, functions:** feed-side `upstox_live_feed.py`:
`_real_net_gex`, `_compute_live_sentiment_full`; engine-side
`gamma_blast_engine.py`: `normalize_live_payload`, `GammaProcessor`,
`GammaProcessor.process_payload`, `_to_strike`, `_repair_iv_surface`,
`_build_metrics`, `_metrics_for`, `_find_gamma_flip`, `bs_greeks`.
Data types: `OptionStrike`, `GammaMetrics`, `GammaProfile`.

### STEP 1 — Raw inputs

- Per active-expiry option contract: strike, side, live OI, COI, and IV.
- Live NIFTY spot and active expiry date.
- COI is derived from live OI minus previous-day OI. Greek exposures use
  **day COI**, not the weekly/open-interest exposure.
- Feed OI/COI are in shares. The gamma engine's configured input scale is
  `shares`, so its `resolve_oi_divisor` divides by lot size (default 65) to
  return to per-contract quantities. The divisor is common to the net/gross
  ratios and cancels in their scale.
- Upstox optionGreeks are not used for this blend; the engine derives its own
  Black–Scholes Greeks.

### STEP 2 — Per-contract Black–Scholes intermediates

For each option leg, with spot `S`, strike `K`, time-to-expiry `T` in years,
annual risk-free rate `r` (default 0.065), and volatility `σ = IV_percent/100`:

```text
u = σ * sqrt(T)
d1 = (ln(S/K) + (r + 0.5*σ^2)*T) / u
d2 = d1 - u
φ(x) = exp(-0.5*x^2) / sqrt(2π)
N(x) = standard normal cumulative distribution function

gamma = φ(d1) / (S * u)
charm_core = φ(d1) * (2*r*T - d2*u) / (2*T*u) / 365
call_charm = -charm_core
put_charm = charm_core - r*exp(-r*T)*N(-d2)/365
vanna = (-φ(d1)*d2/σ) / 100
```

The expiry is the expiry date's 15:30 local market-close time. The engine
uses the remaining fraction of a 365-day year, floored at one hour. Invalid or
missing IVs are repaired per side with the mean of valid IVs on that side in
the chain; if that side has no valid IVs, the fallback is 15%.

Per strike, the engine forms signed day exposures:

```text
day_call_gex = -call_COI * gamma * spot
day_put_gex  =  put_COI * gamma * spot
day_net_gex  = day_call_gex + day_put_gex

day_net_charm = -call_COI * call_charm + put_COI * put_charm
day_net_vanna = -call_COI * call_vanna + put_COI * put_vanna
```

The signs are intentionally asymmetric: calls are negated; puts are positive
in these engine aggregations.

### STEP 3 — Chain-level normalization and flip

Across strikes with both call and put COI present:

```text
net_gex = sum(day_net_gex)
gross_gex = sum(abs(day_call_gex) + abs(day_put_gex))
gex_ratio = net_gex / gross_gex, or 0 if gross_gex == 0
```

For the engine's day-Greek aggregation, a strike is included only when its
normalized row has both `call_coi` and `put_coi` non-`None`; a partially
initialized strike is excluded from day GEX/Vanna/Charm and the day flip.
This is stricter than the direct Momentum aggregation, which treats a missing
individual COI as zero.

Similarly:

```text
net_vanna = sum(day_net_vanna)
gross_vanna = sum(abs(call_COI*call_vanna) + abs(put_COI*put_vanna))
vanna_ratio = net_vanna / gross_vanna, or 0 if gross_vanna == 0

net_charm = sum(day_net_charm)
gross_charm = sum(abs(call_COI*call_charm) + abs(put_COI*put_charm))
charm_ratio = net_charm / gross_charm, or 0 if gross_charm == 0
```

The gamma engine's day-gamma flip is found by sorting strikes, accumulating
day net GEX from the bottom of the supplied chain, and examining zero-crossings
whose strike is within `spot ± 500` points (default `gamma_flip_band`). The
cumulative sum is seeded with strikes below the lower edge. At a crossing,
the flip price is linearly interpolated between adjacent strikes. If several
crossings exist, the largest absolute GEX swing wins; equal swings are broken
by choosing the crossing nearer spot.

The feed bounds the ratios before blending:

```text
gex_ratio   = clamp(gex_ratio,   -1, +1)
vanna_ratio = clamp(vanna_ratio, -1, +1)
charm_ratio = clamp(charm_ratio, -1, +1)
```

The engine rounds `day_gamma.net_gex_ratio` to four decimals before the feed
uses it; Vanna and Charm ratios are divided directly then clamped.

### STEP 4 — Blend, flip multiplier, clipping

```text
base_boost = 10*gex_ratio + 5*vanna_ratio + 5*charm_ratio

flip_distance_pct = 100 * abs(spot - gamma_flip_level) / spot
flip_multiplier = 1.5 if a day flip exists and flip_distance_pct < 0.2
                  else 1.0

gamma_boost = clamp(base_boost * flip_multiplier, -25, +25)
```

The blend's nominal unamplified range is `[-20,+20]`; near the flip it can
reach ±30 before the final ±25 cap. The zone comparison is strict (`< 0.2%`),
not inclusive. No moving average or rolling window is applied to the boost.

**STEP 5 — Final displayed value**

`market_state["gamma_boost_val"]`; API `gamma_boost_val`; Math Console
“Gamma Boost” readout. The value is rounded to four decimals in the feed and
to one decimal for UI text. The display bar visually clips at `[-20,+20]`,
which is narrower than the backend cap of ±25.

**Exact formula in plain English:** add ten times normalized day Net GEX, five
times normalized day Vanna, and five times normalized day Charm; multiply by
1.5 only when spot is strictly within 0.2% of the day gamma-flip level; clamp
the result to ±25.

**Pseudocode**

```text
base = 10 * clamp(net_gex / gross_gex, -1, 1)
     +  5 * clamp(net_vanna / gross_vanna, -1, 1)
     +  5 * clamp(net_charm / gross_charm, -1, 1)
multiplier = 1.5 if flip_exists and abs(spot - flip) / spot * 100 < 0.2 else 1
gamma_boost = clamp(base * multiplier, -25, 25)
```

## Metric dependencies and formula components

```text
Upstox V3 option ticks
  ├─ OI + side + strike ───────────────────────────┐
  │      ├─ spot-distance Gaussian weights ──> Bias ├─> Score
  │      └─ live OI - previous-day OI = COI         │
  │              ├─ put COI - call COI ──> Momentum ┤
  │              │      └─ one-sample difference ──┘
  │              └─ Black–Scholes day exposures ──> Gamma Boost ──┘
  ├─ IV fraction * 100 ──> equal-side means ──> IV Skew
  └─ strike/side/OI + spot ──> weighted vs raw Bias ──> Delta Wt

Index tick ltpc.ltp ──> spot ──> Bias weights and Black–Scholes / flip distance
Option-chain REST prev_oi ──> POI ──> COI ──> Momentum and Gamma Boost

Feed market state ──> date-keyed Redis state ──> live option-chain API
  ──> 1-second browser poll ──> Math Console values and visual bars
```

Dependency notes:

- **Bias** depends on live OI, side, strike, and spot.
- **Momentum** depends on live OI, previous-day OI/COI, total OI, and the
  previous computation's directional COI.
- **Delta Wt** depends on Bias's weighted and unweighted forms.
- **Gamma Boost** depends on COI, IV, spot, expiry, and the locally calculated
  Black–Scholes Greeks and day gamma flip.
- **Score** depends on Bias, Momentum, and Gamma Boost.
- **IV Skew** is calculated alongside them but is not an input to Score or
  Gamma Boost.
- **CVD, OHLC candles, candle ATR, price ROC, and option volume velocity are
  not inputs** to these six metric formulas.

## Temporal classification and update cadence

The code does not formally label any of the six as “leading” or “lagging.”
Those labels below describe input behavior, not a guarantee of predictive
power:

| Metric | Practical characterization | Reason |
|---|---|---|
| Bias | Positioning/structure; not a price-lag indicator | Snapshot of current chain OI distribution, distance-weighted around spot |
| Momentum | Fast, leading-like flow measure | COI versus prior-day OI plus a one-computation first difference |
| Delta Wt | Neither temporal leading nor lagging | Spatial reweighting diagnostic of Bias; no time-series input |
| Score | Composite/confirmation measure; mixed | Transforms Bias, COI Momentum, and Gamma Boost; no price-candle history |
| IV Skew | Forward-looking market-implied sentiment proxy | Current option IV means; not an observed price-return indicator |
| Gamma Boost | Positioning/Greek-structure measure; mixed | Current COI exposures and Black–Scholes Greeks, with a gamma-flip-zone multiplier |

| Metric | Raw-update trigger | Calculation cadence | Candle-based? |
|---|---|---|---|
| Bias | Option OI/strike or index spot tick changes state | Throttled tick publish (default 1-second floor); live 5-second loop can force a quiet-feed refresh | No |
| Momentum | Option OI/POI changes; previous sampled state | Throttled tick publish (default 1-second floor); live 5-second loop can force a quiet-feed refresh | No |
| Delta Wt | OI/spot/strike changes used by Bias | Throttled tick publish (default 1-second floor); live 5-second loop can force a quiet-feed refresh | No |
| Score | Any input to Bias, Momentum, or Gamma Boost changes | Throttled tick publish (default 1-second floor); live 5-second loop can force a quiet-feed refresh | No |
| IV Skew | Option IV tick changes | Throttled tick publish (default 1-second floor); live 5-second loop can force a quiet-feed refresh | No |
| Gamma Boost | Option COI/IV, spot, or active expiry changes | Throttled tick publish (default 1-second floor); live 5-second loop can force a quiet-feed refresh; gamma engine recomputes chain | No |

The browser polls the API every second independently. If the feed's
`REDIS_PUBLISH_MIN_INTERVAL` is configured above that, the same backend value
can be returned on consecutive polls.

## Derivatives, ROC, slope, acceleration, smoothing

| Metric | Velocity / ROC / derivative / slope present? | Implementation detail |
|---|---|---|
| Bias | No | Current weighted OI ratio only |
| Momentum | First difference only | `directional_COI_now - directional_COI_previous`; no elapsed-time division, so not a conventional per-second velocity or ROC |
| Delta Wt | No | Difference between two same-sample Bias calculations, not a temporal derivative |
| Score | No | Static sigmoid of current composite inputs |
| IV Skew | No | Same-sample difference of two IV means |
| Gamma Boost | No | Same-sample Greek exposure ratios and flip-zone condition |

No metric computes a second derivative, price slope, trend acceleration from
candles, or a rolling moving average. The only formula-level derivative-like
term is Momentum's first difference. Peak/HWM decay is presentation/history
tracking and does not smooth the six live calculation outputs:

- Backend Bias and Momentum peaks retain signed session extremes.
- Backend Score peak approaches neutral 50 with a 5-minute half-life.
- The page's `bumpPeak` applies a 5-minute decay toward each metric's neutral
  center for displayed peaks; advanced metric peak caches are browser-local.
- The Math Console progress bars clamp to their configured display ranges; this
  is not calculation clipping.

## Final comparison table

| Metric | Formula | Data Source | Window | Leading/Lagging | Update Frequency |
|---|---|---|---|---|---|
| Bias | `100*(weighted_put_OI-weighted_call_OI)/(weighted_put_OI+weighted_call_OI)` | Active-expiry Upstox OI + side/strike; index spot LTP | Current chain snapshot; Gaussian `sigma=max(1% spot,1)` | Structural/positioning, not classic lagging | Published refresh, default ≥1 s interval |
| Momentum | `100*(put_COI-call_COI)/total_OI + clamp(50*Δ(put_COI-call_COI)/total_OI,-15,15)` | Live OI and previous-day POI; previous computed COI | COI since prior-day OI plus one previous computation | Fast/leading-like flow indicator | Published refresh, default ≥1 s interval |
| Delta Wt | `0.5*(weighted_bias-unweighted_bias)` | Same OI/spot inputs as Bias | Current snapshot only | Neither; spatial adjustment | Published refresh, default ≥1 s interval |
| Score | `100/(1+exp(-0.025*(0.5*weighted_bias+momentum+gamma_boost)))` | Bias + Momentum + Gamma Boost | Current snapshot only | Composite/mixed | Published refresh, default ≥1 s interval |
| IV Skew | `mean(valid_put_IV)-mean(valid_call_IV)` | Upstox option IV fields, fraction converted to percent | Current valid IVs across active chain; no rolling window | Forward-looking proxy, not a price-trend lag | Published refresh, default ≥1 s interval |
| Gamma Boost | `clamp((10*gex_ratio+5*vanna_ratio+5*charm_ratio)*flip_multiplier,-25,25)` | COI + option IV + spot + expiry; locally calculated Black–Scholes day Greeks | Current expiry chain; day flip searches ±500 spot points | Mixed structural/flow measure | Published refresh, default ≥1 s interval |

## Neutral and edge behavior

- The sentiment function returns `bias=0`, `momentum=0`, `score=50`,
  `delta_wt_impact=0`, `gamma_boost_val=0`, and `iv_skew_est=0` if total OI is
  nonpositive or either call or put OI is nonpositive. This is an explicit
  common neutral fallback, not an inference from the UI.
- If previous OI is missing, the feed leaves COI unset; the sentiment
  aggregation reads non-numeric COI as zero. Momentum and day Greek exposures
  can therefore be understated until POI is populated.
- Gamma engine failures, missing spot/expiry, or an empty/invalid normalized
  chain return neutral Greek ratios and no flip; the Gamma Boost then resolves
  to zero from those inputs.
- The Math Console's static template text (example values) is replaced by the
  first successful API poll. Those template defaults are not formula outputs.
