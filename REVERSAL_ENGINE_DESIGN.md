# `reversal_engine.py` Design

## Status and scope

This is a design for a new, downstream-only calculation module at
`Nse_Project/Live_dashboard/reversal_engine.py`. It does not change or replace
the current Bias, Momentum, Delta Wt, Score, IV Skew, or Gamma Boost
calculations. It consumes the five already-published values:

```text
bias
momentum
score
iv_skew_est
gamma_boost_val
```

The module derives eight time-series metrics and two reversal candidate scores.
It should be independent of Django, Redis, Upstox clients, and
`upstox_live_feed.py` to avoid dependency cycles. Its only state is a bounded
in-memory sample history plus small signal-confirmation state.

## Existing implementation and integration context

The live feed computes current sentiment in
[`upstox_live_feed.py`](Nse_Project/Live_dashboard/upstox_live_feed.py#L1739).
`_refresh_market_state` copies the resulting current values into the
module-level `market_state` dictionary, then `_publish_and_persist_sync`
persists that dictionary. The regular tick publish is throttled by
`REDIS_PUBLISH_MIN_INTERVAL` (default 1 second); when the feed is live, a
5-second loop can force a refresh if ticks go quiet. The dashboard API reads
the date-scoped state from Redis and the page polls the API every second.

The existing feed's `market_state["acceleration"]` is **not** the proposed
Momentum Acceleration. It is the one-sample difference in directional COI
inside the current Momentum formula. The new module must not read that field,
modify it, or feed its outputs back into the existing sentiment calculation.
The new Bias/Momentum/Gamma/IV accelerations below mean second derivatives of
the corresponding *published value series*.

The existing Redis-state schema is normalized by
[`market_state.py`](Nse_Project/Live_dashboard/market_state.py); new fields
must be added to `DEFAULT_MARKET_STATE`, or `normalize_market_state()` will
discard them during persistence. The JSON endpoint fields are assembled in
[`views.py`](Nse_Project/Live_dashboard/views.py#L1598), and the Math Console
polls that endpoint from
[`live_signals.html`](Nse_Project/Master_Temp/live_signals.html#L2702).

## Output contract

All velocity and acceleration values are signed floats. Positive Bias,
Momentum, and Gamma velocity/acceleration are bullish in the current
implementation's sign convention. Positive IV Skew is `mean(put IV) -
mean(call IV)`; therefore a **negative** IV Skew velocity/acceleration is
bullish (put-call skew falling), and a positive value is bearish.

| Output | Definition | Units |
|---|---|---|
| `bias_velocity` | First time derivative of published Bias | Bias points/second |
| `momentum_velocity` | First time derivative of published Momentum | Momentum points/second |
| `gamma_velocity` | First time derivative of published Gamma Boost | Gamma-boost points/second |
| `iv_skew_velocity` | First time derivative of published IV Skew | IV percentage-points/second |
| `bias_acceleration` | Second time derivative of published Bias | Bias points/second² |
| `momentum_acceleration` | Second time derivative of published Momentum | Momentum points/second² |
| `gamma_acceleration` | Second time derivative of published Gamma Boost | Gamma-boost points/second² |
| `iv_acceleration` | Second time derivative of published IV Skew | IV percentage-points/second² |
| `early_bull_score` | Bull-directed reversal composite, gated by Score | 0–100 |
| `early_bear_score` | Bear-directed reversal composite, gated by Score | 0–100 |

Also expose a numeric readiness field, e.g. `reversal_ready` (`0` during
warm-up/stale data, `1` when history is sufficient). Until ready, set all ten
numeric metric outputs to `0.0`; consumers must check `reversal_ready` and must
not interpret warm-up zeros as measured neutral velocities.

## 60-second rolling history and derivative calculation

### Buffer contents

For each published refresh, append:

```text
(monotonic_timestamp, bias, momentum, score, iv_skew_est, gamma_boost_val)
```

Use a monotonic clock for the derivative time axis. Wall-clock timestamps can
be retained separately for logging, but must not be used to calculate rates.
Prune samples older than 60 seconds on every update. A deque with a safety
capacity (for example 120 samples) plus timestamp pruning keeps memory bounded
even if the refresh cadence changes.

The five values must be finite numbers. Reject an invalid sample rather than
turning a missing value into zero, because that would create an artificial
one-tick drop and a large false derivative.

### Proposed fit

Fit a quadratic independently to each of the five input series over the
available rolling window. Let `t_now` be the update timestamp and define:

```text
x_i = (t_i - t_now) / 60
y_i = c0 + c1*x_i + c2*x_i^2 + error_i
```

Obtain `c0`, `c1`, and `c2` by ordinary least squares over the timestamped
samples. Since `x` is measured in 60-second units, the derivatives at the
current endpoint (`x=0`) are:

```text
velocity_now     = c1 / 60
acceleration_now = 2*c2 / (60*60)
```

Apply this fit separately to `bias`, `momentum`, `gamma_boost_val`, and
`iv_skew_est` to get the eight outputs. Fit Score only as an input level for
the Early Bull/Bear gate; do not add Score Velocity or Score Acceleration
unless separately requested.

This regression uses actual sample timestamps, tolerates small differences
between the normal publish rate and forced refreshes, and smooths noise without
changing the source values. It is a trend estimate over the last minute, not a
tick-to-tick finite difference.

### Readiness and reset thresholds

Recommended initial guards:

- Require at least **8 valid samples** spanning at least **30 seconds** before
  setting `reversal_ready=1`.
- Use only samples at or newer than `t_now - 60 seconds`.
- If the time since the prior valid update exceeds **10 seconds**, treat the
  series as interrupted: clear the buffer and warm up again. This avoids
  interpreting a feed outage as a fast move.
- Reset on active-expiry change and trading-date rollover. The 60-second
  history is process-local, so a worker restart naturally returns to warm-up.
- If there is insufficient history, a singular/ill-conditioned quadratic fit,
  or non-finite results, keep readiness false and return neutral placeholder
  numbers.

These are new-engine defaults, not existing project constants. They should be
tunable module constants, not environment settings until operational evidence
requires configuration.

## Proposed velocity and acceleration thresholds

Use thresholds both as initial action levels and as scale references for the
composite score. Values below are recommended starting points, not values
already present in the repository. They should be calibrated on recorded live
sessions before being treated as production signal thresholds.

| Series | Velocity reference / threshold | Acceleration reference / threshold |
|---|---:|---:|
| Bias | 0.10 Bias points/s (6 points/min) | 0.003 Bias points/s² |
| Momentum | 0.15 Momentum points/s (9 points/min) | 0.005 Momentum points/s² |
| Gamma Boost | 0.05 boost points/s (3 points/min) | 0.002 boost points/s² |
| IV Skew | 0.025 IV points/s (1.5 points/min) | 0.001 IV points/s² |

The velocity figures express movement over the full 60-second window in
per-second units. Acceleration references express a change in velocity per
second. They are not hard clamps on the reported raw derivative values.

## Early Bull/Bear composite

### Direction mapping

Set a bull-direction sign `d_i` for each input:

```text
d_bias = +1
d_momentum = +1
d_gamma = +1
d_iv_skew = -1
```

The negative IV sign encodes the existing IV Skew definition: falling
put-minus-call IV skew contributes to a bullish reversal; rising skew
contributes to a bearish reversal.

For each series `i`, normalize velocity and acceleration using the proposed
thresholds, then clip only these normalized scoring components:

```text
v_i_norm = clamp(d_i * velocity_i / velocity_threshold_i, -1, +1)
a_i_norm = clamp(d_i * acceleration_i / acceleration_threshold_i, -1, +1)
component_i = 0.70 * v_i_norm + 0.30 * a_i_norm
```

The raw derivatives remain unclipped in the eight reported velocity and
acceleration outputs. The normalized scoring components are clipped to prevent
one unusually large value from dominating the composite.

Weight the four components:

```text
R = 0.30*component_bias
  + 0.30*component_momentum
  + 0.25*component_gamma
  + 0.15*component_iv_skew
```

Weights sum to 1.0. Bias and Momentum are highest-weight because they reflect
the existing directional OI and COI signals; Gamma Boost is a strong
confirmation channel; IV Skew is a lower-weight volatility sentiment channel.
These are initial design weights to validate, not existing weights.

Convert the signed composite to separate nonnegative scores and apply the
existing Score as an “early/not-yet-confirmed” gate:

```text
early_bull_score = 100 * max(R, 0)    if score < 60 else 0
early_bear_score = 100 * max(-R, 0)   if score > 40 else 0
```

The scores are mutually exclusive because they use the positive and negative
parts of one signed composite. Score itself is not recomputed. The asymmetric
gates are mirror images around neutral 50: stop calling a bullish reversal
“early” after Score reaches 60, and stop calling a bearish reversal “early”
after Score reaches 40. The acceleration/velocity signals can therefore turn
before the existing Score crosses its confirmation boundary.

### Candidate trigger thresholds

Recommended first-pass thresholds:

- A candidate is **strong** when its Early Bull/Bear Score is **at least 65**.
- Require at least **3 of 4** series to independently meet its directional
  condition: `d_i * velocity_i >= velocity_threshold_i` **or**
  `d_i * acceleration_i >= acceleration_threshold_i`.
- Require the same candidate to pass both conditions on **2 consecutive
  published updates** before emitting/raising a reversal event. This is a
  persistence gate only; do not average or rewrite the published input values.
- If implementing a latched event, clear it when its candidate score falls
  below **50**, the corresponding Score gate closes, the data becomes stale,
  or the expiry/session resets. To avoid chatter at the Score boundary, an
  optional 2-point hysteresis can clear Early Bull at `score >= 62` and Early
  Bear at `score <= 38`; still report the continuous candidate scores
  independently.

The thresholds are operational starting values. They should be validated
against the project's historical/live signal logs and adjusted only as
new-engine constants; the existing six metric formulas remain untouched.

## Pseudocode

```text
class ReversalEngine:
    history = deque()
    session_key = None
    last_timestamp = None
    pending_bull_count = 0
    pending_bear_count = 0

    update(now_monotonic, session_key, bias, momentum, score,
           iv_skew_est, gamma_boost_val):
        if any input is missing or non-finite:
            return neutral_outputs(ready=False)

        if session_key changed or last_timestamp exists
           and now_monotonic - last_timestamp > 10 seconds:
            clear history and pending confirmation counts

        append (timestamp, five input values)
        prune entries older than now_monotonic - 60 seconds
        last_timestamp = now_monotonic

        if fewer than 8 samples or coverage < 30 seconds:
            return neutral_outputs(ready=False)

        for series in [bias, momentum, gamma_boost_val, iv_skew_est]:
            c0, c1, c2 = least_squares_quadratic(
                x=(sample.timestamp - now_monotonic) / 60,
                y=sample.series_value
            )
            velocity[series] = c1 / 60
            acceleration[series] = 2*c2 / 3600

        for series:
            oriented_v = clamp(direction[series] * velocity[series]
                               / velocity_threshold[series], -1, +1)
            oriented_a = clamp(direction[series] * acceleration[series]
                               / acceleration_threshold[series], -1, +1)
            component[series] = 0.70*oriented_v + 0.30*oriented_a

        R = 0.30*component[bias] + 0.30*component[momentum]
          + 0.25*component[gamma] + 0.15*component[iv_skew]

        bull = 100*max(R, 0) if score < 60 else 0
        bear = 100*max(-R, 0) if score > 40 else 0
        bull_count = number of series meeting bull velocity OR acceleration threshold
        bear_count = number of series meeting bear velocity OR acceleration threshold

        update two-consecutive-refresh confirmation counters
        return eight derivatives, bull, bear, ready=True
```

The production implementation should use a small standard-library least
squares solver or an already approved numerical dependency. It should not add
a heavyweight dependency just for a 3-parameter fit.

## Integration points

1. **Create the module:** add
   `Nse_Project/Live_dashboard/reversal_engine.py`. Define an independent
   `ReversalEngine` with a bounded 60-second deque, injectable monotonic time
   for tests, session/expiry reset behavior, and an `update(...)` method that
   returns the ten metrics plus readiness.
2. **Instantiate once in the feed worker:** import and create one
   `ReversalEngine` singleton in
   [`upstox_live_feed.py`](Nse_Project/Live_dashboard/upstox_live_feed.py).
   Do not instantiate it on every tick or inside `_compute_live_sentiment_full`.
3. **Hook after the original formulas:** in `_refresh_market_state`, after
   `_compute_live_sentiment_full` has returned and the five existing values
   have been assigned to `market_state`, call the engine with exactly those
   values and a monotonic timestamp. Pass active expiry/trading date as a
   reset key, not as a feature. Store only the returned new-engine outputs in
   new `market_state` keys. Do not write to the existing five fields, alter
   `sentiment`, or alter `_compute_live_sentiment_full`.
4. **Preserve the fields:** add numeric defaults for the eight derivative
   outputs, two Early scores, and `reversal_ready` in
   [`market_state.py`](Nse_Project/Live_dashboard/market_state.py). This is
   necessary because its normalization step copies only declared fields.
5. **Expose the API:** add the new fields to the normal result and error
   fallback in `_build_live_option_chain_payload` /
   `live_option_chain_api` in
   [`views.py`](Nse_Project/Live_dashboard/views.py). Keep unavailable or
   warming-up results neutral and expose readiness separately.
6. **Optional dashboard work:** add a distinct Reversal panel to
   `live_signals.html`, rendered from API values. Do not overload the existing
   Bias/Momentum/Gamma/IV fields or their Math Console bars.
7. **Keep the call serialized:** `_refresh_market_state` is invoked from a
   thread-pool publish path. Protect the engine's mutable history and
   confirmation counters with a `threading.Lock`, or otherwise guarantee
   serialized calls. The existing asyncio `state_lock` protects feed-state
   mutations; it does not by itself protect engine calls made in executor
   threads.

### Testing plan for implementation

- Fake-clock linear input: verify each velocity equals the known linear slope
  and each acceleration is approximately zero.
- Fake-clock quadratic input: verify the fitted endpoint velocity and
  second derivative.
- Direction symmetry: bullish-oriented series increases (and IV Skew falls)
  increase Early Bull and not Early Bear; inverse movements do the reverse.
- Score gates: Early Bull is zero at/above 60; Early Bear is zero at/below 40.
- Clipping: extreme derivatives saturate normalized components but leave raw
  derivative outputs unchanged.
- Warm-up, stale gap, session/expiry reset, missing/non-finite input, and
  two-refresh trigger persistence.
- Integration: verify original six values are byte-for-byte/value-for-value
  unchanged when the new engine is wired into `_refresh_market_state`.
