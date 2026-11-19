# Production Implementation Roadmap: Volume Flow, Reversal, Exhaustion

## Objective and safety contract

Implement the three designs in dependency order:

1. **Phase 1 — Volume Flow Engine**
2. **Phase 2 — Reversal Engine**
3. **Phase 3 — Exhaustion Engine**

All phases are additive. None may modify the existing Bias, Momentum, Delta
Wt, Score, IV Skew, or Gamma Boost formulas or their current output fields.
New values must not feed back into `_compute_live_sentiment_full`, change
trade-side selection, or submit orders during this roadmap. Run new signals in
shadow mode until data quality and out-of-sample performance are accepted.

The roadmap is based on
[`MARKET_METRICS_TECHNICAL_AUDIT.md`](MARKET_METRICS_TECHNICAL_AUDIT.md),
[`REVERSAL_ENGINE_DESIGN.md`](REVERSAL_ENGINE_DESIGN.md), and
[`VOLUME_FLOW_EXHAUSTION_DESIGN.md`](VOLUME_FLOW_EXHAUSTION_DESIGN.md).

## Existing integration boundaries

- Upstox V3 message callback and feed state: `build_streamer`,
  `update_live_state_from_message`, `update_instrument_state`, and
  `update_index_spot` in
  [`Nse_Project/Live_dashboard/upstox_live_feed.py`](Nse_Project/Live_dashboard/upstox_live_feed.py).
- Current market metrics are produced in `_refresh_market_state` and
  persisted from `_publish_and_persist_sync` in the same file. That path runs
  in an executor thread.
- Redis market-state fields are explicitly whitelisted/defaulted by
  [`Nse_Project/Live_dashboard/market_state.py`](Nse_Project/Live_dashboard/market_state.py);
  undeclared fields are lost during normalization.
- API fields are assembled in
  [`Nse_Project/Live_dashboard/views.py`](Nse_Project/Live_dashboard/views.py),
  particularly `_build_live_option_chain_payload`.
- The existing dashboard polls the API from
  [`Nse_Project/Master_Temp/live_signals.html`](Nse_Project/Master_Temp/live_signals.html).
- Existing `CVDEngine` in
  [`Nse_Project/Brokers/cvd_engine.py`](Nse_Project/Brokers/cvd_engine.py)
  classifies quote-side tick volume but publishes its main CVD in two-minute
  candles. Do not use the two-minute value as the sole input to a 20-second
  engine. Prefer a new adapter/worker and leave `cvd_engine.py` unchanged
  unless implementation proves a specific shared parser change is necessary.

## Phase summary

| Phase | Deliverable | Depends on | Estimated complexity |
|---|---|---|---|
| 1 | Raw-tick volume/flow and spot feature snapshot | Existing Upstox feed, instrument metadata, Redis/API pipeline | High; approximately 4–6 engineering days plus replay calibration |
| 2 | Derivatives of the five published metrics and Early Bull/Bear scores | Phase 1 is not a formula dependency; shared publish/state path | Medium; approximately 2–4 engineering days plus replay calibration |
| 3 | Bull/Bear Exhaustion scores and watch state | Phase 1 required; Phase 2 derivatives optional corroboration | High; approximately 3–5 engineering days plus replay calibration |

Estimates include focused tests and integration, not dashboard styling,
multi-session data collection, or broker/exchange verification.

---

## Phase 1 — Volume Flow Engine

### Goal and dependency

Build an independent tick-window engine for CE/PE option volume activity,
signed futures aggressor flow, and NIFTY spot movement. Use cumulative `vtt`
deltas and live quote/price ticks; do not modify current volume-velocity or
sentiment formulas. The engine is the mandatory data dependency for Phase 3.

### Exact files to add or modify

**Add**

- `Nse_Project/Live_dashboard/volume_flow_engine.py` — stateless parsing
  helpers plus the bounded-history engine and result types.
- `Nse_Project/TESTING/volume_flow_engine_test.py` — deterministic unit tests
  using supplied timestamps, volume counters, spot ticks, and quote cases.

**Modify**

- `Nse_Project/Live_dashboard/upstox_live_feed.py` — normalize/enrich raw
  feed ticks with side/strike/expiry metadata; route option/future samples to
  the engine and canonical index ticks to its spot history; instantiate one
  worker-owned engine; expose a synchronized latest snapshot; reset on
  expiry/session transitions. Do not change `_compute_live_sentiment_full`.
- `Nse_Project/Live_dashboard/market_state.py` — add defaults for the
  Phase-1 outputs and readiness/status metadata.
- `Nse_Project/Live_dashboard/views.py` — return the new fields from the
  normal API response and safe error/default response.
- `Nse_Project/Master_Temp/live_signals.html` — optional but recommended
  additive panel for volume, spot, freshness, and coverage; do not reuse or
  redefine existing Math Console fields.

**Do not modify by default**

- `Nse_Project/Brokers/cvd_engine.py`: reuse its known Upstox field semantics
  only as reference. Avoid coupling the new 20–60s engine to its queue,
  two-minute aggregation, or state.
- Any existing sentiment, score, or trade-side formulas.

### New class and functions

**New class**

- `VolumeFlowEngine` — owns per-instrument cumulative-VTT histories, quote
  side classification histories, spot samples, session/expiry key, rolling
  calculations, and data-quality flags. Use one bounded, serialized worker
  or locks for state safety.

**New functions/method responsibilities**

- `normalize_feed_tick(...)` — converts one Upstox V3 instrument update into a
  normalized record: instrument key, timestamp, LTP, VTT, best bid/ask,
  expiry, strike, and option side.
- `ingest_tick(...)` — validates ordering/freshness, handles VTT counter
  resets, and appends the sample.
- `ingest_message(...)` — extracts the feed records from a V3 envelope or
  accepts records from the existing callback adapter.
- `update_spot(...)` — accepts canonical index `ltpc.ltp` only; records spot
  observations with event and monotonic receive times.
- `snapshot(...)` — returns the latest 20/40/60s side velocities, flow
  metrics, spot metrics, coverage, age, and readiness flags.
- Internal functions for horizon deltas, strike weighting, aggressor
  classification, freshness/coverage, realized volatility, and history reset.

Names may be adjusted to repository conventions during implementation, but
the module must not import `upstox_live_feed.py` or Django.

### New market-state fields and API fields

Use the same names in Redis market state and the API response. Store only
latest numeric summaries and statuses, not raw samples:

**Option volume activity**

- `call_volume_velocity_20`, `call_volume_velocity_40`,
  `call_volume_velocity_60`
- `put_volume_velocity_20`, `put_volume_velocity_40`,
  `put_volume_velocity_60`
- `total_option_volume_velocity_20`, `_40`, `_60`
- `call_put_volume_imbalance_20`, `_40`, `_60`
- `relative_volume_20`, `_40`, `_60`

**Signed flow**

- `futures_aggressor_imbalance_20`, `_40`, `_60`
- `futures_classified_coverage_20`
- `option_directional_flow_20`
- `option_classified_coverage_20`

**Spot and data quality**

- `spot_velocity_20`, `spot_velocity_40`, `spot_velocity_60`
- `spot_acceleration_20_40`, `spot_acceleration_40_60`
- `spot_realized_volatility_60_bp`
- `volume_flow_ready`, `spot_flow_ready`
- `volume_flow_updated_at`, `volume_flow_status`

Document units explicitly: VTT/feed quantity units per second; spot velocity
in basis-points per second; spot acceleration in basis-points per second
squared; imbalance/coverage as fractions in `[0,1]`. Do not apply lot-size
conversion to VTT.

### Integration point and sequence

1. Add unit tests for the independent module before wiring it to the feed.
2. Route raw V3 records from `build_streamer(...).on_message` to the new
   engine through a bounded queue or equivalent nonblocking adapter.
3. Feed canonical index spot ticks from `update_index_spot` without waiting
   for `_refresh_market_state`'s publish cadence.
4. On VTT reset, expired contract, stale leg, or session rollover, invalidate
   affected histories and readiness rather than emitting artificial zero or
   spike values.
5. Fold the immutable engine snapshot into `market_state` in the publish
   boundary, without holding the asyncio feed-state lock across computation or
   I/O.
6. Add market-state defaults and API output before optional UI rendering.
7. Release in shadow mode: display/log only; do not connect to trade
   decisions.

### Testing plan and gate

- Unit tests: regular and irregular timestamps; 20/40/60 horizon endpoints;
  CE/PE aggregation and weighting; VTT counter rollback; duplicate and
  out-of-order ticks; stale contracts; expiry isolation; invalid quotes;
  inside-spread unclassified volume; locked/crossed quote handling; zero
  activity; missing baseline; spot return and acceleration.
- Integration tests: raw feed envelope normalization; metadata enrichment;
  nonblocking ingestion; state snapshot concurrency; Redis normalization
  retains every new field; API normal/error payload shapes.
- Verify existing `compute_volume_velocity`, CVD, sentiment, and dashboard
  contracts remain unchanged.
- **Gate:** no Phase 2 dependency until shadow telemetry demonstrates valid
  freshness/coverage and no volume spikes on reconnect/session reset across
  representative sessions.

---

## Phase 2 — Reversal Engine

### Goal and dependency

Calculate 20/40/60s velocities and accelerations from the **published**
`bias`, `momentum`, `gamma_boost_val`, and `iv_skew_est` series, then form
Early Bull/Bear scores using the unchanged published Score as a gate. The
engine has no formula dependency on Phase 1, but should follow Phase 1 in the
production sequence so shared tick/publish plumbing and state conventions
are already proven.

Use a bounded 60-second buffer, actual timestamps, explicit warm-up and stale
resets, and the multi-horizon improvement from the quantitative review.
Do not treat the current feed's `acceleration` field (a one-sample COI
difference used by existing Momentum) as any of the new second derivatives.

### Exact files to add or modify

**Add**

- `Nse_Project/Live_dashboard/reversal_engine.py` — independent
  `ReversalEngine`, rolling sample buffer, robust per-window slope and
  acceleration estimates, score normalization, readiness and optional
  two-refresh confirmation state.
- `Nse_Project/TESTING/reversal_engine_test.py` — fake-clock, known-linear,
  known-quadratic, gate, reset, and direction tests.

**Modify**

- `Nse_Project/Live_dashboard/upstox_live_feed.py` — instantiate one engine;
  call it after the five authoritative values are computed and assigned in
  `_refresh_market_state`; write only new keys. Ensure serialized access
  because this path runs in an executor thread.
- `Nse_Project/Live_dashboard/market_state.py` — declare default values for
  derivative scores and readiness/status.
- `Nse_Project/Live_dashboard/views.py` — include fields in success and
  failure/default API payloads.
- `Nse_Project/TESTING/market_state_test.py` — if such a focused test file
  does not exist, add it; otherwise extend the existing market-state test
  coverage in place.
- `Nse_Project/Master_Temp/live_signals.html` — optional separate
  Reversal panel. Do not overwrite or rename the existing six metric nodes.

### New class and functions

**New class**

- `ReversalEngine` — bounded time-series storage, resets, multiscale
  derivatives, normalized sentiment components, Early Bull/Bear scores, and
  persistence counters.

**New function/method responsibilities**

- `update(timestamp, session_key, bias, momentum, score, iv_skew_est,
  gamma_boost_val)` — validates the inputs, resets on session/expiry/stale
  gaps, inserts the sample, and returns all outputs.
- `fit_slope(series, horizon)` — estimates a timestamp-aware robust or
  least-squares slope on a trailing 20/40/60-second horizon.
- `derive_acceleration(series)` — estimates acceleration from adjacent
  horizon slopes with their effective midpoint separation.
- `score_reversal(...)` — applies direction orientation, threshold
  normalization, component weights, Score gate, and persistence criteria.
- `reset(session_key)` — clears samples and pending counts on expiry/date
  changes.

The four named input series are Bias, Momentum, Gamma Boost, and IV Skew.
Score is a gate, not a velocity-series input. Preserve raw derivatives
unclipped; clip only normalized scoring components.

### New market-state fields and API fields

Use the following exact additive fields:

- `bias_velocity_20`, `_40`, `_60`
- `momentum_velocity_20`, `_40`, `_60`
- `gamma_velocity_20`, `_40`, `_60`
- `iv_skew_velocity_20`, `_40`, `_60`
- `bias_acceleration_20_40`, `bias_acceleration_40_60`
- `momentum_acceleration_20_40`, `momentum_acceleration_40_60`
- `gamma_acceleration_20_40`, `gamma_acceleration_40_60`
- `iv_acceleration_20_40`, `iv_acceleration_40_60`
- `early_bull_score`, `early_bear_score`
- `reversal_ready`, `reversal_updated_at`, `reversal_status`

For compatibility with the first reversal design's singular derivative
names, optionally expose `bias_velocity`, `momentum_velocity`,
`gamma_velocity`, and `iv_skew_velocity` as aliases of the designated
20-second slope, and `bias_acceleration`, `momentum_acceleration`,
`gamma_acceleration`, and `iv_acceleration` as aliases of the
20/40-second estimate. Document the aliases; do not store duplicate
independently calculated values.

### Integration point and sequence

1. Start only after the Phase-1 publication/state path and schema tests pass.
2. In `_refresh_market_state`, call the reversal engine after
   `_compute_live_sentiment_full` completes and after the authoritative five
   fields are assigned. Pass those values directly; do not call the metric
   calculation again.
3. Use a monotonic timestamp for rate calculations. Use a session key
   containing trading date and active expiry for deterministic resets.
4. Keep engine mutation serialized or protected by an explicit lock; the
   asyncio `state_lock` alone does not protect executor-thread calls.
5. Persist only current outputs and status. The raw 60-second sample buffer
   remains in memory and restarts in a warm-up state.
6. Add the Redis schema defaults and API contract. Render the new panel only
   after API behavior is verified.
7. Remain shadow-only and do not gate CE/PE selection.

### Testing plan and gate

- Fake-clock linear data must return known slopes and near-zero acceleration;
  quadratic data must return expected endpoint slopes/second derivatives.
- Test irregular sample intervals, missing/nonfinite data, insufficient
  coverage, stale gap, session/expiry reset, and process warm-up.
- Test correct sign mapping, including falling IV Skew as bull-oriented.
- Test independent 20/40/60 behavior and ensure a 60-second countertrend
  context does not suppress a valid early candidate by itself.
- Test Score gates/hysteresis, score bounds, and persistence counters.
- Integration regression: snapshot the six existing values before/after
  adding the hook and assert no values or formulas changed.
- **Gate:** require stable derivatives and score distributions in replay,
  with no output during unready/stale states, before Phase 3 consumes the
  reversal outputs.

---

## Phase 3 — Exhaustion Engine

### Goal and dependency

Score Bull Exhaustion after a stretched down move begins losing downside
force; score Bear Exhaustion after a stretched up move begins losing upside
force. Consume Phase-1 spot, activity, quote-side flow, freshness, and
coverage. Phase-2 velocities may be included as optional context only; avoid
counting correlated OI/COI-derived Bias, Momentum, Gamma, and Score signals
as independent votes.

### Exact files to add or modify

**Add**

- `Nse_Project/Live_dashboard/exhaustion_engine.py` — `ExhaustionEngine`,
  scoring components, readiness/eligibility logic, watch state, cancellation
  and hysteresis behavior.
- `Nse_Project/TESTING/exhaustion_engine_test.py` — synthetic price/flow
  scenarios and missing/stale-data cases.

**Modify**

- `Nse_Project/Live_dashboard/upstox_live_feed.py` — instantiate and feed the
  engine from the latest synchronized Phase-1 snapshot plus optional Phase-2
  outputs. Store new outputs only; do not change existing calculations or
  trade routing.
- `Nse_Project/Live_dashboard/market_state.py` — add defaults for exhaustion
  scores, state, readiness, timestamps, and any component diagnostics chosen
  for operations.
- `Nse_Project/Live_dashboard/views.py` — return the fields in both normal
  and fallback API payloads.
- `Nse_Project/Master_Temp/live_signals.html` — separate Bull/Bear Exhaustion
  watch panel, if dashboard display is included in release.

### New class and functions

**New class**

- `ExhaustionEngine` — combines trend stretch, price deceleration, flow
  opposition, price-flow divergence, and relative-volume climax; manages
  readiness, candidate persistence, active watch, invalidation, and reset.

**New function/method responsibilities**

- `update(timestamp, session_key, volume_flow_snapshot, spot_snapshot,
  optional_reversal_snapshot)` — consumes aligned snapshots and emits scores
  plus statuses.
- `calculate_stretch(...)` — tests whether the 60-second move is large enough
  relative to a floor and realized volatility.
- `calculate_deceleration(...)` — scores acceleration opposing the prior
  60-second trend.
- `calculate_flow_opposition(...)` — uses futures aggressor imbalance only
  when classified coverage is sufficient.
- `calculate_divergence(...)` — checks price movement against opposing signed
  flow; optionally tracks confirmed swing pivots as slower confirmation.
- `calculate_exhaustion_scores(...)` — applies the specified bull/bear
  component weights and renormalizes only when a component is unavailable.
- `update_watch_state(...)` — requires two consecutive valid evaluations,
  applies hysteresis, and clears on stale inputs, invalidation, or reset.

### New market-state fields and API fields

Essential outputs:

- `bull_exhaustion_score`
- `bear_exhaustion_score`
- `exhaustion_ready`
- `exhaustion_status` (`unready`, `neutral`, `bull_watch`,
  `bear_watch`, or `invalidated`)
- `exhaustion_updated_at`

Recommended operational diagnostics:

- `bull_exhaustion_stretch`
- `bear_exhaustion_stretch`
- `bull_exhaustion_deceleration`
- `bear_exhaustion_deceleration`
- `bull_exhaustion_flow`
- `bear_exhaustion_flow`
- `bull_exhaustion_divergence`
- `bear_exhaustion_divergence`
- `exhaustion_volume_climax`
- `exhaustion_data_quality`

Expose the same names through the API. Component diagnostics make a score
auditable; do not add them to existing metric fields. If minimizing schema
surface is necessary, keep the component values in structured logs during
shadow validation and publish only essential outputs.

### Integration point and sequence

1. Begin after Phase 1 has passed raw-tick and coverage gates. Phase 2 should
   also be stable if its outputs are used; otherwise exclude it from the
   score until it passes.
2. Evaluate from a consistent, timestamp-aligned snapshot. Do not combine a
   current spot tick with stale option activity or stale aggressor flow.
3. Require a stretched directional 60-second move, deceleration, and valid
   flow/divergence confirmation. Relative-volume climax alone cannot activate
   a score.
4. Require two consecutive valid evaluations before changing a watch state.
   Publish scores and state for observation only.
5. Add market-state defaults, API fields, fallback behavior, and optional
   dashboard panel.
6. Shadow-trade/replay through representative weekly-expiry, expiry-day,
   opening, midday, and close regimes. Do not create order actions as part of
   this phase.

### Testing plan and release gate

- Bull exhaustion: a stretched downtrend, improving spot acceleration,
  opposing buy flow, and/or lower-low/higher-flow divergence raises Bull
  Exhaustion; mirrored inputs raise Bear Exhaustion.
- A flat market, same-direction acceleration, or activity spike without
  opposing flow/divergence must not create an exhaustion alert.
- Missing or low-coverage aggressor classification must make the result
  unready or renormalize only as specified; it must never be treated as
  measured neutral flow.
- Test 65/80 watch thresholds, two-update persistence, hysteresis,
  invalidation, stale data, session/expiry reset, and time alignment.
- Integration test normal/error API payload shapes and ensure new fields
  cannot mutate the existing six metrics or trade decisions.
- **Release gate:** remain shadow-only until multi-session replay demonstrates
  acceptable false-alert rate, reversal delay, post-alert excursion, and net
  results after slippage/fees. Promotion to any trading gate requires a
  separate explicit approval and risk review.

---

## Production implementation sequence

### Gate 0 — Baseline protection

1. Capture the current behavior of the six existing metrics using existing
   focused feed tests and deterministic representative payloads.
2. Add/confirm regression assertions for their exact output fields and
   values. Do not edit the production formulas.
3. Define units, event-time policy, stale-state behavior, session key, and
   field naming before adding schema changes.

### Gate 1 — Phase 1, shadow-only

1. Implement and unit-test the isolated Volume Flow Engine.
2. Integrate raw tick ingestion with nonblocking bounded queue semantics.
3. Add Redis market-state defaults and API fields with readiness/status.
4. Verify counter resets, quote coverage, instrument/expiry filtering, and
   thread safety.
5. Add an optional isolated dashboard panel or structured telemetry.
6. Collect and replay sessions; do not route any signal to order logic.

### Gate 2 — Phase 2, shadow-only

1. Implement and test the Reversal Engine independently with fake time.
2. Hook after the existing values are computed in `_refresh_market_state`.
3. Add its fields to market-state normalization and both API response paths.
4. Confirm concurrent executor publishes cannot mutate the buffer
   concurrently.
5. Assert old metric outputs are unchanged; collect score and derivative
   distributions in replay.

### Gate 3 — Phase 3, shadow-only

1. Implement the Exhaustion Engine against immutable Phase-1 and optional
   Phase-2 snapshots.
2. Add auditable component outputs and explicit unready/stale status.
3. Add persistence/invalidation tests and score calibration replay.
4. Display only as an informational watch. Do not connect it to existing
   order selection or execute orders.

### Gate 4 — Operational acceptance

- Compare feed event time and local receive time; monitor queue depth, missed
  ticks, stale ages, and coverage.
- Validate on multiple days and regimes, including weekly expiry and expiry
  day; use out-of-sample data for threshold selection.
- Review false positives, signal delay, spreads, slippage, and transaction
  costs.
- Preserve a kill switch that disables only the new engines/outputs without
  affecting the existing metrics or feed.
- Any later connection to trade decisions is a separate change with explicit
  risk controls, paper-trading evidence, and approval.

## Cross-phase regression checklist

- No change to existing metric formulas, values, names, thresholds, or
  cadence.
- No new engine treats missing/stale input as a real zero or uses stale
  restored spot as a current tick.
- All timeseries are bounded, event-time aware, and reset on session/expiry
  changes or relevant counter rollback.
- New Redis fields are declared in `DEFAULT_MARKET_STATE`; API failure output
  has safe types and readiness flags.
- Raw histories do not get persisted to the primary live snapshot unless
  specifically required; only compact results/statuses are published.
- New outputs do not affect trade-side selection, order execution, or
  existing dashboards except through explicitly additive display.
