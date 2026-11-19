# Phase-1: Volume Flow Engine Module Implementation Plan

**Date**: 2026-11-18  
**Spec**: User request for Phase-1 `Nse_Project/Live_dashboard/volume_flow_engine.py`  
**Design inputs**: `REVERSAL_ENGINE_DESIGN.md`, `VOLUME_FLOW_EXHAUSTION_DESIGN.md`, `IMPLEMENTATION_ROADMAP_REVERSAL_ENGINES.md`, and `MARKET_METRICS_TECHNICAL_AUDIT.md`

## Summary

Implement a standalone, standard-library-only `VolumeFlowEngine` in
`Nse_Project/Live_dashboard/volume_flow_engine.py`. It will accept normalized
Upstox V3 option/future ticks and canonical NIFTY spot ticks; maintain bounded
timestamped histories; derive VTT deltas safely; calculate weighted CE/PE
volume velocities over 20, 40, and 60 seconds; and expose immutable snapshots
with aggressor-flow and spot-velocity diagnostics.

The module is deliberately independent of Django, Redis, Upstox client
packages, `upstox_live_feed.py`, and `cvd_engine.py`. This plan does not
include feed/API/dashboard wiring, market-state changes, or Phase-2/Phase-3
work. The only production source file in scope is the new engine module; the
requested tests are a separate new test file.

## Requirements

- **REQ-001**: Implement Phase-1 as a new, standalone
  `Nse_Project/Live_dashboard/volume_flow_engine.py`; do not modify existing
  metric formulas or add feed/API/dashboard integration in this phase.
- **REQ-002**: Define explicit normalized input, registration, horizon, and
  output data structures, plus a clear public `VolumeFlowEngine` API.
- **REQ-003**: Maintain bounded per-instrument tick/VTT histories and
  correctly baseline, accumulate, and reset cumulative VTT deltas.
- **REQ-004**: Calculate call and put volume velocities over 20s, 40s, and
  60s using actual elapsed time, active-expiry/near-ATM filtering, and
  coverage/freshness checks.
- **REQ-005**: Provide a nonblocking bounded ingestion queue, a single
  history-owning worker, safe lifecycle, and thread-safe immutable snapshots.
- **REQ-006**: Include normalized signed aggressor-flow and NIFTY spot
  velocity/acceleration results as specified by the Phase-1 design.
- **REQ-007**: Add deterministic unit tests for formulas, resets, queue
  behavior, coverage, spot calculations, and concurrency/lifecycle.

## Technical Context

**Language/Version**: Python; follow the repository's current type-hint and
dataclass conventions.  
**Primary Dependencies**: Python standard library only (`dataclasses`,
`collections`, `queue`, `threading`, `time`, `datetime`, `math`, `typing`,
`logging`). No new package installation.  
**Storage**: In-memory bounded histories only. Do not write to Redis, disk, or
database from this module.  
**Testing**: Standalone Python test runner using the standard library, in the
style of `Nse_Project/TESTING/poi_velocity_test.py`; no pytest dependency is
required.  
**Target Platform**: Existing Python feed worker environment, including
Windows.  
**Performance Goals**: Ingestion must never wait for history calculations;
accepted tick enqueue is nonblocking and bounded. History mutation and
calculation are performed by one worker. Snapshot reads are short and
thread-safe.  
**Constraints**:

- The module accepts normalized ticks; the Upstox protobuf/feed adapter is
  outside this scope.
- VTT is cumulative quantity. Do not apply the OI lot-size conversion to VTT.
- Use local monotonic receive time for elapsed-time calculations and event
  timestamps for ordering/deduplication where valid.
- Invalid/missing/stale samples produce unavailable/degraded metrics, not
  synthetic zeros or fabricated flow.
- A new `VolumeFlowEngine` must not modify or import the existing six-metric
  implementation.
- No `constitution.md` or project topology was found. The four named design
  documents are the upstream design inputs; the user request is the feature
  specification for this scoped plan.

## Constitution Check

No repository constitution file was found. Based on the supplied designs and
the current feed conventions:

| Principle | Result | Plan evidence |
|---|---|---|
| Preserve existing calculations and behavior | PASS | New module only; no existing formula, feed, or API modifications |
| Keep tick callback responsive | PASS | `queue.Queue.put_nowait`; all rolling calculations on one worker |
| Bound memory and expose data quality | PASS | Time-pruned deques, capacity safeguards, explicit readiness/drop counters |
| Make time-based calculations deterministic | PASS | Injectable monotonic clock and explicit timestamped input records |
| Avoid new unnecessary dependencies | PASS | Standard library only |

## Applied Guidelines

No migration/transformation guideline applies to this standalone Python
analytics module. Follow existing project patterns from
`Nse_Project/Brokers/cvd_engine.py` for normalized tick records, bounded
`queue.Queue` ingestion, a single daemon worker, idempotent `start()` /
`stop()`, and nonblocking input. Do not copy its two-minute candle or Redis
publishing responsibilities.

## Implementation Steps

### Step 1: [Cross-cutting] Establish module contracts and deterministic test fixtures

- **Requirements**: REQ-001, REQ-002, REQ-007
- **Design inputs**: Phase-1 Volume Flow design; current CVD worker queue and
  tick-record conventions.
- **Description**: Define and freeze the public input/output contract before
  adding calculations. Create fake-clock and normalized input fixtures in
  the standalone test file. The module must be importable without Django,
  Redis, Upstox SDK, pandas, or `cvd_engine.py`.

#### Class structure

`VolumeFlowEngine` is the only public stateful engine class. It owns the
registration catalog, bounded queue, worker lifecycle, internal histories,
quality counters, and the last published immutable snapshot.

Public API responsibilities:

- `register_instrument(spec)`: enqueue or register immutable metadata for an
  active option leg or selected NIFTY future.
- `set_active_expiry(expiry, strike_interval=None)`: enqueue a session/expiry
  context change so the worker resets histories in-order.
- `ingest(tick) -> bool`: validate only the cheap envelope essentials and
  perform a nonblocking enqueue; return whether accepted.
- `ingest_spot(tick) -> bool`: enqueue a canonical index spot record.
- `start()`: start one worker idempotently.
- `stop(timeout=...)`: signal shutdown and join with a bounded timeout.
- `snapshot() -> VolumeFlowSnapshot`: return a thread-safe immutable copy of
  the latest calculation.
- `queue_size()` and `health_snapshot()`: provide queue/drop/error diagnostics
  without exposing mutable internal buffers.

Private method responsibilities:

- `_worker_loop`: consume records serially and update all mutable histories.
- `_process_tick` / `_process_spot`: validate ordering and update state.
- `_derive_vtt_delta`: initialize, increment, or reset a cumulative VTT
  baseline.
- `_calculate_snapshot(now)`: compute each horizon and data-quality state.
- `_calculate_option_horizon(h, now, spot)`: aggregate eligible CE/PE volume
  rates and coverage for one horizon.
- `_classify_aggressor(...)`: classify only valid bid/ask executions.
- `_calculate_spot_metrics(now)`: compute returns, velocities, acceleration,
  and realized volatility.
- `_prune_history(...)`: enforce time and capacity limits while preserving
  enough boundary data for all supported horizons.
- `_reset_state(reason)`: clear histories on expiry/date changes or explicit
  reset.

#### Dataclasses

Use frozen/slotted dataclasses where they carry immutable input, metadata, or
snapshot values. Keep internal mutable history objects private.

1. **`InstrumentSpec`**
   - `instrument_key: str`
   - `kind: str` — `"option"` or `"future"`
   - `expiry: str | None`
   - `strike: float | None`
   - `option_side: str | None` — `"CE"` or `"PE"` for options
   - Metadata validation must reject options without a valid side/positive
     strike and must not require option-only fields for the future.

2. **`VolumeFlowTick`**
   - `spec: InstrumentSpec`
   - `ltp: float | None`
   - `cumulative_volume: float | None`
   - `best_bid: float | None`, `best_ask: float | None`
   - `event_time: datetime | None`
   - `received_monotonic: float`
   - The record is already normalized by its caller; it does not contain a
     raw protobuf payload.

3. **`SpotTick`**
   - `spot: float`
   - `event_time: datetime | None`
   - `received_monotonic: float`
   - Accept only a positive finite canonical index LTP.

4. **`InstrumentRegistration`** and **`EngineReset`**
   - Private control records placed on the same queue as data records to
     ensure registration/reset takes effect in input order.

5. **`InstrumentSample`** (private)
   - Monotonic receive time, event time/order key, cumulative VTT, LTP,
     bid/ask, derived volume delta, buy/sell/unclassified increments, and
     sample validity/gap flags.

6. **`SpotSample`** (private)
   - Monotonic receive time, event time/order key, and spot price.

7. **`HorizonMetrics`**
   - Horizon seconds; call/put/total volume velocity; call-put activity
     imbalance; optional relative volume; future aggressor imbalance and
     classified coverage; option directional-flow imbalance and coverage;
     per-side coverage/readiness.

8. **`VolumeFlowSnapshot`**
   - Immutable tuple of 20/40/60-second `HorizonMetrics`; spot velocity for
     20/40/60s; spot acceleration for 20/40 and 40/60; 60s realized
     volatility; overall readiness/status; snapshot time; queue drop count;
     and relevant stale/coverage diagnostics.

Use one consistent name/type for unavailable values (`None` plus an explicit
readiness/status field). Do not use `0.0` to mean “not enough samples.”

#### Internal buffers and capacities

- `_instrument_specs: dict[instrument_key, InstrumentSpec]`
- `_instrument_history: dict[instrument_key, deque[InstrumentSample]]`
- `_spot_history: deque[SpotSample]`
- `_last_vtt: dict[instrument_key, float]` if not derived from the last
  accepted sample.
- `_last_event_key: dict[instrument_key, ...]` for duplicate/out-of-order
  rejection.
- `_latest_snapshot: VolumeFlowSnapshot`, replaced atomically.
- `_queue`, `_stop_event`, worker handle, and health counters.

Prune samples older than 60 seconds plus one necessary boundary sample. Add a
per-instrument hard capacity (initial proposal: 4096 samples); if capacity
truncation prevents full horizon coverage, mark affected horizons unavailable
instead of silently calculating a shorter “60-second” rate. Bound registered
instruments to the active near-ATM option universe plus selected future.

### Step 2: [Cross-cutting] Implement VTT delta tracking and state resets

- **Requirements**: REQ-003
- **Design inputs**: VTT counter semantics from the Phase-1 design and
  `compute_volume_velocity` in `upstox_live_feed.py`.
- **Description**: Track cumulative volume independently for each instrument.
  Update the baseline only on a valid accepted tick. Do not multiply VTT by
  lot size.

#### VTT transition rules

| Condition | `delta_volume` | Required state behavior |
|---|---:|---|
| First valid VTT for an instrument | unavailable/baseline only | Seed counter; do not infer traded volume |
| New VTT greater than prior VTT | `new - prior` | Append nonnegative increment and quote/price data |
| New VTT equal to prior VTT | `0` | Append quote/price sample if changed; no traded volume |
| New VTT less than prior VTT | unavailable/reset | Count reset, discard affected instrument horizon history, seed new counter |
| Invalid/negative/nonfinite VTT | unavailable | Reject volume field; do not replace prior baseline with invalid data |
| Duplicate event with unchanged payload | none | Ignore as duplicate |
| Older event timestamp | none | Reject as out of order; increment diagnostic counter |

If no valid exchange event timestamp exists, use receive monotonic time for
ordering. A VTT reset must not generate a negative rate, positive spike, or
aggressor classification. Expiry/date reset clears all per-instrument
baselines and histories. Queue overflow or worker data loss invalidates
quote-side classification for the affected rolling window; it must be
observable in snapshot status.

### Step 3: [Cross-cutting] Implement 20s/40s/60s volume aggregation

- **Requirements**: REQ-004
- **Design inputs**: Near-ATM, Gaussian-weighted CE/PE volume velocity and
  same-time-of-day baseline definitions from `VOLUME_FLOW_EXHAUSTION_DESIGN.md`.
- **Description**: For each configured horizon `h ∈ {20,40,60}`, calculate
  per-instrument cumulative-volume change against the latest valid baseline
  at or immediately before `now-h`; divide by the actual elapsed time from
  that baseline to `now`. Require the boundary overshoot and data freshness
  to remain within configured tolerances. This avoids claiming an exact
  20-second interval when the last sample is older than the cutoff.

For eligible contract `i`:

```text
delta_i(h) = max(V_i(now) - V_i(base_h), 0)
elapsed_i(h) = now - time(base_h)
rate_i(h) = delta_i(h) / elapsed_i(h)

w_i = exp(-0.5 * ((strike_i - spot) / sigma_K)^2)
sigma_K = 2 * strike_interval

CallVolumeVelocity_h = sum(w_i * rate_i(h) for fresh CE contracts)
PutVolumeVelocity_h  = sum(w_i * rate_i(h) for fresh PE contracts)
TotalVolumeVelocity_h = CallVolumeVelocity_h + PutVolumeVelocity_h
CallPutImbalance_h =
    (CallVolumeVelocity_h - PutVolumeVelocity_h)
    / (TotalVolumeVelocity_h + epsilon)
```

Use only current active-expiry option contracts within ATM ±4 strike
intervals. Exclude stale contracts. Require at least two valid legs per side
and at least 60% of the expected configured weight per side. An unavailable
strike interval, stale spot, missing baseline, excessive horizon-boundary
overshoot, or insufficient side coverage makes the affected horizon
unavailable.

Relative volume is supported only when a same-time-of-day prior-session
baseline is supplied to the engine:

```text
RelativeVolume_h =
    (TotalVolumeVelocity_h + epsilon)
    / (HistoricalMedianTotalVolumeVelocity_h + epsilon)
```

Define an optional standard-library `BaselineProvider` protocol or immutable
baseline input to the constructor. Do not invent historical values or use a
current-session rolling average as if it were the prior-session baseline. If
no baseline is injected, set only relative-volume fields to unavailable while
keeping raw CE/PE rates usable.

### Step 4: [Cross-cutting] Implement quote-side flow, spot metrics, and immutable snapshots

- **Requirements**: REQ-005, REQ-006
- **Design inputs**: Upstox TBT quote-side classification and spot formulas
  from `VOLUME_FLOW_EXHAUSTION_DESIGN.md`.
- **Description**: Compute signed flow only from valid quote-side data.
  Compute spot metrics from canonical spot samples using actual timestamps.
  Publish consistent immutable snapshots under a short lock.

#### Aggressor flow

For positive VTT increment `dv`, valid uncrossed quotes where
`best_bid < best_ask`:

```text
buy_increment  = dv when LTP >= best_ask
sell_increment = dv when LTP <= best_bid
unclassified   = dv otherwise

aggressor_imbalance_h =
    (buy_volume_h - sell_volume_h)
    / (buy_volume_h + sell_volume_h + epsilon)

classified_coverage_h =
    (buy_volume_h + sell_volume_h)
    / (buy_volume_h + sell_volume_h + unclassified_volume_h + epsilon)
```

Do not double-classify locked/crossed/invalid quotes. Require at least 50%
classified coverage before flow is eligible for downstream confirmation.
Keep future aggressor imbalance primary; option-side aggressor flow is a
secondary proxy.

#### Spot metrics

For canonical NIFTY index spot `S`:

```text
spot_return_h_bp = 10,000 * ln(S(now) / S(now-h))
spot_velocity_h = spot_return_h_bp / h
spot_acceleration_20_40 =
    (spot_velocity_20 - spot_velocity_40) / 10
spot_acceleration_40_60 =
    (spot_velocity_40 - spot_velocity_60) / 10
```

Expose 60-second realized volatility from valid timestamped spot returns.
Never use an option underlying field, index close, OHLC candle, or restored
stale spot as a current tick. Require maximum age 3 seconds for index/future
inputs and 5 seconds for option inputs.

#### Queue and thread safety

- Use `queue.Queue(maxsize=100_000)` initially, configurable on construction.
- Public ingestion uses `put_nowait`; it must never block the Upstox callback.
- On `queue.Full`, do not silently drop or coalesce quote-flow ticks. Increment
  a thread-safe drop counter, mark the engine degraded, and ensure the
  snapshot reports the loss. Do not emit trusted signed flow until an
  uninterrupted valid window is rebuilt.
- One worker thread is the exclusive writer for instrument metadata,
  histories, VTT baselines, and derived metrics.
- `queue.Queue` provides producer/consumer synchronization. A dedicated
  `threading.Lock` protects the published snapshot and health counters only;
  never hold it while waiting on the queue or doing horizon calculations.
- Worker loop uses a timed `get`, processes control and data records in
  order, and calls `task_done` in `finally`. Stop signals the worker and
  drains/finishes queued work within a bounded join timeout; no sentinel must
  be forced into a full queue.
- `start()` is idempotent. `stop()` is safe if not started or already stopped.
  Worker exceptions must be logged with traceback and reflected in health
  status; malformed individual input must not terminate the worker.
- Snapshot returns a frozen value object (and immutable tuples, not internal
  deques/dicts). Snapshot reader lock duration is limited to copying or
  retrieving the immutable reference.
- Inject a monotonic clock for deterministic unit tests. Keep event-time
  normalization/order distinct from monotonic elapsed-time calculations.

### Step 5: [Cross-cutting] Complete unit coverage and module verification

- **Requirements**: REQ-001 through REQ-007
- **Design inputs**: All Phase-1 implementation steps.
- **Description**: Run the standalone module test suite after each coherent
  increment. Tests cover no external services and no live Upstox credentials.
  Phase 2, Phase 3, API, Redis, and UI tests are out of scope.

#### Concrete task breakdown

- [ ] T001 [Plan:1.1] Create a standalone standard-library test harness, fake monotonic clock, instrument fixtures, and assertion helpers in `Nse_Project/TESTING/volume_flow_engine_test.py`.
- [ ] T002 [Plan:1.1] Define immutable `InstrumentSpec`, `VolumeFlowTick`, `SpotTick`, `HorizonMetrics`, and `VolumeFlowSnapshot` contracts plus the documented public API in `Nse_Project/Live_dashboard/volume_flow_engine.py`.
- [ ] T003 [Plan:1.2] Implement per-instrument and spot history ownership, timestamp ordering/deduplication, bounded pruning, VTT baseline transitions, and expiry/session reset handling in `Nse_Project/Live_dashboard/volume_flow_engine.py`.
- [ ] T004 [Plan:1.3] Implement active-expiry/near-ATM filtering, Gaussian strike weights, 20s/40s/60s cumulative-VTT rate calculations, boundary coverage, freshness, and optional injected relative-volume baselines in `Nse_Project/Live_dashboard/volume_flow_engine.py`.
- [ ] T005 [Plan:1.4] Implement safe bid/ask aggressor classification, classified-volume coverage, canonical-spot velocities/accelerations, and 60s realized volatility in `Nse_Project/Live_dashboard/volume_flow_engine.py`.
- [ ] T006 [Plan:1.4] Implement bounded nonblocking queue ingestion, idempotent worker lifecycle, overflow/error health state, and locked immutable snapshot publication in `Nse_Project/Live_dashboard/volume_flow_engine.py`.
- [ ] T007 [Plan:1.5] Add unit cases for VTT initialization/increase/equality/reset, volume horizons, strike weighting, expiry filtering, stale/coverage gates, missing baseline, and quote classification in `Nse_Project/TESTING/volume_flow_engine_test.py`.
- [ ] T008 [Plan:1.5] Add unit cases for spot log-return velocity/acceleration, injected-clock irregular intervals, queue-full degradation, FIFO control ordering, start/stop behavior, immutable snapshots, and concurrent producer/snapshot access in `Nse_Project/TESTING/volume_flow_engine_test.py`.
- [ ] T009 [Plan:1.5] Run the standalone suite with the repository Python interpreter and verify the module imports without Django/Redis/Upstox SDK and no existing source files were changed.

## Project Structure

```text
Nse_Project/
├── Live_dashboard/
│   └── volume_flow_engine.py       # new standalone engine (only production file in scope)
└── TESTING/
    └── volume_flow_engine_test.py  # new deterministic stdlib unit suite
```

No existing feed file, market-state schema, API view, dashboard template, or
other engine is modified by this Phase-1 module plan.

## Testing Strategy

**appType**: Standalone Python analytics module/library, not an HTTP service.  
**Critical test journeys**:

1. A registered CE/PE chain receives cumulative VTT ticks; the engine seeds
   the first sample, accumulates later deltas, and reports correctly weighted
   20/40/60-second velocities.
2. A VTT rollback, duplicate event, stale contract, expiry change, or
   out-of-order tick produces a reset/unavailable status rather than a false
   volume spike.
3. Valid quote-side observations produce correct future/option signed-flow
   imbalance and classified coverage; invalid or inside-spread observations
   remain unclassified.
4. Timestamped index spot ticks produce known log-return velocities and
   20/40/60 acceleration estimates under irregular intervals.
5. Concurrent ingestion, worker processing, stop/start, and snapshot reads
   remain bounded, nonblocking, and free from partially-mutated outputs.

**primaryValidationStack**: Standard-library `unittest` (or equivalent
standard-library test harness) with injected monotonic clock and synthetic
normalized tick records. Run directly:

```text
.venv\Scripts\python.exe Nse_Project\TESTING\volume_flow_engine_test.py
```

**fallbackMatrix**:

- `infra-tier`: No database, Redis, broker, network service, or Docker
  dependency → no infrastructure tier required.
- `browser-tier`: No HTTP/UI surface in scope → no browser tier required.

**Environment requirements**: Existing project Python interpreter; standard
library only. No live Upstox token, Redis, Docker, Node.js, or network access.

**knownGaps**: Unit tests prove deterministic engine semantics and concurrent
access behavior, but do not validate actual Upstox tick rate, wire-field
normalization, production instrument metadata, Redis/API serialization, or
live queue capacity. Those belong to a later feed-integration phase and are
explicitly excluded here.

**Test data strategy**: Synthetic `InstrumentSpec`, `VolumeFlowTick`, and
`SpotTick` values with deterministic timestamps and fake clock. Use independent
instrument keys/expiry values per test; construct a fresh engine and stop its
worker in teardown. No database or external state to clean.

**Acceptance criteria**:

- Rates use cumulative VTT deltas divided by actual baseline elapsed seconds;
  they do not scale VTT by option lot size.
- First observation yields no artificial volume; a rollback clears/reseeds
  history and yields no negative delta/spike.
- Each horizon is unavailable unless its history span, cutoff quality,
  freshness, expiry, and coverage satisfy configured gates.
- Invalid quote data never becomes signed directional flow.
- Spot velocity and acceleration match analytic fixtures within documented
  floating-point tolerance.
- Ingestion never blocks when the queue is full; loss is visible and
  signed-flow readiness is degraded.
- Snapshot objects do not expose mutable internal buffers.
- Engine works without Django, Redis, Upstox SDK, pandas, or external
  dependencies.
- Existing six sentiment formulas and project source remain untouched.

**Validation review expectations**: Review formulas, units, boundary and
freshness conditions, VTT reset behavior, queue-overflow semantics, lock
ownership, and test evidence. Require zero failing tests and no modification
outside the two new files in the scoped implementation.

## Requirement Mapping

| REQ ID | Description | Plan Items | Implementation Evidence |
|---|---|---|---|
| REQ-001 | Keep Phase-1 isolated to the new engine module; no existing formulas or integration | 1.1, 1.5 | `Nse_Project/Live_dashboard/volume_flow_engine.py`; no existing production file changes |
| REQ-002 | Define module classes, dataclasses, and public API | 1.1 | `InstrumentSpec`, `VolumeFlowTick`, `SpotTick`, `HorizonMetrics`, `VolumeFlowSnapshot`, `VolumeFlowEngine` |
| REQ-003 | Use bounded histories and robust cumulative VTT delta tracking/resets | 1.2 | `InstrumentSample`, per-instrument history, VTT reset/dedup unit tests |
| REQ-004 | Compute 20s/40s/60s CE/PE weighted rates with correct coverage/freshness | 1.3 | Horizon calculation methods and volume-rate unit tests |
| REQ-005 | Use bounded queue, worker lifecycle, thread-safe immutable output | 1.4 | Queue/worker/snapshot design and overflow/concurrency tests |
| REQ-006 | Provide quote-side flow and spot velocity/acceleration outputs | 1.4 | Aggressor/spot calculations in module and synthetic tests |
| REQ-007 | Provide a deterministic unit-test plan and runnable validation | 1.5 | `Nse_Project/TESTING/volume_flow_engine_test.py` and direct interpreter command |
