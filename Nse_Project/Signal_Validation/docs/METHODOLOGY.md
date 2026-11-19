# Methodology

## Question

For each signal (Score, Bias, Momentum, Gamma Boost), does its direction predict
the short-term (15/30/60/120s) NIFTY move better than chance, after costs?

## Labels

* Direction: `score >= bull_threshold` (55) → bull, `score <= bear_threshold`
  (45) → bear; `bias` / `momentum` / `gamma_boost` use `sign()`; `iv_skew` is
  inverted (falling skew → bullish).
* Outcome return: spot in index points, pinned ATM CE/PE in %.
* Economic label vs the cost buffer (spot 2 points, option 1%):
  `success` = directed return ≥ +cost; `failure` = ≤ −cost; else `neutral`
  (excluded from hit rate, counted separately).

## Metrics

* `hit_rate` = successes / (successes + failures).
* `expectancy` = mean directed return per call.
* `profit_factor` = gross profit / gross loss.
* `max_drawdown` = peak-to-trough of cumulative directed returns (raw per-call).
* `max_drawdown_cooldown` = same, on non-overlapping trades spaced by the
  horizon window, so overlapping 1-second ticks don't distort drawdown.
* `sharpe` = mean / std of directed returns.

## Significance

* Binomial test (hit rate vs 0.5), one-sample t-test (mean vs 0) via the
  regularized incomplete beta, and bootstrap confidence intervals on the mean.
* Bonferroni correction across all (signal × bucket × horizon × outcome) cells.
* Low-sample flagging below `sampling.min_sample_size` / `min_successes` /
  `min_failures`.

## Benchmarks & edge

`always_long` / `always_short` / `hold_spot` are computed per horizon on spot
moves. `edge_delta = signal hit_rate − always_long hit_rate`; positive means the
signal beats the naive long baseline. A signal whose `edge_delta ≈ 0` has no
edge over market drift, regardless of its raw hit rate.

## Splits

Chronological train/validation/test (70/15/15) with a 120s boundary buffer so a
forward horizon never crosses a split. The report shows full-sample and
out-of-sample (test) metrics.

## Limitations

* One day of trending data is not evidence; conclusions require many sessions
  across trend-up / trend-down / range / expiry-day regimes.
* Score, Bias, Momentum, and Gamma Boost are highly correlated (Score is built
  from the others), so they are not independent confirmations.
* `max_drawdown` is a cumulative per-call statistic, not a portfolio drawdown.
