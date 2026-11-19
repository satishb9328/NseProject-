"""Statistical significance: binomial, t-test, bootstrap CI, correction.

Implemented with stdlib + numpy only (no scipy): the t-test uses the
regularized incomplete beta function via a continued fraction (Numerical
Recipes), and the binomial test uses the normal approximation.
"""

from __future__ import annotations

import math

import numpy as np


def _norm_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def binomial_p_value(successes: int, n: int, p0: float = 0.5) -> float:
    """Two-sided binomial p-value for H0: p = p0 (normal approximation)."""
    if n <= 0:
        return float("nan")
    phat = successes / n
    se = math.sqrt(p0 * (1.0 - p0) / n)
    if se == 0:
        return 1.0
    z = (phat - p0) / se
    return min(1.0, 2.0 * (1.0 - _norm_cdf(abs(z))))


def _betacf(a: float, b: float, x: float, itmax: int = 200, eps: float = 3e-12) -> float:
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < 1e-30:
        d = 1e-30
    d = 1.0 / d
    h = d
    for m in range(1, itmax + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < 1e-30:
            d = 1e-30
        c = 1.0 + aa / c
        if abs(c) < 1e-30:
            c = 1e-30
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < 1e-30:
            d = 1e-30
        c = 1.0 + aa / c
        if abs(c) < 1e-30:
            c = 1e-30
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < eps:
            break
    return h


def _betai(a: float, b: float, x: float) -> float:
    """Regularized incomplete beta function I_x(a, b)."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    lbeta = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log(1.0 - x)
    bt = math.exp(lbeta)
    if x < (a + 1.0) / (a + b + 2.0):
        return bt * _betacf(a, b, x) / a
    return 1.0 - bt * _betacf(b, a, 1.0 - x) / b


def t_test_p_value(mean: float, std: float, n: int) -> float:
    """Two-sided one-sample t-test p-value for H0: mean = 0."""
    if n < 2 or std is None or std == 0 or math.isnan(std):
        return float("nan")
    t = mean / (std / math.sqrt(n))
    df = n - 1
    x = df / (df + t * t)
    return _betai(df / 2.0, 0.5, x)


def bootstrap_mean_ci(values, n_iter: int = 2000, ci: float = 0.95, rng=None) -> tuple:
    """Bootstrap confidence interval for the mean (vectorized, chunked)."""
    values = np.asarray(values, dtype=float)
    values = values[~np.isnan(values)]
    n = values.size
    if n == 0:
        return (float("nan"), float("nan"))
    rng = rng or np.random.default_rng()
    chunk = min(n_iter, 500)
    means = np.empty(n_iter, dtype=float)
    done = 0
    while done < n_iter:
        k = min(chunk, n_iter - done)
        idx = rng.integers(0, n, size=(k, n))
        means[done:done + k] = values[idx].mean(axis=1)
        done += k
    lo = float(np.percentile(means, (1.0 - ci) / 2.0 * 100.0))
    hi = float(np.percentile(means, (1.0 + ci) / 2.0 * 100.0))
    return (lo, hi)


def annotate(rows: list, config: dict) -> list:
    """Add p-values, CIs, significance and low-sample flags to metric rows."""
    sig = config.get("significance", {})
    alpha = float(sig.get("alpha", 0.05))
    correction = sig.get("multiple_testing", "bonferroni")
    n_iter = int(sig.get("bootstrap_iterations", 2000))
    ci = float(sig.get("bootstrap_ci", 0.95))
    sampling = config.get("sampling", {})
    min_n = int(sampling.get("min_sample_size", 100))
    min_succ = int(sampling.get("min_successes", 30))
    min_fail = int(sampling.get("min_failures", 30))

    for row in rows:
        trials = row["n_success"] + row["n_failure"]
        row["p_binomial"] = binomial_p_value(row["n_success"], trials) if trials > 0 else float("nan")

    n_tests = max(1, sum(1 for r in rows if r["p_binomial"] == r["p_binomial"]))

    for row in rows:
        p = row["p_binomial"]
        if correction == "bonferroni" and p == p:
            row["p_binomial_adj"] = min(1.0, p * n_tests)
        else:
            row["p_binomial_adj"] = p

        directed = row.pop("_directed", None)
        if directed is not None and directed.size >= 2:
            mean = float(np.mean(directed))
            std = float(np.std(directed, ddof=1))
            row["p_ttest"] = t_test_p_value(mean, std, int(directed.size))
            row["ci_low"], row["ci_high"] = bootstrap_mean_ci(directed, n_iter=n_iter, ci=ci)
        else:
            row["p_ttest"] = float("nan")
            row["ci_low"] = float("nan")
            row["ci_high"] = float("nan")

        row["significant"] = bool(row["p_binomial_adj"] == row["p_binomial_adj"] and row["p_binomial_adj"] < alpha)
        row["low_sample"] = bool(
            row["n_calls"] < min_n or row["n_success"] < min_succ or row["n_failure"] < min_fail
        )
    return rows

