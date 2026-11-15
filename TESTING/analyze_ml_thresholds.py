"""Read-only analysis of D:/Nifty_ML_Data to discover the exact ofi_imbalance,
volume_delta, CVD-slope and vwap_distance thresholds preceding target==1
(high-probability NIFTY moves) on 2-minute bars."""
import glob
import os

import numpy as np
import pandas as pd

DATA_DIR = r"D:\Nifty_ML_Data"
OHLC_PATH = os.path.join(DATA_DIR, "nifty_fut_1m_ohlc_2026_08_10.parquet")


def load_ml():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "nifty_features_2min_*.parquet")))
    if not files:
        raise FileNotFoundError("no nifty_features_2min_*.parquet in " + DATA_DIR)
    df = pd.concat([pd.read_parquet(f) for f in files]).sort_index()
    df.index = pd.to_datetime(df.index, utc=True)
    n0 = len(df)
    df = df[~df.index.duplicated(keep="last")]
    print(f"[LOAD] {len(files)} ML files -> {n0} rows -> {len(df)} unique 2m bars")
    return df


def load_ohlc_2m():
    if not os.path.exists(OHLC_PATH):
        print("[LOAD] OHLC cache missing - skipping price join")
        return pd.DataFrame()
    o = pd.read_parquet(OHLC_PATH)
    o.index = pd.to_datetime(o.index, utc=True)
    ist = o.index.tz_convert("Asia/Kolkata")
    m = (ist.time >= pd.to_datetime("09:15").time()) & (ist.time < pd.to_datetime("15:30").time())
    o = o[m]
    o2 = (o.resample("2min", label="left", closed="left")
            .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
            .dropna(subset=["open", "close"]))
    print(f"[LOAD] OHLC {len(o)} 1m -> {len(o2)} 2m bars (09:15-15:30 IST)")
    return o2[["open", "high", "low", "close"]]


def fwd_max_fav(close, high, low, dates, K):
    """Direction-agnostic max favourable move (pts) over next K bars, same day."""
    n = len(close)
    out = np.full(n, np.nan)
    i = 0
    while i < n:
        d = dates[i]
        j = i
        while j < n and dates[j] == d:
            j += 1
        for k in range(i, j):
            e = min(k + K, j - 1)
            if k + 1 <= e:
                seg_h = high[k + 1:e + 1]
                seg_l = low[k + 1:e + 1]
                if np.isnan(seg_h).all() or np.isnan(seg_l).all():
                    continue
                out[k] = max(float(np.nanmax(seg_h)) - close[k],
                             close[k] - float(np.nanmin(seg_l)))
        i = j
    return out


def build():
    df = load_ml()
    ohlc = load_ohlc_2m()
    if not ohlc.empty:
        df = df.join(ohlc, how="left")
    ist = df.index.tz_convert("Asia/Kolkata")
    df["ist_date"] = ist.date
    df["ist_time"] = ist.time
    df = df.drop(columns=["oi_change", "prev_oi_change"], errors="ignore")
    df["cvd"] = df.groupby("ist_date")["volume_delta"].cumsum()
    df["cvd_change_3"] = df.groupby("ist_date")["cvd"].diff(3)
    df["cvd_aligned"] = np.sign(df["volume_delta"]) == np.sign(df["cvd_change_3"])
    df["abs_ofi_imbalance"] = df["ofi_imbalance"].abs()
    df["abs_volume_delta"] = df["volume_delta"].abs()
    df["abs_vwap_distance"] = df["vwap_distance"].abs()
    df["abs_ofi"] = df["ofi"].abs()
    df["abs_cvd_change_3"] = df["cvd_change_3"].abs()
    df["is_midday_window"] = (df["ist_time"] >= pd.to_datetime("10:30").time()) & (
        df["ist_time"] <= pd.to_datetime("13:00").time())
    if {"open", "high", "low", "close"}.issubset(df.columns):
        c = df["close"].to_numpy(float)
        h = df["high"].to_numpy(float)
        l = df["low"].to_numpy(float)
        dt = df["ist_date"].to_numpy()
        df["fwd3_fav"] = fwd_max_fav(c, h, l, dt, 3)
        df["fwd5_fav"] = fwd_max_fav(c, h, l, dt, 5)
    print(f"[JOIN] {len(df)} rows | {df['ist_date'].nunique()} days | baseline target==1 = {df['target'].mean()*100:.2f}%")
    return df


def part_a(df):
    cols = ["abs_ofi_imbalance", "abs_volume_delta", "abs_cvd_change_3",
            "abs_vwap_distance", "abs_ofi", "fwd3_fav", "fwd5_fav"]
    cols = [c for c in cols if c in df.columns]
    rows = []
    for c in cols:
        r = {"feature": c}
        for t in (0, 1):
            q = df.loc[df["target"] == t, c].dropna().quantile([0.5, 0.75, 0.9])
            r[f"t{t}_p50"] = q[0.5]
            r[f"t{t}_p75"] = q[0.75]
            r[f"t{t}_p90"] = q[0.9]
        rows.append(r)
    tab = pd.DataFrame(rows).set_index("feature")
    print("\n" + "=" * 90)
    print("PART A - percentile distribution: target==0 vs target==1")
    print("=" * 90)
    n0 = int((df["target"] == 0).sum())
    n1 = int((df["target"] == 1).sum())
    print(f"target==0 n={n0}   |   target==1 n={n1}")
    def _num(x):
        if x != x:
            return "nan"
        if 0 < abs(x) < 0.01:
            return f"{x:.6f}"
        return f"{x:,.2f}"
    with pd.option_context("display.float_format", _num, "display.width", 250):
        print(tab.to_string())
def part_b(df):
    feats = ["candle_buy_vol", "candle_sell_vol", "volume_delta", "ofi", "ofi_imbalance",
             "vwap_distance", "prev_delta", "prev_ofi_imbalance", "cvd", "cvd_change_3",
             "abs_ofi_imbalance", "abs_volume_delta", "abs_vwap_distance", "abs_ofi",
             "abs_cvd_change_3", "cvd_aligned", "is_midday_window"]
    X = df[feats].copy()
    for c in ["cvd_aligned", "is_midday_window"]:
        X[c] = X[c].astype(int)
    y = df["target"].astype(int)
    pb = X.corrwith(y).abs().rename("point_biserial")
    tab = pd.DataFrame({"point_biserial": pb})
    try:
        from sklearn.ensemble import RandomForestClassifier
        rf = RandomForestClassifier(n_estimators=300, max_depth=8, random_state=42,
                                    n_jobs=-1, class_weight="balanced")
        rf.fit(X.fillna(0), y)
        tab["rf_importance"] = pd.Series(rf.feature_importances_, index=feats)
    except Exception as exc:
        print("  [RF skipped]", exc)
    tab = tab.sort_values("point_biserial", ascending=False)
    print("\n" + "=" * 90)
    print("PART B - feature importance for target==1 (sorted by |point-biserial|)")
    print("=" * 90)
    with pd.option_context("display.float_format", lambda x: f"{x:,.4f}", "display.width", 250):
        print(tab.to_string())


def part_c(df):
    baseline = float(df["target"].mean() * 100.0)
    ofi_thrs = [0.15, 0.25, 0.35, 0.45]
    vd_thrs = [500_000, 1_000_000, 1_500_000, 2_000_000]
    aligned_opts = [True, False]
    bands = {
        "near<0.0005": lambda d: d["abs_vwap_distance"] < 0.0005,
        "mid 0.0005-0.0015": lambda d: (d["abs_vwap_distance"] >= 0.0005) & (d["abs_vwap_distance"] < 0.0015),
        "far>=0.0015": lambda d: d["abs_vwap_distance"] >= 0.0015,
        "all": lambda d: pd.Series(True, index=d.index),
    }
    res = []
    for ot in ofi_thrs:
        for vt in vd_thrs:
            for al in aligned_opts:
                for bn, bf in bands.items():
                    m = (df["abs_ofi_imbalance"] >= ot) & (df["abs_volume_delta"] >= vt)
                    m &= (df["cvd_aligned"] == al)
                    m &= bf(df)
                    s = df[m]
                    n = int(len(s))
                    if n < 25:
                        continue
                    prec = float(s["target"].mean() * 100.0)
                    mid = s[s["is_midday_window"]]
                    nm = int(len(mid))
                    pm = float(mid["target"].mean() * 100.0) if nm else float("nan")
                    fm = float(s["fwd5_fav"].median()) if "fwd5_fav" in s else float("nan")
                    res.append({"ofi_imb>=": ot, "vol_delta>=": vt, "cvd_aligned": al,
                                "vwap_band": bn, "n": n, "prec%": prec,
                                "lift": prec / baseline, "n_mid": nm,
                                "prec_mid%": pm, "med_fwd5": fm})
    tab = pd.DataFrame(res).sort_values("prec%", ascending=False).head(10).reset_index(drop=True)
    print("\n" + "=" * 100)
    print(f"PART C - top 10 threshold combos (min 25 signals; baseline target==1 = {baseline:.2f}%)")
    print("=" * 100)
    with pd.option_context("display.float_format", lambda x: f"{x:,.2f}", "display.width", 250):
        print(tab.to_string(index=False))


def main():
    df = build()
    part_a(df)
    part_b(df)
    part_c(df)


if __name__ == "__main__":
    main()

