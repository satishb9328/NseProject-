import pandas as pd


def _safe_float(val) -> float:
    if val is None:
        return 0.0
    try:
        if isinstance(val, str):
            val = val.strip()
        return float(val)
    except Exception:
        return 0.0


def filter_combo_setups(scanner_data):
    """Categorize scanner output into the required combo buckets.

    Expected scanner_data is either a list of dicts or a pandas DataFrame.
    """
    if scanner_data is None:
        return {
            "PERFECT_COMBO": [],
            "STRONG_SEPA": [],
            "SMC_ACTIVE": [],
        }

    if isinstance(scanner_data, pd.DataFrame):
        scanner_data = scanner_data.where(pd.notnull(scanner_data), None).to_dict(orient="records")

    categories = {
        "PERFECT_COMBO": [],
        "STRONG_SEPA": [],
        "SMC_ACTIVE": [],
    }

    for row in scanner_data:
        if not isinstance(row, dict):
            continue

        rs = _safe_float(row.get("rs_vs_index") or row.get("RS") or row.get("RSI"))
        vcp = _safe_float(row.get("vcp"))
        prx = _safe_float(row.get("proximity") or row.get("Prx") or row.get("Prx"))
        smc_signal = str(row.get("smc_signal") or row.get("SMC") or "NO_SETUP").strip().upper()

        perfect_combo = rs >= 10 and vcp >= 10 and prx >= 10 and smc_signal != "NO_SETUP"
        strong_sepa = rs >= 10 and vcp >= 10 and prx >= 10 and smc_signal == "NO_SETUP"
        smc_active = (rs < 10 or vcp < 10 or prx < 10) and smc_signal != "NO_SETUP"

        if perfect_combo:
            categories["PERFECT_COMBO"].append(row)
        elif strong_sepa:
            categories["STRONG_SEPA"].append(row)
        elif smc_active:
            categories["SMC_ACTIVE"].append(row)

    return categories
