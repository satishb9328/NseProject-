"""Print the nearest tradable NIFTY index-future instrument key from Upstox."""

from datetime import date
import gzip
import io

import pandas as pd
import requests


MASTER_INSTRUMENTS_URL = (
	"https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
)
HEADERS = {
	"User-Agent": (
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
		"(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
	),
}


def find_nearest_nifty_future() -> tuple[str, str, date]:
	response = requests.get(MASTER_INSTRUMENTS_URL, headers=HEADERS, timeout=30)
	response.raise_for_status()
	contracts = pd.read_csv(io.BytesIO(gzip.decompress(response.content)))

	required_columns = {"instrument_key", "tradingsymbol", "name", "instrument_type", "expiry"}
	missing_columns = required_columns.difference(contracts.columns)
	if missing_columns:
		raise RuntimeError(f"Upstox master file is missing columns: {sorted(missing_columns)}")

	nifty_futures = contracts.loc[
		contracts["instrument_type"].astype(str).str.upper().eq("FUTIDX")
		& contracts["name"].astype(str).str.upper().eq("NIFTY")
	].copy()
	nifty_futures["expiry"] = pd.to_datetime(nifty_futures["expiry"], errors="coerce")
	nifty_futures = nifty_futures.dropna(subset=["instrument_key", "expiry"])
	nifty_futures = nifty_futures[nifty_futures["expiry"].dt.date >= date.today()]

	if nifty_futures.empty:
		raise RuntimeError("No unexpired NIFTY FUTIDX contracts were found in the Upstox master file.")

	current_future = nifty_futures.sort_values("expiry").iloc[0]
	return (
		str(current_future["instrument_key"]),
		str(current_future["tradingsymbol"]),
		current_future["expiry"].date(),
	)


if __name__ == "__main__":
	instrument_key, symbol, expiry = find_nearest_nifty_future()
	print("Instrument Key:", instrument_key)
	print("Symbol:", symbol)
	print("Expiry:", expiry)
