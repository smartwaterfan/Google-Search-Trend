#!/usr/bin/env python3
# GST Weekly Data Gathering (per-year files) — US, Web Search, 2014–2019
# Writes: GST Data/Weekly Data/<TICKER>_<YEAR>.csv with columns: Week,<TICKER>,isPartial

from __future__ import annotations
from pathlib import Path
import time, random
import pandas as pd
from pytrends.request import TrendReq

# -------- Settings --------
TICKERS = [
    "TSLA","NVDA","AAPL","MSFT","AMZN",
    "GOOGL","NFLX","ADBE","NKE","SBUX",
    "INTC","TTWO","QCOM","GME","EBAY"
]

START_YEAR = 2014
END_YEAR   = 2019
YEARS = list(range(START_YEAR, END_YEAR + 1))

GEO = "US"     # United States
GPROP = ""     # Web Search
CATEGORY = 0   # All categories

# Output folder
OUTDIR = Path("GST Data") / "Weekly Data"
OUTDIR.mkdir(parents=True, exist_ok=True)

# Light pacing to reduce 429s
BETWEEN_YEARS   = (1.0, 2.0)   # delay between year requests for a ticker
BETWEEN_TICKERS = (1.5, 3.0)   # delay between tickers
BATCH_SIZE      = 6
BATCH_PAUSE_S   = 20
HARD_429_SLEEP  = 60           # on 429, cool-off

def fetch_one_year(pt: TrendReq, term: str, year: int) -> pd.DataFrame:
    """Weekly GST for a single calendar year -> DataFrame[Week, term, isPartial]."""
    timeframe = f"{year}-01-01 {year}-12-31"
    MAX_ATTEMPTS = 5
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            pt.build_payload([term], timeframe=timeframe, geo=GEO, gprop=GPROP, cat=CATEGORY)
            df = pt.interest_over_time()
            if df is None or df.empty or term not in df.columns:
                raise RuntimeError("Empty result or missing term column")

            out = pd.DataFrame({
                "Week": df.index.date,
                term: pd.to_numeric(df[term], errors="coerce").fillna(0).round(0).astype(int),
                "isPartial": df["isPartial"].astype(bool)
            })
            # Keep only weeks inside the exact year (GT can spill across edges)
            out = out[(pd.to_datetime(out["Week"]).dt.year == year)]
            return out
        except Exception as e:
            msg = str(e).lower()
            pause = HARD_429_SLEEP if ("429" in msg or ("rate" in msg and "limit" in msg) or "too many" in msg) \
                   else min(6, 2 * attempt) + random.uniform(0, 0.5)
            print(f"[{term} {year}] attempt {attempt} failed: {e} — sleeping {pause:.1f}s")
            time.sleep(pause)

    # Failure → header-only shape for this year
    return pd.DataFrame(columns=["Week", term, "isPartial"])

def main():
    print(f"Fetching WEEKLY Google Trends for {len(TICKERS)} tickers "
          f"(US, {START_YEAR}–{END_YEAR}, Web Search)…\n")

    # Important: retries=0 avoids urllib3 'method_whitelist' issue inside pytrends
    pt = TrendReq(hl="en-US", tz=0, timeout=(30, 60), retries=0, backoff_factor=0.0)

    for i, term in enumerate(TICKERS, start=1):
        total_rows = 0
        for yr in YEARS:
            out_path = OUTDIR / f"{term}_{yr}.csv"
            df = fetch_one_year(pt, term, yr)

            if df.empty:
                out_path.write_text(f"Week,{term},isPartial\n", encoding="utf-8")
                print(f"[{term}] {yr}: FAILED — wrote header-only CSV")
            else:
                df.to_csv(out_path, index=False, encoding="utf-8")
                mx = int(df[term].max())
                weeks = len(df)
                total_rows += weeks
                print(f"[{term}] {yr}: wrote {weeks} weeks → {out_path.name} (max={mx})")

            time.sleep(random.uniform(*BETWEEN_YEARS))

        print(f"[{term}] total rows across {START_YEAR}-{END_YEAR}: {total_rows}")

        # anti-burst pause between tickers
        time.sleep(random.uniform(*BETWEEN_TICKERS))
        if i % BATCH_SIZE == 0 and i != len(TICKERS):
            print(f"— small batch pause ({BATCH_PAUSE_S}s) —")
            time.sleep(BATCH_PAUSE_S)

    print("\nDone. CSVs are in:", OUTDIR.resolve())

if __name__ == "__main__":
    main()
