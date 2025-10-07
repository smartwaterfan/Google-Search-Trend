#!/usr/bin/env python3
# GST Weekly Sorting over 85 (with overlaps)
# Reads:  "GST Data/Weekly Data/<TICKER>_<YEAR>.csv"  (columns: Week,<TICKER>,isPartial)
# Writes: "GST Data/Weekly filtered, w/ overlaps/<TICKER>_<YEAR>_th85.csv"
# Keeps the same 3 columns, just filtered to GST >= THRESHOLD.

from __future__ import annotations
from pathlib import Path
import pandas as pd

# ---------- Settings ----------
TICKERS = [
    "TSLA","NVDA","AAPL","MSFT","AMZN",
    "GOOGL","NFLX","ADBE","NKE","SBUX",
    "INTC","TTWO","QCOM","GME","EBAY"
]

YEARS = list(range(2014, 2020))
THRESHOLD = 85

IN_DIR  = Path("GST Data") / "Weekly Data"
OUT_DIR = Path("GST Data") / "Weekly filtered over 85, with overlaps"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Helpers ----------
def _normalize_label(s: str) -> str:
    s = str(s).strip()
    s = s.split(":", 1)[0]
    s = s.split("(", 1)[0]
    return s.strip().lower()

def _pick_value_column(df: pd.DataFrame, term: str) -> str | None:
    """Find the column that holds the values for this ticker (be forgiving)."""
    cols = [c for c in df.columns if c.lower() not in {"week", "ispartial"}]
    # exact
    for c in cols:
        if c.lower() == term.lower():
            return c
    # normalized
    tgt = _normalize_label(term)
    for c in cols:
        if _normalize_label(c) == tgt:
            return c
    # contains
    for c in cols:
        if tgt in _normalize_label(c):
            return c
    return cols[0] if cols else None

def filter_one_year(ticker: str, year: int) -> pd.DataFrame:
    path = IN_DIR / f"{ticker}_{year}.csv"
    if not path.exists():
        print(f"[{ticker} {year}] MISSING input: {path}")
        return pd.DataFrame(columns=["Week", ticker, "isPartial"])

    df = pd.read_csv(path, encoding="utf-8-sig")
    if "Week" not in df.columns:
        print(f"[{ticker} {year}] malformed CSV (no 'Week'): {path.name}")
        return pd.DataFrame(columns=["Week", ticker, "isPartial"])

    val_col = _pick_value_column(df, ticker)
    if not val_col:
        print(f"[{ticker} {year}] could not find value column in {path.name}")
        return pd.DataFrame(columns=["Week", ticker, "isPartial"])

    # Parse dates once (datetime64); keep a separate date-only version for output
    weeks_dt = pd.to_datetime(df["Week"], errors="coerce")
    weeks_out = weeks_dt.dt.date

    # Values and partial flags
    vals = pd.to_numeric(df[val_col].astype(str).replace("<1", "0"), errors="coerce").fillna(0).round(0).astype(int)
    part = (df["isPartial"].astype(str).str.lower().map({"true": True, "false": False})
            if "isPartial" in df.columns else pd.Series([False] * len(df)))

    # Keep exactly the requested year (FIX: use .dt.year)
    mask_year = weeks_dt.dt.year.eq(year)
    weeks_out, vals, part = weeks_out[mask_year], vals[mask_year], part[mask_year]

    # Filter by threshold
    keep = vals >= THRESHOLD
    if int(keep.sum()) == 0:
        return pd.DataFrame(columns=["Week", ticker, "isPartial"])

    return pd.DataFrame({
        "Week": weeks_out[keep].astype(str).values,
        ticker: vals[keep].astype(int).values,
        "isPartial": part[keep].astype(bool).values
    })

# ---------- Main ----------
def main():
    total_files = 0
    total_rows = 0

    for tkr in TICKERS:
        for yr in YEARS:
            filtered = filter_one_year(tkr, yr)
            out_path = OUT_DIR / f"{tkr}_{yr}_th{THRESHOLD}.csv"

            if filtered.empty:
                # Always write a header so you end up with 6 per ticker
                pd.DataFrame(columns=["Week", tkr, "isPartial"]).to_csv(out_path, index=False, encoding="utf-8")
                print(f"[{tkr} {yr}] no weeks ≥ {THRESHOLD}; wrote header-only → {out_path.name}")
            else:
                filtered.to_csv(out_path, index=False, encoding="utf-8")
                print(f"[{tkr} {yr}] kept {len(filtered)} rows ≥ {THRESHOLD} → {out_path.name}")
                total_rows += len(filtered)

            total_files += 1

    print(f"\nDone. Wrote {total_files} files to: {OUT_DIR.resolve()}")
    print(f"Total filtered rows across all files: {total_rows}")

if __name__ == "__main__":
    main()
