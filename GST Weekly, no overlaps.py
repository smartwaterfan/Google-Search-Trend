#!/usr/bin/env python3
# De-overlap GST hits (>=85): enforce >= 21 days (exactly 3 weeks or more)
# between anchor week starts (the middle week of the 3-week window).
# Input : "GST Data/Weekly filtered over 85, with overlaps/<TICKER>_<YEAR>_th85.csv"
# Output: "GST Data/Weekly filtered over 85, no overlaps/<TICKER>_<YEAR>_th85_no_overlap.csv"

from __future__ import annotations
from pathlib import Path
from typing import List, Dict
import pandas as pd

# ----- Settings -----
TICKERS = [
    "TSLA","NVDA","AAPL","MSFT","AMZN",
    "GOOGL","NFLX","ADBE","NKE","SBUX",
    "INTC","TTWO","QCOM","GME","EBAY"
]
YEARS = list(range(2014, 2020))   # 2014..2019 inclusive
THRESHOLD = 85
MIN_GAP_DAYS = 21                 # *** exactly 3 weeks or more between anchor starts ***

BASE = Path("GST Data")
IN_DIR  = BASE / "Weekly filtered over 85, with overlaps"
OUT_DIR = BASE / "Weekly filtered over 85, no overlaps"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ----- IO helpers -----
def read_one(ticker: str, year: int) -> pd.DataFrame:
    path = IN_DIR / f"{ticker}_{year}_th{THRESHOLD}.csv"
    if not path.exists():
        return pd.DataFrame(columns=["Week", ticker, "isPartial"])

    df = pd.read_csv(path, encoding="utf-8-sig")
    if "Week" not in df.columns:
        return pd.DataFrame(columns=["Week", ticker, "isPartial"])

    # locate the value column (usually the ticker name)
    value_col = next((c for c in df.columns if c.lower() not in {"week", "ispartial"}), None)
    if value_col is None:
        return pd.DataFrame(columns=["Week", ticker, "isPartial"])

    out = pd.DataFrame({
        "Week": pd.to_datetime(df["Week"], errors="coerce"),
        ticker: pd.to_numeric(df[value_col], errors="coerce").round(0).astype("Int64"),
        "isPartial": (
            df["isPartial"].astype(str).str.lower().map({"true": True, "false": False})
            if "isPartial" in df.columns else False
        )
    }).dropna(subset=["Week"])

    # constrain to the year (safety)
    out = out[out["Week"].dt.year.eq(year)].reset_index(drop=True)
    return out

def write_split_by_year(kept: pd.DataFrame, ticker: str) -> None:
    for yr in YEARS:
        out_path = OUT_DIR / f"{ticker}_{yr}_th{THRESHOLD}_no_overlap.csv"
        sub = kept[kept["Week"].dt.year.eq(yr)][["Week", ticker, "isPartial"]].copy()
        if sub.empty:
            pd.DataFrame(columns=["Week", ticker, "isPartial"]).to_csv(out_path, index=False, encoding="utf-8")
        else:
            sub["Week"] = sub["Week"].dt.date.astype(str)
            sub[ticker] = sub[ticker].astype("Int64")
            sub.to_csv(out_path, index=False, encoding="utf-8")

# ----- De-overlap: >= 21-day spacing between anchor starts -----
def enforce_no_overlap(all_hits: pd.DataFrame) -> pd.DataFrame:
    if all_hits.empty:
        return all_hits
    df = (all_hits
          .sort_values("Week")
          .drop_duplicates(subset=["Week"])
          .reset_index(drop=True))
    keep_idx: List[int] = []
    last_kept = None
    for i, dt in enumerate(df["Week"]):
        # keep if this anchor starts >= 21 days after the last kept anchor
        if last_kept is None or (dt - last_kept).days >= MIN_GAP_DAYS:
            keep_idx.append(i)
            last_kept = dt
        # else drop (too close)
    return df.loc[keep_idx].reset_index(drop=True)

# ----- Main -----
def main():
    total_in_all, total_kept_all = 0, 0

    for tkr in TICKERS:
        per_year_in: Dict[int, int] = {yr: 0 for yr in YEARS}
        frames = []
        for yr in YEARS:
            df = read_one(tkr, yr)
            per_year_in[yr] = len(df)
            if not df.empty:
                frames.append(df)

        if not frames:
            for yr in YEARS:
                (pd.DataFrame(columns=["Week", tkr, "isPartial"])
                 .to_csv(OUT_DIR / f"{tkr}_{yr}_th{THRESHOLD}_no_overlap.csv", index=False, encoding="utf-8"))
                print(f"[{tkr} {yr}] kept 0 of {per_year_in[yr]} (removed 0) — 3-week spacing")
            print(f"[{tkr} TOTAL] kept 0 of 0 (removed 0)")
            continue

        all_hits = pd.concat(frames, ignore_index=True)
        kept = enforce_no_overlap(all_hits)
        total_in_all  += len(all_hits)
        total_kept_all += len(kept)

        write_split_by_year(kept, tkr)

        per_year_kept = kept["Week"].dt.year.value_counts().to_dict()
        kept_total = 0
        in_total   = 0
        for yr in YEARS:
            kept_count = per_year_kept.get(yr, 0)
            in_count   = per_year_in[yr]
            removed    = max(in_count - kept_count, 0)
            kept_total += kept_count
            in_total   += in_count
            print(f"[{tkr} {yr}] kept {kept_count} of {in_count} (removed {removed}) — 3-week spacing")

        removed_total = max(in_total - kept_total, 0)
        print(f"[{tkr} TOTAL] kept {kept_total} of {in_total} (removed {removed_total})")

    print(f"\nDone. Output folder: {OUT_DIR.resolve()}")
    print(f"Kept {total_kept_all} of {total_in_all} total hits across all tickers "
          f"(removed {max(total_in_all - total_kept_all, 0)}).")

if __name__ == "__main__":
    main()
