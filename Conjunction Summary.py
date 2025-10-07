#!/usr/bin/env python3
# Overall Conjunction Summary.py
# Build one summary across all tickers from the per-event conjunction files.

from __future__ import annotations
from pathlib import Path
import pandas as pd

CONJ_DIR = Path("Conjunction")
OUTFILE  = CONJ_DIR / "----OVERALL CONJUNCTION SUMMARY----.csv"

def main():
    CONJ_DIR.mkdir(parents=True, exist_ok=True)
    rows = []

    # Use the per-event files, NOT the *_summary.csv ones
    for f in sorted(CONJ_DIR.glob("*_conjunction.csv")):
        ticker = f.stem.replace("_conjunction", "")
        try:
            df = pd.read_csv(f)
        except Exception:
            continue
        if df.empty:
            rows.append({"ticker": ticker,
                         "avg_pos_in_window_pos": None,
                         "avg_pos_streak_days_from_max": None})
            continue

        # numeric, ignore NaN; for streaks, ignore non-positive (0/blank)
        pos = pd.to_numeric(df.get("pos_in_window_pos"), errors="coerce").dropna()
        streak = pd.to_numeric(df.get("pos_streak_days_from_max"), errors="coerce")
        streak = streak[streak > 0].dropna()

        avg_pos = float(pos.mean()) if len(pos) else None
        avg_streak = float(streak.mean()) if len(streak) else None

        rows.append({
            "ticker": ticker,
            "avg_pos_in_window_pos": round(avg_pos, 2) if avg_pos is not None else None,
            "avg_pos_streak_days_from_max": round(avg_streak, 2) if avg_streak is not None else None,
        })

    # Write the overall table (even if empty, with headers)
    out_df = pd.DataFrame(rows, columns=[
        "ticker", "avg_pos_in_window_pos", "avg_pos_streak_days_from_max"
    ]).sort_values("ticker")
    out_df.to_csv(OUTFILE, index=False, encoding="utf-8")
    print(f"Wrote {len(out_df)} rows → {OUTFILE}")

if __name__ == "__main__":
    main()
