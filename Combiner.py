#!/usr/bin/env python3
# Combiner.py — one CSV per stock, years stacked (year ↓), reporting
# max ABSOLUTE excess return (sign kept) AND max POSITIVE excess return,
# formatted as percents with 3 decimals, e.g., "-4.170%".
# Adds: pos_streak_days_from_max and a per-ticker summary CSV.

from __future__ import annotations
from pathlib import Path
from typing import List, Dict
import pandas as pd
from datetime import timedelta

TICKERS = [
    "TSLA","NVDA","AAPL","MSFT","AMZN",
    "GOOGL","NFLX","ADBE","NKE","SBUX",
    "INTC","TTWO","QCOM","GME","EBAY"
]
YEARS = list(range(2014, 2020))  # 2014–2019

GST_NO_OVERLAP_DIR = Path("GST Data/Weekly filtered over 85, no overlaps")
DAILY_DIR          = Path("Daily Excess Return Data")
OUT_DIR            = Path("Conjunction")
OUT_DIR.mkdir(parents=True, exist_ok=True)

EX_COL = "Excess Daily Return"   # from daily files (decimal, e.g., 0.0123 = 1.23%)
DP = 3                           # percent decimals

def _read_gst_hits(ticker: str, year: int) -> pd.DataFrame:
    p = GST_NO_OVERLAP_DIR / f"{ticker}_{year}_th85_no_overlap.csv"
    if not p.exists():
        return pd.DataFrame(columns=["Week", ticker, "isPartial"])
    df = pd.read_csv(p, encoding="utf-8-sig")
    if "Week" not in df.columns:
        return pd.DataFrame(columns=["Week", ticker, "isPartial"])
    df["Week"] = pd.to_datetime(df["Week"], errors="coerce")
    df = df.dropna(subset=["Week"])
    return df[df["Week"].dt.year.eq(year)].reset_index(drop=True)

def _window(anchor: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    # 3-week window centered on anchor week start X: [X-7, X+13] inclusive
    return anchor - timedelta(days=7), anchor + timedelta(days=13)

def _read_daily_excess_span(ticker: str,
                            start_date: pd.Timestamp,
                            end_date: pd.Timestamp) -> pd.DataFrame:
    years = {start_date.year, end_date.year}
    if end_date.year - start_date.year > 1:
        years.update(range(start_date.year, end_date.year + 1))

    frames: List[pd.DataFrame] = []
    for yr in sorted(years):
        p = DAILY_DIR / f"{ticker}_{yr}_excess.csv"
        if not p.exists():
            continue
        tmp = pd.read_csv(p)
        if "Date" not in tmp.columns or EX_COL not in tmp.columns:
            continue
        tmp["Date"] = pd.to_datetime(tmp["Date"], errors="coerce")
        tmp = tmp.dropna(subset=["Date"])
        frames.append(tmp[["Date", EX_COL]])

    if not frames:
        return pd.DataFrame(columns=["Date", EX_COL])

    df = pd.concat(frames, ignore_index=True)
    df = df[(df["Date"] >= pd.to_datetime(start_date)) & (df["Date"] <= pd.to_datetime(end_date))]
    df = df.sort_values("Date").reset_index(drop=True)
    df[EX_COL] = pd.to_numeric(df[EX_COL], errors="coerce").round(6)  # decimal
    return df.dropna(subset=[EX_COL]).reset_index(drop=True)

def _fmt_pct(x: float | None) -> str:
    if x is None:
        return ""
    return f"{x*100:.{DP}f}%"  # decimal -> percent string with symbol

def main():
    for tkr in TICKERS:
        rows: List[Dict] = []

        for yr in YEARS:
            hits = _read_gst_hits(tkr, yr)
            if hits.empty:
                continue

            for _, r in hits.iterrows():
                anchor = pd.to_datetime(r["Week"])
                w_start, w_end = _window(anchor)

                daily = _read_daily_excess_span(tkr, w_start, w_end)
                if daily.empty:
                    rows.append({
                        "ticker": tkr, "year": yr,
                        "anchor_week_start": anchor.date().isoformat(),
                        "window_start": w_start.date().isoformat(),
                        "window_end": w_end.date().isoformat(),
                        "max_abs_excess_return_date": "",
                        "max_abs_excess_return_pct": "",
                        "pos_in_window_abs": "",
                        "max_pos_excess_return_date": "",
                        "max_pos_excess_return_pct": "",
                        "pos_in_window_pos": "",
                        "pos_streak_days_from_max": "",
                        "num_trading_days": 0
                    })
                    continue

                # Largest ABSOLUTE move (keep sign)
                daily["_abs"] = daily[EX_COL].abs()
                idx_abs = daily["_abs"].idxmax()
                date_abs = pd.to_datetime(daily.loc[idx_abs, "Date"])
                val_abs_dec = float(daily.loc[idx_abs, EX_COL])
                pos_abs = int(daily.index.get_loc(idx_abs) + 1)

                # Largest POSITIVE move (if any) + streak of >=0 from that day
                pos_df = daily[daily[EX_COL] > 0]
                if not pos_df.empty:
                    idx_pos = pos_df[EX_COL].idxmax()
                    date_pos = pd.to_datetime(daily.loc[idx_pos, "Date"])
                    val_pos_dec = float(daily.loc[idx_pos, EX_COL])
                    pos_pos = int(daily.index.get_loc(idx_pos) + 1)

                    # count consecutive non-negative days starting at the max-positive day
                    streak = 0
                    for v in daily.loc[idx_pos:, EX_COL].values:
                        if pd.notna(v) and v >= 0:
                            streak += 1
                        else:
                            break

                    date_pos_str = date_pos.date().isoformat()
                    val_pos_pct_str = _fmt_pct(val_pos_dec)
                    pos_pos_out = pos_pos
                    streak_out = streak
                else:
                    date_pos_str = ""
                    val_pos_pct_str = ""
                    pos_pos_out = ""
                    streak_out = ""

                rows.append({
                    "ticker": tkr,
                    "year": yr,
                    "anchor_week_start": anchor.date().isoformat(),
                    "window_start": w_start.date().isoformat(),
                    "window_end": w_end.date().isoformat(),
                    "max_abs_excess_return_date": date_abs.date().isoformat(),
                    "max_abs_excess_return_pct": _fmt_pct(val_abs_dec),
                    "pos_in_window_abs": pos_abs,
                    "max_pos_excess_return_date": date_pos_str,
                    "max_pos_excess_return_pct": val_pos_pct_str,
                    "pos_in_window_pos": pos_pos_out,
                    "pos_streak_days_from_max": streak_out,
                    "num_trading_days": int(len(daily))
                })

        out = OUT_DIR / f"{tkr}_conjunction.csv"
        if not rows:
            pd.DataFrame(columns=[
                "ticker","year","anchor_week_start","window_start","window_end",
                "max_abs_excess_return_date","max_abs_excess_return_pct","pos_in_window_abs",
                "max_pos_excess_return_date","max_pos_excess_return_pct","pos_in_window_pos",
                "pos_streak_days_from_max","num_trading_days"
            ]).to_csv(out, index=False, encoding="utf-8")
            print(f"[{tkr}] wrote 0 rows → {out.name}")
            continue

        df_all = pd.DataFrame(rows)
        df_all["anchor_week_start"] = pd.to_datetime(df_all["anchor_week_start"])
        df_all = df_all.sort_values(["year", "anchor_week_start"], ascending=[False, True])
        df_all["anchor_week_start"] = df_all["anchor_week_start"].dt.date.astype(str)
        df_all.to_csv(out, index=False, encoding="utf-8")
        print(f"[{tkr}] wrote {len(df_all)} rows → {out.name}")

        # --- per-ticker summary with two averages ---
        pos_series = pd.to_numeric(df_all["pos_in_window_pos"], errors="coerce").dropna()
        avg_pos = float(pos_series.mean()) if len(pos_series) else None

        streak_series = pd.to_numeric(df_all["pos_streak_days_from_max"], errors="coerce")
        streak_series = streak_series[streak_series > 0].dropna()
        avg_streak = float(streak_series.mean()) if len(streak_series) else None

        summary = pd.DataFrame([{
            "ticker": tkr,
            "avg_pos_in_window_pos": avg_pos,                  # average index (1–15)
            "avg_pos_streak_days_from_max": avg_streak         # average consecutive non-negative days
        }])
        summary_path = OUT_DIR / f"{tkr}_conjunction_summary.csv"
        summary.to_csv(summary_path, index=False, encoding="utf-8")

    print("\nDone. Combined files saved in:", OUT_DIR.resolve())

if __name__ == "__main__":
    main()
