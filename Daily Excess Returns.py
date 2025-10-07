#!/usr/bin/env python3
# Daily Excess Returns (yfinance) with % columns (3 dp)

from __future__ import annotations
from pathlib import Path
from typing import List
import pandas as pd
import yfinance as yf

TICKERS: List[str] = [
    "TSLA","NVDA","AAPL","MSFT","AMZN",
    "GOOGL","NFLX","ADBE","NKE","SBUX",
    "INTC","TTWO","QCOM","GME","EBAY"
]
YEARS = list(range(2014, 2020))
BENCH = "SPY"
OUTDIR = Path("Daily Excess Return Data")
OUTDIR.mkdir(parents=True, exist_ok=True)

RAW_DP = 4   # raw decimals
PCT_DP = 3   # percent columns (with %)

def _fmt_pct(x: float) -> str:
    return f"{x*100:.{PCT_DP}f}%"

def _dl_prices(ticker: str, start: str, end: str) -> pd.Series:
    """
    Download adjusted prices as a Series (name=ticker).
    Handles cases where df['Close'] is returned as a DataFrame.
    """
    df = yf.download(
        ticker, start=start, end=end,
        auto_adjust=True, progress=False, group_by="column", threads=False
    )
    if df is None or df.empty:
        return pd.Series(dtype=float)

    # Prefer 'Close' (auto_adjust=True puts adjusted in Close). Fallback to 'Adj Close'.
    close = None
    if "Close" in df.columns:
        close = df["Close"]
    elif "Adj Close" in df.columns:
        close = df["Adj Close"]
    else:
        # Some odd cases return a single column – squeeze it.
        close = df.squeeze("columns")

    # If still a DataFrame (e.g., MultiIndex or single-col DataFrame), squeeze to Series.
    if isinstance(close, pd.DataFrame):
        if close.shape[1] == 1:
            close = close.iloc[:, 0]
        else:
            # Multiple cols not expected for a single ticker; pick first as fallback
            close = close.iloc[:, 0]

    s = pd.Series(close.values, index=pd.to_datetime(close.index), name=ticker).sort_index()
    return s

def _daily_returns(s: pd.Series) -> pd.Series:
    return s.pct_change().dropna()

def _one_year_df(tkr: str, year: int) -> pd.DataFrame:
    start = f"{year}-01-01"
    end   = f"{year+1}-01-01"

    s_tkr = _dl_prices(tkr, start, end)
    s_spy = _dl_prices(BENCH, start, end)

    if s_tkr.empty or s_spy.empty:
        return pd.DataFrame(columns=[
            "Date", f"{tkr} Daily Return", "SPY Daily Return", "Excess Daily Return",
            f"{tkr} Daily Return (%)", "SPY Daily Return (%)", "Excess Daily Return (%)"
        ])

    r_tkr = _daily_returns(s_tkr)
    r_spy = _daily_returns(s_spy)

    idx = r_tkr.index.intersection(r_spy.index)
    r_tkr = r_tkr.loc[idx]
    r_spy = r_spy.loc[idx]
    r_exc = (r_tkr - r_spy)

    df = pd.DataFrame({
        "Date": idx,
        f"{tkr} Daily Return": r_tkr.round(RAW_DP).values,
        "SPY Daily Return":   r_spy.round(RAW_DP).values,
        "Excess Daily Return": r_exc.round(RAW_DP).values,
    })

    df[f"{tkr} Daily Return (%)]"] = [ _fmt_pct(x) for x in r_tkr.values ]
    df["SPY Daily Return (%)"]     = [ _fmt_pct(x) for x in r_spy.values ]
    df["Excess Daily Return (%)"]  = [ _fmt_pct(x) for x in r_exc.values ]

    df = df[[
        "Date",
        f"{tkr} Daily Return", "SPY Daily Return", "Excess Daily Return",
        f"{tkr} Daily Return (%)]", "SPY Daily Return (%)", "Excess Daily Return (%)"
    ]]

    return df.reset_index(drop=True)

def main():
    for tkr in TICKERS:
        for yr in YEARS:
            out = OUTDIR / f"{tkr}_{yr}_excess.csv"
            df = _one_year_df(tkr, yr)
            df.to_csv(out, index=False, encoding="utf-8")
            print(f"[{tkr} {yr}] wrote {len(df):4d} rows → {out.name}")
    print("\nDone. Files saved in:", OUTDIR.resolve())

if __name__ == "__main__":
    main()
