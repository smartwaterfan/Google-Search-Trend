#!/usr/bin/env python3
"""Utility script to locate high-attention Google Search Trend weeks for popular tickers.

The script downloads weekly Google Trends data for the calendar year requested, spots
"attention spikes" (GST >= 80), anchors each week to the appropriate trading day, removes
overlapping windows, and writes the final list of events to a CSV file.

It also stores raw weekly GST pulls per ticker so analysts can audit the underlying data.

Example usage (defaults to 2023 retail favourites):

    python 01_gst_events_2023.py \
        --tickers "AMC,GME,TSLA" \
        --year 2023 \
        --add-stock-term \
        --geo "US"
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse
import pandas as pd
import requests
from dotenv import load_dotenv
from pytrends.request import TrendReq
from tqdm import tqdm

# --------------------------------------------------------------------------------------
# Constants and configuration helpers
# --------------------------------------------------------------------------------------

DEFAULT_TICKERS: Tuple[str, ...] = (
    "AMC",
    "GME",
    "TSLA",
    "NVDA",
    "AAPL",
    "NIO",
    "PLTR",
    "AMD",
    "RIVN",
    "LCID",
    "HOOD",
    "SOFI",
    "COIN",
    "META",
    "BBBYQ",
)

TIMEFRAME_TEMPLATE = "{year}-01-01 {year}-12-31"
LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(message)s"

# --------------------------------------------------------------------------------------
# Logging / directory helpers
# --------------------------------------------------------------------------------------


def ensure_directories(paths: Iterable[Path]) -> None:
    """Create all directories listed if they do not already exist."""

    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def configure_logger(log_path: Path) -> logging.Logger:
    """Configure a root logger that writes to file and prints to stdout."""

    ensure_directories([log_path.parent])

    logger = logging.getLogger("gst_events")
    logger.setLevel(logging.INFO)

    # Remove any handlers that might have been left around if the script runs multiple times
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False

    logger.info("Logging initialised. Writing detailed logs to %s", log_path)
    return logger


# --------------------------------------------------------------------------------------
# HTTP helper for Polygon.io
# --------------------------------------------------------------------------------------


def http_get_json(url: str, session: Optional[requests.Session], logger: logging.Logger) -> Dict:
    """GET JSON payload with retry/backoff logic.

    Parameters
    ----------
    url:
        Full URL to request.
    session:
        Optional `requests.Session` to reuse connections. If None, a temporary session is used.
    logger:
        Logger for progress/error messages.
    """

    parsed = urlparse(url)
    session_to_use = session or requests.Session()
    max_attempts = 6
    backoff_seconds = 1.0

    for attempt in range(1, max_attempts + 1):
        try:
            response = session_to_use.get(url, timeout=30)
        except requests.RequestException as exc:  # Network hiccup
            logger.warning(
                "GET %s failed on attempt %d/%d due to %s. Retrying...",
                parsed.path,
                attempt,
                max_attempts,
                exc,
            )
            sleep_for = backoff_seconds * (2 ** (attempt - 1)) + random.uniform(0, 1)
            time.sleep(min(sleep_for, 60))
            continue

        logger.info("GET %s - status %s (attempt %d/%d)", parsed.path, response.status_code, attempt, max_attempts)

        if response.status_code == 200:
            return response.json()

        if response.status_code == 401:
            logger.error("Polygon API key rejected with HTTP 401. Please check POLYGON_API_KEY in .env.")
            raise SystemExit(1)

        if response.status_code == 403:
            # Polygon free tier sometimes blocks intraday/extended usage. We retry once more then exit.
            if attempt == max_attempts:
                logger.error(
                    "Polygon API returned HTTP 403 for %s even after retries. "
                    "This usually indicates the current plan does not allow the requested data.",
                    parsed.path,
                )
                raise SystemExit(1)
            logger.warning("HTTP 403 received. Waiting briefly before retrying...")
            time.sleep(min(backoff_seconds * (2 ** (attempt - 1)), 30))
            continue

        if response.status_code in (429,) or 500 <= response.status_code < 600:
            wait_for = min(backoff_seconds * (2 ** (attempt - 1)) + random.uniform(0, 1), 60)
            logger.warning("Received %s from Polygon. Sleeping %.1f seconds before retrying...", response.status_code, wait_for)
            time.sleep(wait_for)
            continue

        # Any other 4xx is considered fatal for this run.
        logger.error(
            "Unexpected HTTP status %s from Polygon for %s: %s",
            response.status_code,
            parsed.path,
            response.text,
        )
        raise SystemExit(1)

    logger.error("Failed to retrieve data from Polygon after %d attempts.", max_attempts)
    raise SystemExit(1)


# --------------------------------------------------------------------------------------
# Polygon / trading calendar helpers
# --------------------------------------------------------------------------------------


def fetch_spy_trading_dates(year: int, logger: logging.Logger) -> Tuple[date, set[date]]:
    """Fetch SPY daily bars for the year and return a set of trading dates.

    Returns
    -------
    Tuple containing the maximum trading date observed and the set of all trading dates.
    The maximum is helpful as a guard when shifting anchors to the next trading day.
    """

    load_dotenv()
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        logger.error("POLYGON_API_KEY not found in environment. Please create a .env file with the key.")
        raise SystemExit(1)

    url = (
        "https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/"
        f"{year}-01-01/{year}-12-31?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
    )

    session = requests.Session()
    payload = http_get_json(url, session=session, logger=logger)
    results = payload.get("results", [])

    if not results:
        logger.error("Polygon response did not contain any results. Payload: %s", payload)
        raise SystemExit(1)

    df = pd.DataFrame(results)
    if "t" not in df:
        logger.error("Unexpected Polygon payload structure: missing 't' field. Payload: %s", payload)
        raise SystemExit(1)

    df["date"] = pd.to_datetime(df["t"], unit="ms").dt.date
    trading_dates = set(df["date"].tolist())

    max_trading_date = max(trading_dates)
    logger.info("Fetched %d SPY trading days. Last trading day in dataset: %s", len(trading_dates), max_trading_date)
    return max_trading_date, trading_dates


# --------------------------------------------------------------------------------------
# Google Trends helpers
# --------------------------------------------------------------------------------------


def build_pytrends_client() -> TrendReq:
    """Create a PyTrends client with reasonable retry behaviour."""

    return TrendReq(hl="en-US", tz=0, retries=3, backoff_factor=0.2, requests_args={"timeout": 30})


def fetch_weekly_trends(
    client: TrendReq,
    ticker: str,
    include_stock_term: bool,
    timeframe: str,
    geo: str,
    logger: logging.Logger,
    max_attempts: int = 6,
) -> pd.DataFrame:
    """Fetch weekly Google Trends interest for the ticker (and optional stock term).

    The returned DataFrame contains a datetime index, columns for each search term,
    and a `gst_value` column holding the combined series used for spike detection.
    """

    terms = [ticker]
    if include_stock_term:
        terms.append(f"{ticker} stock")

    for attempt in range(1, max_attempts + 1):
        try:
            client.build_payload(kw_list=terms, timeframe=timeframe, geo=geo, gprop="")
            data = client.interest_over_time()
        except Exception as exc:  # Pytrends raises generic Exceptions on rate limits
            sleep_time = min(2 ** attempt + random.uniform(0, 1), 60)
            logger.warning(
                "PyTrends request failed for %s on attempt %d/%d: %s. Sleeping %.1fs before retrying...",
                ticker,
                attempt,
                max_attempts,
                exc,
                sleep_time,
            )
            time.sleep(sleep_time)
            continue

        if data is None or data.empty:
            logger.warning("No Google Trends data returned for %s (attempt %d/%d).", ticker, attempt, max_attempts)
            time.sleep(min(2 ** attempt, 30))
            continue

        data = data.drop(columns=["isPartial"], errors="ignore")
        data.index = pd.to_datetime(data.index)
        data.sort_index(inplace=True)

        if include_stock_term and len(terms) > 1:
            data["gst_value"] = data.max(axis=1)
        else:
            data["gst_value"] = data[terms[0]]

        data["ticker"] = ticker
        return data

    logger.error("PyTrends returned no data for %s after %d attempts.", ticker, max_attempts)
    return pd.DataFrame()


# --------------------------------------------------------------------------------------
# Spike detection and window management
# --------------------------------------------------------------------------------------


def resolve_overlaps(events: List[Dict]) -> List[Dict]:
    """Remove overlapping events, keeping the earlier spike and flagging it appropriately."""

    if not events:
        return []

    sorted_events = sorted(events, key=lambda e: e["anchor_date_dt"])
    kept_events: List[Dict] = []
    last_kept_event: Optional[Dict] = None
    last_window_end: Optional[date] = None

    for event in sorted_events:
        if last_window_end is not None and event["window_start_dt"] <= last_window_end:
            if last_kept_event is not None:
                last_kept_event["overlap_dropped_later_flag"] = 1
            continue

        event.setdefault("overlap_dropped_later_flag", 0)
        kept_events.append(event)
        last_kept_event = event
        last_window_end = event["window_end_dt"]

    return kept_events


def analyse_ticker(
    ticker: str,
    gst_df: pd.DataFrame,
    trading_dates: set[date],
    max_trading_date: date,
    include_stock_term: bool,
    year: int,
    logger: logging.Logger,
) -> List[Dict]:
    """Convert weekly GST data for a ticker into a list of spike events."""

    if gst_df.empty:
        logger.warning("Skipping %s because no GST data was retrieved.", ticker)
        return []

    gst_series = gst_df["gst_value"].copy()
    spikes = gst_series[gst_series >= 80]

    if spikes.empty:
        logger.info("No GST spikes detected for %s in %d.", ticker, year)
        return []

    events: List[Dict] = []
    search_basis = "ticker|max(ticker,'ticker stock')" if include_stock_term else "ticker"

    for week_start_ts, gst_value in spikes.items():
        week_start_date = week_start_ts.date()
        week_end_date = week_start_date + timedelta(days=6)
        window_start_date = week_start_date - timedelta(days=7)
        window_end_date = week_end_date + timedelta(days=7)

        anchor_date = week_start_date + timedelta(days=3)  # Wednesday
        shifted_flag = 0
        while anchor_date not in trading_dates:
            shifted_flag = 1
            anchor_date += timedelta(days=1)
            if anchor_date > max_trading_date:
                logger.warning(
                    "Anchor for %s on %s extended beyond available trading calendar; "
                    "using %s regardless.",
                    ticker,
                    week_start_date,
                    anchor_date,
                )
                break

        event = {
            "ticker": ticker,
            "gst_week_start": week_start_date,
            "gst_week_end": week_end_date,
            "gst_value": float(gst_value),
            "anchor_date_dt": anchor_date,
            "anchor_shifted_holiday_flag": int(shifted_flag),
            "window_start_dt": window_start_date,
            "window_end_dt": window_end_date,
            "search_basis": search_basis,
            "overlap_dropped_later_flag": 0,
        }
        events.append(event)

    kept_events = resolve_overlaps(events)
    return kept_events


# --------------------------------------------------------------------------------------
# CLI and orchestration
# --------------------------------------------------------------------------------------


def parse_arguments(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect attention spikes in Google Search Trends for popular tickers.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--tickers",
        type=str,
        default=",".join(DEFAULT_TICKERS),
        help="Comma-separated list of tickers to analyse.",
    )
    parser.add_argument("--year", type=int, default=2023, help="Calendar year to analyse.")
    parser.add_argument(
        "--add-stock-term",
        action="store_true",
        help="Also fetch '{TICKER} stock' and use the max across search terms for spike detection.",
    )
    parser.add_argument("--geo", type=str, default="", help="Geographical code for Google Trends ('' = worldwide).")
    parser.add_argument(
        "--output",
        type=str,
        default="output/gst_events_2023.csv",
        help="Path to the final events CSV.",
    )
    parser.add_argument(
        "--log",
        type=str,
        default="logs/gst_events_2023.log",
        help="Path to the log file.",
    )

    return parser.parse_args(argv)


def write_raw_gst_csv(ticker: str, gst_df: pd.DataFrame, year: int, destination_dir: Path, logger: logging.Logger) -> None:
    """Persist the raw weekly Google Trends pulls for reproducibility."""

    if gst_df.empty:
        return

    destination_dir.mkdir(parents=True, exist_ok=True)

    # Reset index so the week start date becomes a column.
    export_df = gst_df.copy()
    export_df.insert(0, "date", export_df.index.date)

    file_path = destination_dir / f"{ticker}_weekly_{year}.csv"
    export_df.to_csv(file_path, index=False)
    logger.info("Saved raw GST data for %s to %s", ticker, file_path)


def finalise_events(events: List[Dict]) -> pd.DataFrame:
    """Convert list of dict events into a tidy DataFrame ready for CSV export."""

    if not events:
        return pd.DataFrame(
            columns=[
                "ticker",
                "gst_week_start",
                "gst_week_end",
                "gst_value",
                "anchor_date",
                "anchor_shifted_holiday_flag",
                "overlap_dropped_later_flag",
                "window_start",
                "window_end",
                "search_basis",
            ]
        )

    formatted_events = []
    for event in events:
        formatted_events.append(
            {
                "ticker": event["ticker"],
                "gst_week_start": event["gst_week_start"].isoformat(),
                "gst_week_end": event["gst_week_end"].isoformat(),
                "gst_value": event["gst_value"],
                "anchor_date": event["anchor_date_dt"].isoformat(),
                "anchor_shifted_holiday_flag": int(event["anchor_shifted_holiday_flag"]),
                "overlap_dropped_later_flag": int(event["overlap_dropped_later_flag"]),
                "window_start": event["window_start_dt"].isoformat(),
                "window_end": event["window_end_dt"].isoformat(),
                "search_basis": event["search_basis"],
            }
        )

    df = pd.DataFrame(formatted_events)
    df.sort_values(["anchor_date", "ticker"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_arguments(argv)

    tickers = [ticker.strip().upper() for ticker in args.tickers.split(",") if ticker.strip()]
    year = args.year
    timeframe = TIMEFRAME_TEMPLATE.format(year=year)

    output_path = Path(args.output)
    log_path = Path(args.log)
    raw_dir = Path("data/gst_raw")

    ensure_directories([output_path.parent, raw_dir, Path("logs")])
    logger = configure_logger(log_path)

    logger.info("Running GST attention spike detection for %d tickers in %d.", len(tickers), year)

    max_trading_date, trading_dates = fetch_spy_trading_dates(year, logger)
    pytrends_client = build_pytrends_client()

    all_events: List[Dict] = []

    with tqdm(tickers, desc="Tickers", unit="ticker") as progress:
        for ticker in progress:
            progress.set_postfix_str(ticker)
            gst_df = fetch_weekly_trends(
                pytrends_client,
                ticker=ticker,
                include_stock_term=args.add_stock_term,
                timeframe=timeframe,
                geo=args.geo,
                logger=logger,
            )

            write_raw_gst_csv(ticker, gst_df, year, raw_dir, logger)

            ticker_events = analyse_ticker(
                ticker=ticker,
                gst_df=gst_df,
                trading_dates=trading_dates,
                max_trading_date=max_trading_date,
                include_stock_term=args.add_stock_term,
                year=year,
                logger=logger,
            )

            all_events.extend(ticker_events)

    events_df = finalise_events(all_events)
    ensure_directories([output_path.parent])
    events_df.to_csv(output_path, index=False)
    logger.info("Wrote %d GST events to %s", len(events_df), output_path)

    if events_df.empty:
        logger.info("No GST spikes were identified for the selected configuration.")


if __name__ == "__main__":
    main()
