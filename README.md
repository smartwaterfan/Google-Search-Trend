Hello! We are Tyler He and Harshith Akurati, two high school students from St. Louis MO working with Saint Louis University professor Marcus Painter to answer a question: 

When people suddenly search for a stock more on Google, do prices move in a consistent way?



---A basic rundown---



Google Search Trends (GST) shows how often a term is searched on Google, scaled 0–100 within the time period you choose.
We use United States, Web Search, weekly data (Google’s weeks run Sunday→Saturday).
A value of 100 means “most-searched for this ticker in that year,” not an absolute count.
We pick 15 retail-heavy tickers (e.g., AAPL, TSLA, NVDA…).
We flag “spike weeks” when weekly GST is ≥ 85 (a high-attention week).
To avoid counting the same story twice, we remove overlapping spikes so anchor weeks are at least 3 calendar weeks apart.
Around each spike week we look at ~15 trading days (a 3-week window):
We compute each stock’s daily excess return = stock return − S&P 500 ETF (SPY) return.
We record which trading day (1–15) has the largest positive excess return (the “position”) and...how long returns stay non-negative afterwards (a simple “streak” length).
The end result is a clean, per-ticker summary like: “On average, the biggest positive move arrives around day 8, and the non-negative streak lasts ~2 days.” (We’ll use this later to test simple trading rules.)



---Workflow---



First, install the necessary packages:

pip install pandas numpy yfinance pytrends python-dateutil

1) Fetch weekly Google Trends

Script: GST Weekly Data Gathering.py
What it does: Downloads weekly GST for each ticker and year (2014–2019).
Output folder: GST Data/Weekly Data/
Files: TICKER_YEAR.csv

Columns:
Week (week start date)
<TICKER> (0–100, scaled within that year)
isPartial (True/False if the week was incomplete at download time)

2) Keep only big-attention weeks (≥ 85)

Script: GST Weekly Sorting over 85, with overlaps.py
What it does: Filters each year’s file down to weeks with GST ≥ 85.
Output folder: GST Data/Weekly filtered over 85, with overlaps/
Files: TICKER_YEAR_th85.csv
(These can still be close together in time.)

3) Remove overlaps (spacing rule)

Script: GST Weekly, no overlaps.py
What it does: Enforces ≥ 3 calendar weeks between spike anchors.
Output folder: GST Data/Weekly filtered over 85, no overlaps/
Files: TICKER_YEAR_th85_no_overlap.csv

4) Build daily excess returns (stock – SPY)

Script: Daily Excess Returns.py
What it does: Uses free Yahoo Finance data (via yfinance) to compute:

Stock Daily Return (raw) — decimal (e.g., 0.0123 = 1.23%)
SPY Daily Return (raw)
Excess Daily Return (raw) = stock − SPY...and percent versions of each (nice for reading).

Output folder: Daily Excess Return Data/
Files: TICKER_YEAR_excess.csv
Columns (simplified): Date, three “(raw)” columns, three matching “(%)” columns.

5) Join GST spikes with returns around them

Script: Combiner.py
What it does: For every spike week (from step 3), look at a 3-week trading window (~15 trading days) centered on the week and record:

Max Positive Excess Return (%) and the day index (1–15) where it occurred,
Non-negative streak length starting at that day (how long excess return stays ≥ 0),
Also logs the Max Absolute Excess Return (%) (largest move by magnitude, sign kept)

Output folder: Conjunction/
Files: TICKER_conjunction.csv (one row per spike)

6) One simple summary per ticker (averages)

Script: Conjunction Summary.py

What it does: Produces an easy table: per ticker,
Average position of the max positive (1–15)
Average non-negative streak length

Output folder: Conjunction/
File: overall_conjunction_summary.csv



---How to read the outputs---



0–100 GST numbers: A “100” is the peak search week for that year for that ticker.
Excess return: If AAPL is +1.2% and SPY is +0.3%, then excess = +0.9% (AAPL beat the market by 0.9% that day).
“Position” 1–15: Counts trading days in the window. If position = 9, the best positive day was the 9th trading day after the window started.
“Streak length”: Starting from that best positive day, how many consecutive days had excess return ≥ 0 (0 means it didn’t persist).



---Notes, caveats, and quick tips---



Google rate limits (429): Scripts include gentle backoff. If you hit a 429, just re-run.
Weekly alignment: Google’s week is Sun→Sat. We anchor the spike to the week start and look at the surrounding trading days (markets close on weekends/holidays).
Why compare to SPY? Using excess returns removes “market-wide” moves (e.g., a Fed announcement) so we focus on the stock’s move beyond the market.



---What next? (as of October 7th, 2025)---



Develop a trading strategy using a defined simple “live” attention jump (e.g., percent change vs a running median of prior weeks).
Backtest simple rules with three “knobs”:
Attention threshold (how big the jump must be),
Entry delay (how many days to wait after the spike),
Exit rule (fixed days or when excess return turns negative).
We’ll tune on earlier years and check on later years.



---Troubleshooting---

Empty outputs: Make sure each previous step ran and produced CSVs in the folders listed above.
Different tickers/years: You can edit the lists at the top of each script (they’re clearly labeled).
Percent vs raw: The percent columns are for humans; the raw decimal columns are best for calculations.
