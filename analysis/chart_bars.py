"""
chart_bars.py — Plot 1-min OHLCV candlestick chart for a ticker/date from price_bars.csv.

Usage:
  python utils/chart_bars.py IXHL 2025-05-14
  python utils/chart_bars.py IXHL 2025-05-14 --acceptance-dt "2025-05-14T08:31:04"
"""
import argparse
import sys

import mplfinance as mpf
import pandas as pd

BARS_CSV  = "data/prices/price_bars.csv"
PRICE_DATA = "data/prices/price_data.csv"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("ticker", help="Ticker symbol")
    parser.add_argument("date", help="Date string YYYY-MM-DD")
    parser.add_argument("--acceptance-dt", metavar="DATETIME",
                        help="Override acceptance_dt for T0 line (ISO format)")
    args = parser.parse_args()

    bars = pd.read_csv(BARS_CSV, low_memory=False, on_bad_lines="skip")
    bars = bars[(bars["ticker"] == args.ticker) & (bars["date_str"] == args.date)]

    if bars.empty:
        print(f"No bars found for {args.ticker} on {args.date}")
        sys.exit(1)

    bars["dt"] = pd.to_datetime(bars["t"], unit="ms", utc=True).dt.tz_convert("US/Eastern")
    bars = bars.sort_values("dt").reset_index(drop=True)
    bars.index.name = "Date"
    bars = bars.rename(columns={"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"})

    # Auto-lookup acceptance_dt from price_data.csv if not overridden
    acceptance_dt = args.acceptance_dt
    if not acceptance_dt:
        try:
            pd_df = pd.read_csv(PRICE_DATA, low_memory=False)
            match = pd_df[(pd_df["ticker"] == args.ticker) & (pd_df["date_str"] == args.date)]
            if not match.empty:
                acceptance_dt = match.iloc[0]["acceptance_dt"]
        except Exception:
            pass

    vlines = None
    if acceptance_dt:
        t0 = pd.to_datetime(acceptance_dt).tz_localize("US/Eastern").floor("min")
        print(f"acceptance_dt: {t0}")
        # Find nearest bar index to t0, slice ±100 bars
        diffs = (bars["dt"] - t0).abs()
        t0_idx = diffs.idxmin()
        start = max(0, t0_idx - 100)
        end   = min(len(bars) - 1, t0_idx + 100)
        bars = bars.iloc[start:end + 1]
        t0_in_range = bars["dt"].iloc[0] <= t0 <= bars["dt"].iloc[-1]
        if t0_in_range:
            vlines = dict(vlines=[t0], linewidths=1.5, linestyle="--", colors="red")

    bars = bars.set_index("dt")

    kwargs = dict(
        type="candle",
        style="nightclouds",
        title=f"{args.ticker} — {args.date} (1-min bars, ET)",
        ylabel="Price",
        volume=True,
        figsize=(16, 8),
        warn_too_much_data=2000,
        show_nontrading=False,
    )
    if vlines:
        kwargs["vlines"] = vlines
    mpf.plot(bars, **kwargs)


if __name__ == "__main__":
    main()
