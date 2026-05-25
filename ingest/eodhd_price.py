"""
eodhd_price.py — Fetch current (15-min delayed) price for a ticker via EODHD free tier.

Free tier: 20 API calls/day, 15-20 min delayed quotes.
Requires a free API key from https://eodhd.com (sign up, no credit card needed).

Set your key as an env var:
    set EODHD_API_KEY=your_key_here

Usage:
    python scraper/eodhd_price.py
    python scraper/eodhd_price.py --ticker AAPL
"""

import argparse
import os
import requests

API_KEY = os.environ.get("EODHD_API_KEY", "demo")
BASE_URL = "https://eodhd.com/api/real-time"


def get_latest_quote(ticker: str) -> dict:
    url = f"{BASE_URL}/{ticker}"
    resp = requests.get(url, params={"api_token": API_KEY, "fmt": "json"}, timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_latest_1min_candle(ticker: str) -> dict:
    url = f"https://eodhd.com/api/intraday/{ticker}"
    resp = requests.get(url, params={"api_token": API_KEY, "interval": "1m", "fmt": "json"}, timeout=10)
    resp.raise_for_status()
    candles = resp.json()
    return candles[-1] if candles else {}


def print_quote(data: dict):
    print(f"Ticker:       {data.get('code')}")
    print(f"Price:        {data.get('close')}")
    print(f"Open:         {data.get('open')}")
    print(f"High:         {data.get('high')}")
    print(f"Low:          {data.get('low')}")
    print(f"Volume:       {data.get('volume')}")
    print(f"Change:       {data.get('change')} ({data.get('change_p')}%)")
    print(f"Timestamp:    {data.get('timestamp')} (15-min delayed)")


def print_candle(data: dict):
    import datetime
    ts = data.get("timestamp")
    dt = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M UTC") if ts else "N/A"
    print(f"Datetime:     {dt}")
    print(f"Open:         {data.get('open')}")
    print(f"High:         {data.get('high')}")
    print(f"Low:          {data.get('low')}")
    print(f"Close:        {data.get('close')}")
    print(f"Volume:       {data.get('volume')}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", default="NVDA.US", help="ticker in EODHD format e.g. NVDA.US")
    parser.add_argument("--mode", choices=["quote", "candle"], default="candle",
                        help="quote = latest delayed price, candle = latest 1-min OHLCV (default: candle)")
    args = parser.parse_args()

    if API_KEY == "demo":
        print("warning: using demo key — only works for AAPL.US, TSLA.US, AMZN.US, VTI.US")
        print("         set EODHD_API_KEY env var for any ticker including NVDA\n")

    if args.mode == "candle":
        print(f"Latest 1-min candle for {args.ticker}:")
        print_candle(get_latest_1min_candle(args.ticker))
    else:
        print(f"Latest quote for {args.ticker}:")
        print_quote(get_latest_quote(args.ticker))


if __name__ == "__main__":
    main()
