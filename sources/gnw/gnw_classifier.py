"""
gnw_classifier.py — Classify GNW rows: ticker, exchange (MIC), catalyst tags.

Pipeline:
  ticker (existing) → MIC via ticker_universe lookup
  if no ticker     → source name → ticker via name index (O(log N) prefix match)
  title            → catalyst tags (gated on listed exchange; else 'unlisted')

Usage:
  python scripts/gnw_classifier.py
  python scripts/gnw_classifier.py --input data/gnw_news.csv --output data/gnw_classified.csv
"""
import argparse
import csv
import os

from sources.prnw.prn_classifier import build_ticker_index, lookup_ticker, _LISTED_EXCHANGES
from regex.catalysts import classify_catalyst


def build_ticker_to_mic(path: str) -> dict[str, str]:
    """Read ticker_universe.csv → {ticker: primary_exchange MIC}. First row wins on dupes."""
    out: dict[str, str] = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            t = (row.get("ticker") or "").strip().upper()
            e = (row.get("primary_exchange") or "").strip().upper()
            if t and e and t not in out:
                out[t] = e
    return out


def classify_row(
    row: dict,
    name_index: dict[str, tuple[str, str]],
    sorted_keys: list[str],
    ticker_to_mic: dict[str, str],
) -> dict:
    """Resolve ticker/exchange/catalyst for one GNW row."""
    title = row.get("title") or ""
    ticker = (row.get("ticker") or "").strip().upper()
    exchange = ""

    if ticker:
        exchange = ticker_to_mic.get(ticker, "")
    else:
        hit = lookup_ticker(row.get("source") or "", name_index, sorted_keys)
        if hit:
            ticker, exchange = hit[0], hit[1]

    is_listed = exchange in _LISTED_EXCHANGES
    catalyst = str(classify_catalyst(title)) if (is_listed and title) else str(["unlisted"])
    return {"ticker": ticker, "exchange": exchange, "catalyst": catalyst}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  default="data/gnw_news.csv")
    parser.add_argument("--output", default="data/gnw_classified.csv")
    parser.add_argument("--ticker-universe", default="data/ticker_universe.csv")
    args = parser.parse_args()

    # Append-safe: skip URLs already classified
    done_urls: set[str] = set()
    if os.path.exists(args.output):
        with open(args.output, encoding="utf-8") as f:
            done_urls = {row["url"] for row in csv.DictReader(f)}
        print(f"Resuming — {len(done_urls)} URLs already classified")

    print(f"Building indexes from {args.ticker_universe}...")
    name_index, sorted_keys = build_ticker_index(args.ticker_universe)
    ticker_to_mic = build_ticker_to_mic(args.ticker_universe)
    print(f"  {len(name_index)} names, {len(ticker_to_mic)} tickers loaded")

    fieldnames = ["datetime", "source", "url", "title", "ticker", "exchange", "catalyst"]
    write_header = not os.path.exists(args.output)
    total = 0

    with open(args.input, encoding="utf-8") as f_in, \
         open(args.output, "a", newline="", encoding="utf-8") as f_out:
        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        for row in reader:
            url = row.get("url", "")
            if not url or url in done_urls:
                continue
            result = classify_row(row, name_index, sorted_keys, ticker_to_mic)
            writer.writerow({
                "datetime": row.get("datetime", ""),
                "source":   row.get("source", ""),
                "url":      url,
                "title":    row.get("title", ""),
                "ticker":   result["ticker"],
                "exchange": result["exchange"],
                "catalyst": result["catalyst"],
            })
            done_urls.add(url)
            total += 1

    print(f"\nDone. {total} rows written to {args.output}")
