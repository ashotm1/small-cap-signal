"""
stocktitan_scraper.py — Scrape StockTitan news by date, iterating backwards.

Fetches https://www.stocktitan.net/news/YYYY-MM-DD/ for each date,
parses all news rows, and appends to data/stocktitan_news.csv.

Fields: date, time, datetime, ticker, exchange, title, url, tags, impact_score, sentiment_score

Usage:
    python scraper/stocktitan_scraper.py                        # from today backwards 30 days
    python scraper/stocktitan_scraper.py --days 90              # go back 90 days
    python scraper/stocktitan_scraper.py --from 2026-04-01 --to 2026-04-26
    python scraper/stocktitan_scraper.py --from 2026-04-01      # from date to today
"""

import argparse
import csv
import os
import time
import random
from datetime import date, timedelta

from bs4 import BeautifulSoup
from curl_cffi import requests

OUTPUT_CSV = "data/stocktitan_news.csv"
BASE_URL   = "https://www.stocktitan.net/news"
DELAY      = 2.0  # safe floor from probe results

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

CSV_FIELDS = ["date", "time", "datetime", "ticker", "exchange", "title", "url", "tags", "impact_score", "sentiment_score"]


def _count_filled(bar_div) -> int:
    """Count filled dots/segments in an impact or sentiment bar — returns 0-10."""
    if not bar_div:
        return 0
    return len(bar_div.find_all(class_="full"))


def parse_page(html: str, date_str: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("div", class_=lambda c: c and "d-flex" in c and "py-2" in c and "news-row" in c)

    articles = []
    for row in rows:
        try:
            # ticker + exchange
            ticker_div = row.find("div", attrs={"name": "tickers"})
            ticker = ticker_div.find("span", class_="symbol-link").get_text(strip=True) if ticker_div else None
            exchange_text = ticker_div.get_text(strip=True) if ticker_div else ""
            exchange = exchange_text.split(":")[-1].strip() if ":" in exchange_text else None

            # date + time
            date_span = row.find("span", attrs={"name": "date"})
            time_span = row.find("span", attrs={"name": "time"})
            date_val = date_span.get_text(strip=True) if date_span else date_str
            time_val = time_span.get_text(strip=True) if time_span else None
            dt = f"{date_val} {time_val}".strip() if time_val else date_val

            # title + url
            title_div = row.find("div", attrs={"name": "title"})
            link = title_div.find("a") if title_div else None
            title = link.get_text(strip=True) if link else None
            url = "https://www.stocktitan.net" + link["href"] if link and link.get("href", "").startswith("/") else (link["href"] if link else None)

            # tags
            tags_div = row.find("div", attrs={"name": "tags"})
            tags = [t.get_text(strip=True) for t in tags_div.find_all("span")] if tags_div else []

            # impact + sentiment scores (count filled elements out of 10)
            indicators = row.find_all("div", class_="news-indicator")
            impact_score = None
            sentiment_score = None
            for ind in indicators:
                label = ind.find("span", class_="news-indicator-title")
                if not label:
                    continue
                bar = ind.find("div", class_=lambda c: c and "bar" in c)
                score = _count_filled(bar)
                if "IMPACT" in label.get_text():
                    impact_score = score
                elif "SENTIMENT" in label.get_text():
                    sentiment_score = score

            if not ticker or not title:
                continue

            articles.append({
                "date": date_val,
                "time": time_val,
                "datetime": dt,
                "ticker": ticker,
                "exchange": exchange,
                "title": title,
                "url": url,
                "tags": "|".join(tags),
                "impact_score": impact_score,
                "sentiment_score": sentiment_score,
            })

        except Exception as e:
            print(f"  warning: skipped row — {e}")
            continue

    return articles


def load_existing_urls() -> set:
    if not os.path.exists(OUTPUT_CSV):
        return set()
    with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
        return {row["url"] for row in csv.DictReader(f)}


# Scraped HTML occasionally carries "unusual" line terminators (LS/PS/NEL).
# They corrupt nothing but trip editors' "unusual line terminator" warnings.
# Map each to a space (1:1, so field lengths are preserved).
_LINE_SEP_FIX = {0x2028: " ", 0x2029: " ", 0x85: " "}


def _clean_row(row: dict) -> dict:
    return {k: (v.translate(_LINE_SEP_FIX) if isinstance(v, str) else v)
            for k, v in row.items()}


def append_rows(rows: list[dict]):
    file_exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, "a", newline="\n", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(_clean_row(r) for r in rows)


def scrape_date(d: date, session, existing_urls: set) -> int:
    date_str = d.strftime("%Y-%m-%d")
    url = f"{BASE_URL}/{date_str}/"

    resp = session.get(url, headers=HEADERS, timeout=15)

    if resp.status_code == 404:
        print(f"  {date_str}  404 — skipping")
        return 0, 0

    if resp.status_code != 200:
        print(f"  {date_str}  status={resp.status_code} — skipping")
        return 0, 0

    articles = parse_page(resp.text, date_str)
    new = [a for a in articles if a["url"] not in existing_urls]

    if new:
        append_rows(new)
        for a in new:
            existing_urls.add(a["url"])

    print(f"  {date_str}  {len(articles)} articles  {len(new)} new")
    return len(articles), len(new)


def date_range(start: date, end: date):
    """Yields dates from end down to start (newest first)."""
    d = end
    while d >= start:
        yield d
        d -= timedelta(days=1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30, help="number of days back from today (default: 30)")
    parser.add_argument("--from", dest="from_date", help="start date YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", help="end date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    today = date.today()

    if args.from_date:
        start = date.fromisoformat(args.from_date)
        end = date.fromisoformat(args.to_date) if args.to_date else today
    else:
        end = today
        start = end - timedelta(days=args.days - 1)

    print(f"Scraping {start} to {end} ({(end - start).days + 1} days)")
    print(f"Output: {OUTPUT_CSV}\n")

    existing_urls = load_existing_urls()
    print(f"Already have {len(existing_urls)} articles in CSV\n")

    session = requests.Session(impersonate="chrome124")
    total_new = 0
    empty_streak = 0

    for d in date_range(start, end):
        total_count, new_count = scrape_date(d, session, existing_urls)
        total_new += new_count

        if total_count == 0:
            empty_streak += 1
            if empty_streak >= 3:
                print(f"\n3 consecutive days with 0 articles on page — stopping early")
                break
        else:
            empty_streak = 0

        time.sleep(DELAY + random.uniform(0, 0.5))

    print(f"\nDone. {total_new} new articles added to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
