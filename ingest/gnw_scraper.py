"""
gnw_scraper.py — Scrape GlobeNewsWire via paginated search (Nasdaq + NYSE only).

Iterates day-by-day, paginates each day's results (pageSize=50).
Extracts ticker from preview text via regex where available.

Fields: date, time, datetime, ticker, exchange, source, title, url

Usage:
    python scraper/gnw_scraper.py                              # last 30 days
    python scraper/gnw_scraper.py --days 90
    python scraper/gnw_scraper.py --from 2022-01-01 --to 2024-12-31
"""

import argparse
import csv
import os
import re
import signal
import time
import random
from datetime import date, datetime, timedelta

from bs4 import BeautifulSoup
from curl_cffi import requests

OUTPUT_CSV = "data/gnw_news.csv"
BASE_URL   = "https://www.globenewswire.com"
DELAY      = 2

CSV_FIELDS = ["date", "time", "datetime", "ticker", "exchange", "source", "title", "url"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# from pr_detection.py — matches "(NYSE: CCL)", "(NASDAQ: AAPL)", "(NYSE/LSE: CCL; NYSE: CUK)"
# also matches without parens for GNW preview text: "NYSE American: TGB"
_TICKER_RE = re.compile(
    r"\(?(?P<exchange>NYSE American|NYSE Arca|NASDAQ GSM|NASDAQ CM|NASDAQ|NYSE|OTCQB|OTCQX)"
    r"(?:/(?:NYSE|NASDAQ|LSE))?[:\s]+(?P<ticker>[A-Z]{1,6})[;,\s)]*",
    re.IGNORECASE,
)

# matches GNW date strings: "April 30, 2026 17:50 ET"
_DATE_RE = re.compile(
    r'([A-Z][a-z]+ \d{1,2}, \d{4})\s+(\d{1,2}:\d{2})\s*ET'
)


def _search_url(date_str: str, page: int) -> str:
    d = f"%5B{date_str}%2520TO%2520{date_str}%5D"
    return f"{BASE_URL}/en/search/date/{d}/exchange/Nasdaq,NYSE/load/more?page={page}&pageSize=50"


def parse_ticker(text: str) -> tuple:
    """Extract first (ticker, exchange) from text, or ('', '')."""
    m = _TICKER_RE.search(text)
    if m:
        return m.group("ticker").upper(), m.group("exchange").upper()
    return "", ""


def parse_date(text: str) -> tuple:
    """Extract (date_str, time_str, datetime_str) from 'April 30, 2026 17:50 ET'."""
    m = _DATE_RE.search(text)
    if not m:
        return "", "", ""
    try:
        d = datetime.strptime(m.group(1), "%B %d, %Y").strftime("%Y-%m-%d")
        t = m.group(2)
        return d, t, f"{d} {t}"
    except ValueError:
        return "", "", ""


def parse_page(html: str) -> list:
    soup = BeautifulSoup(html, "html.parser")
    items = []

    for li in soup.find_all("li"):
        # article link — must point to a news-release
        a = li.find("a", href=lambda h: h and "/news-release/" in h)
        if not a:
            continue

        title = a.get_text(strip=True)
        url   = a["href"]
        if not url.startswith("http"):
            url = BASE_URL + url

        # company/source — link to organization search
        org = li.find("a", href=lambda h: h and "/en/search/organization/" in h)
        source = org.get_text(strip=True) if org else ""

        # date from full li text
        full_text = li.get_text(" ", strip=True)
        d, t, dt  = parse_date(full_text)

        # ticker from preview text (often contains "NYSE: XYZ")
        ticker, exchange = parse_ticker(full_text)

        if not title or not url:
            continue

        items.append({
            "date": d, "time": t, "datetime": dt,
            "ticker": ticker, "exchange": exchange,
            "source": source, "title": title, "url": url,
        })

    return items


def scrape_day(d: date, session, existing_urls: set) -> tuple:
    """Returns (total, new_count, blocked) where blocked=True signals a hard stop."""
    date_str = d.strftime("%Y-%m-%d")
    total = new_count = page = 0

    while True:
        page += 1
        url = _search_url(date_str, page)
        print(f"  {date_str} p{page}: fetching...", end=" ", flush=True)
        try:
            resp = session.get(url, headers=HEADERS, timeout=(10, 20))
        except Exception as e:
            print(f"error — {e}")
            break

        print(f"got {resp.status_code} {len(resp.content)//1024}KB", end=" ", flush=True)

        if resp.status_code != 200:
            print(f"— blocked")
            return total, new_count, True

        body = resp.text
        _lower = body[:2000].lower()
        if any(k in _lower for k in ("captcha", "access denied", "403 forbidden", "blocked", "robot")):
            print(f"  {date_str} p{page}: BLOCKED (body) — stopping")
            return total, new_count, True
        if len(body) < 500:
            print(f"  {date_str} p{page}: suspiciously small response ({len(body)}B) — may be blocked")

        print(f"parsing...", end=" ", flush=True)
        items = parse_page(body)
        if not items:
            print("0 items — done")
            break

        new = [i for i in items if i["url"] not in existing_urls]

        # stop if all items are duplicates (GNW re-serving already-seen articles)
        if len(new) == 0 and len(items) > 0:
            print(f"all duplicates — done")
            break

        if new:
            _append(new)
            for i in new:
                existing_urls.add(i["url"])

        total     += len(items)
        new_count += len(new)
        print(f"{len(items)} items  {len(new)} new")

        if len(items) < 50:
            break  # last page

        time.sleep(DELAY + random.uniform(0, 0.4))

    return total, new_count, False


# Scraped HTML occasionally carries "unusual" line terminators (LS/PS/NEL).
# They corrupt nothing but trip editors' "unusual line terminator" warnings.
# Map each to a space (1:1, so field lengths are preserved).
_LINE_SEP_FIX = {0x2028: " ", 0x2029: " ", 0x85: " "}


def _clean_row(row: dict) -> dict:
    return {k: (v.translate(_LINE_SEP_FIX) if isinstance(v, str) else v)
            for k, v in row.items()}


def _append(rows: list):
    file_exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, "a", newline="\n", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(_clean_row(r) for r in rows)


def load_existing_urls() -> set:
    if not os.path.exists(OUTPUT_CSV):
        return set()
    with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
        return {row["url"] for row in csv.DictReader(f)}


def date_range(start: date, end: date):
    d = end
    while d >= start:
        yield d
        d -= timedelta(days=1)


def main():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--from", dest="from_date")
    parser.add_argument("--to",   dest="to_date")
    args = parser.parse_args()

    today = date.today()
    if args.from_date:
        start = date.fromisoformat(args.from_date)
        end   = date.fromisoformat(args.to_date) if args.to_date else today
    else:
        end   = today
        start = end - timedelta(days=args.days - 1)

    print(f"Scraping {start} to {end} ({(end - start).days + 1} days)")
    print(f"Output: {OUTPUT_CSV}\n")

    existing_urls = load_existing_urls()
    print(f"Already have {len(existing_urls)} articles in CSV\n")

    session   = requests.Session(impersonate="chrome124")
    total_new = 0
    empty_streak = 0
    block_streak = 0

    for d in date_range(start, end):
        total, new, blocked = scrape_day(d, session, existing_urls)
        total_new += new
        print(f"  {d}  {total} articles  {new} new{' [BLOCKED]' if blocked else ''}")

        if blocked:
            block_streak += 1
            if block_streak >= 2:
                print("\n2 consecutive blocks — exiting")
                break
        else:
            block_streak = 0

        if total == 0 and not blocked:
            empty_streak += 1
            if empty_streak >= 7:
                print("\n7 consecutive empty days — stopping")
                break
        else:
            empty_streak = 0

        time.sleep(DELAY + random.uniform(0, 0.4))

    print(f"\nDone. {total_new} new articles added to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
