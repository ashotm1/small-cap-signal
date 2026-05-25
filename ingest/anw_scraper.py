"""
anw_scraper.py — Convert ACCESS Newswire monthly XML sitemaps to CSV.

Fetches sitemap index, downloads each monthly .xml, extracts all press release
entries: url, date, industry, language. Industry parsed from URL path
(/newsroom/en/<industry>/<slug>-<id>). Company + title come from slug at
classifier stage.

Fields: date, language, industry, url

Usage:
    python scraper/anw_scraper.py                          # all months
    python scraper/anw_scraper.py --from 2022-01           # from month onwards
    python scraper/anw_scraper.py --from 2022-01 --to 2024-12
    python scraper/anw_scraper.py --refetch                # re-process already-done months
"""

import argparse
import csv
import os
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

OUTPUT_DIR  = "data/anw"
DONE_FILE   = "data/anw/sitemap_done.txt"
INDEX_URL   = "https://www.accessnewswire.com/public/sitemap/index.xml"
DELAY       = 2

CSV_FIELDS  = ["date", "language", "industry", "url"]

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0"}

# Monthly sitemap URL pattern: .../sitemap/YYYY/MM.xml → "YYYY-MM"
_MONTH_URL = re.compile(r"/sitemap/(\d{4})/(\d{2})\.xml$")

# Industry segment: /newsroom/<lang>/<industry>/<slug>
_INDUSTRY = re.compile(r"^/newsroom/[a-z-]+/([^/]+)/")


def url_to_month(url: str) -> str:
    """Return 'YYYY-MM' for a monthly sitemap URL, or '' if unparseable."""
    m = _MONTH_URL.search(url)
    return f"{m.group(1)}-{m.group(2)}" if m else ""


def fetch_index() -> list[str]:
    """Return list of monthly sitemap URLs from the index."""
    req = urllib.request.Request(INDEX_URL, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read()
    root = ET.fromstring(data)
    return [loc.text for loc in root.findall(".//sm:loc", NS) if loc.text]


def industry_from_url(url: str) -> str:
    """Extract industry slug from URL path. Empty if path doesn't match."""
    path = urllib.parse.urlparse(url).path
    m = _INDUSTRY.match(path)
    return m.group(1) if m else ""


def parse_month(month_url: str) -> list[dict]:
    """Download and parse one monthly sitemap. Returns list of row dicts."""
    req = urllib.request.Request(month_url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read()
    root = ET.fromstring(raw)

    rows = []
    for url_el in root.findall("sm:url", NS):
        loc      = url_el.findtext("sm:loc",      namespaces=NS) or ""
        lastmod  = url_el.findtext("sm:lastmod",  namespaces=NS) or ""
        language = url_el.findtext("sm:language", namespaces=NS) or ""
        if not loc:
            continue
        rows.append({
            "date":     lastmod,
            "language": language,
            "industry": industry_from_url(loc),
            "url":      loc,
        })
    return rows


def csv_path(month: str) -> str:
    return os.path.join(OUTPUT_DIR, f"anw_{month}.csv")


def load_done() -> set[str]:
    if not os.path.exists(DONE_FILE):
        return set()
    with open(DONE_FILE, encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def mark_done(url: str):
    with open(DONE_FILE, "a", encoding="utf-8") as f:
        f.write(url + "\n")


def write_month(rows: list[dict], month: str):
    path = csv_path(month)
    with open(path, "w", newline="\n", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="from_month", help="Start month YYYY-MM (inclusive)")
    parser.add_argument("--to",   dest="to_month",   help="End month YYYY-MM (inclusive)")
    parser.add_argument("--refetch", action="store_true", help="Re-process already-done months")
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Fetching sitemap index...")
    urls = fetch_index()
    print(f"  {len(urls)} sitemap URLs found")

    done = set() if args.refetch else load_done()
    print(f"  {len(done)} months already done\n")

    filtered = []
    for url in urls:
        month = url_to_month(url)
        if not month:
            continue  # skip non-monthly entries (e.g. the static sitemap.xml)
        if args.from_month and month < args.from_month:
            continue
        if args.to_month and month > args.to_month:
            continue
        filtered.append((url, month))

    filtered.sort(key=lambda x: x[1])
    print(f"Processing {len(filtered)} months\n")

    total_new = 0
    for url, month in filtered:
        if url in done:
            print(f"  {month}  already done — skip")
            continue
        print(f"  {month}  downloading...", end=" ", flush=True)
        try:
            rows = parse_month(url)
        except Exception as e:
            print(f"ERROR — {e}")
            continue
        write_month(rows, month)
        total_new += len(rows)
        mark_done(url)
        print(f"{len(rows)} entries  ->  {csv_path(month)}")
        time.sleep(DELAY)

    print(f"\nDone. {total_new} total rows written to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
