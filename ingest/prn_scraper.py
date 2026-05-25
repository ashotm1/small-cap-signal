"""
prn_scraper.py — Convert PRN monthly gz sitemaps to CSV.

Fetches sitemap-gz.xml, downloads each monthly .xml.gz, decompresses,
and extracts all press release entries: url, datetime, issuer, image_url.

Fields: date, time, datetime, issuer, image_url, url

Usage:
    python scraper/prn_scraper.py                           # all months
    python scraper/prn_scraper.py --from 2022-01            # from month onwards
    python scraper/prn_scraper.py --from 2022-01 --to 2024-12
    python scraper/prn_scraper.py --refetch                 # re-process already-done months
"""

import argparse
import csv
import gzip
import os
import re
import time
import urllib.request
import xml.etree.ElementTree as ET

OUTPUT_DIR  = "data/prn_data"
DONE_FILE   = "data/prn_data/gz_done.txt"
GZ_INDEX    = "https://www.prnewswire.com/sitemap-gz.xml"
DELAY       = 2

CSV_FIELDS  = ["date", "time", "datetime", "issuer", "image_url", "url"]

NS = {
    "sm":    "http://www.sitemaps.org/schemas/sitemap/0.9",
    "image": "http://www.google.com/schemas/sitemap-image/1.1",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

_PRNEWSFOTO = re.compile(r"PRNewsfoto/([^)]+)")

# month token in filename: Sitemap_Index_Apr_2026.xml.gz → "2026-04"
_MONTH_MAP = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}
_GZ_MONTH = re.compile(r"Sitemap_Index_([A-Za-z]+)_(\d{4})\.xml\.gz")


def gz_to_month(gz_url: str) -> str:
    """Return 'YYYY-MM' for a gz URL, or '' if unparseable."""
    m = _GZ_MONTH.search(gz_url)
    if not m:
        return ""
    mon = _MONTH_MAP.get(m.group(1).capitalize(), "")
    year = m.group(2)
    if not mon or len(year) != 4:
        return ""
    return f"{year}-{mon}"


def fetch_gz_index() -> list[str]:
    """Return list of all gz sitemap URLs from sitemap-gz.xml."""
    req = urllib.request.Request(GZ_INDEX, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read()
    root = ET.fromstring(data)
    return [loc.text for loc in root.findall(".//sm:loc", NS) if loc.text]


_INVALID_XML_CHARS = re.compile(rb"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_URL_BLOCK    = re.compile(rb"<url>(.*?)</url>", re.DOTALL)
_RX_LOC       = re.compile(rb"<loc>(https://[^<]+)</loc>")
_RX_LASTMOD   = re.compile(rb"<lastmod>([^<]+)</lastmod>")
_RX_CAPTION   = re.compile(rb"<image:caption>([^<]*)</image:caption>")
_RX_IMG_LOC   = re.compile(rb"<image:loc>(https://[^<]+)</image:loc>")


def _parse_xml(raw: bytes):
    """Parse XML bytes with two fallbacks for malformed content."""
    try:
        return ET.fromstring(raw)
    except ET.ParseError:
        pass
    try:
        return ET.fromstring(_INVALID_XML_CHARS.sub(b"", raw))
    except ET.ParseError:
        pass
    from lxml import etree
    return etree.fromstring(raw, parser=etree.XMLParser(recover=True))


def _regex_extract(raw: bytes) -> list[dict]:
    """Extract entries via regex on raw bytes — handles any XML corruption."""
    rows = []
    for block in _URL_BLOCK.finditer(raw):
        chunk = block.group(1)
        loc_m = _RX_LOC.search(chunk)
        if not loc_m:
            continue
        loc     = loc_m.group(1).decode("utf-8", errors="replace")
        lastmod = (_RX_LASTMOD.search(chunk) or type("", (), {"group": lambda s, n: b""})()).group(1)
        if isinstance(lastmod, bytes):
            lastmod = lastmod.decode("utf-8", errors="replace")
        img_cap = (_RX_CAPTION.search(chunk) or type("", (), {"group": lambda s, n: b""})()).group(1)
        if isinstance(img_cap, bytes):
            img_cap = img_cap.decode("utf-8", errors="replace")
        img_url_m = _RX_IMG_LOC.search(chunk)
        img_url = img_url_m.group(1).decode("utf-8", errors="replace") if img_url_m else ""

        date = time_ = ""
        if lastmod:
            parts = lastmod.split("T")
            date = parts[0]
            if len(parts) > 1:
                time_ = parts[1][:5]

        issuer = ""
        m = _PRNEWSFOTO.search(img_cap)
        if m:
            issuer = m.group(1).strip()

        rows.append({
            "date": date, "time": time_, "datetime": lastmod,
            "issuer": issuer, "image_url": img_url, "url": loc,
        })
    return rows


def _decompress(data: bytes) -> bytes:
    """Decompress gz bytes, tolerating truncated streams."""
    try:
        return gzip.decompress(data)
    except EOFError:
        # truncated gz — decompress what we have via zlib (wbits=47 = gzip format)
        import zlib
        d = zlib.decompressobj(wbits=47)
        try:
            return d.decompress(data)
        except zlib.error:
            return d.flush()


def _xml_to_rows(root) -> list[dict]:
    rows = []
    for url_el in root.findall("sm:url", NS):
        loc     = url_el.findtext("sm:loc",                    namespaces=NS) or ""
        lastmod = url_el.findtext("sm:lastmod",                namespaces=NS) or ""
        img_cap = url_el.findtext("image:image/image:caption", namespaces=NS) or ""
        img_url = url_el.findtext("image:image/image:loc",     namespaces=NS) or ""

        if not loc:
            continue

        date = time_ = ""
        if lastmod:
            parts = lastmod.split("T")
            date = parts[0]
            if len(parts) > 1:
                time_ = parts[1][:5]

        issuer = ""
        m = _PRNEWSFOTO.search(img_cap)
        if m:
            issuer = m.group(1).strip()

        rows.append({
            "date": date, "time": time_, "datetime": lastmod,
            "issuer": issuer, "image_url": img_url, "url": loc,
        })
    return rows


def parse_gz(gz_url: str) -> list[dict]:
    """Download, decompress, and parse one gz sitemap. Returns list of row dicts."""
    req = urllib.request.Request(gz_url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = _decompress(resp.read())

    root = _parse_xml(raw)
    rows = _xml_to_rows(root)

    # if XML parsing recovered far fewer entries than are in the raw bytes, fall
    # back to regex extraction which skips corrupt blocks individually
    loc_count = raw.count(b"<loc>")
    if loc_count > 0 and len(rows) < loc_count * 0.5:
        rows = _regex_extract(raw)

    return rows


def csv_path(month: str) -> str:
    return os.path.join(OUTPUT_DIR, f"prn_{month}.csv")


def load_done() -> set[str]:
    if not os.path.exists(DONE_FILE):
        return set()
    with open(DONE_FILE, encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def mark_done(gz_url: str):
    with open(DONE_FILE, "a", encoding="utf-8") as f:
        f.write(gz_url + "\n")


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

    print("Fetching gz index...")
    gz_urls = fetch_gz_index()
    print(f"  {len(gz_urls)} gz files found")

    done = set() if args.refetch else load_done()
    print(f"  {len(done)} months already done\n")

    # filter by month range
    filtered = []
    for gz_url in gz_urls:
        month = gz_to_month(gz_url)
        if not month:
            print(f"  SKIP unparseable: {gz_url}")
            continue
        if args.from_month and month < args.from_month:
            continue
        if args.to_month and month > args.to_month:
            continue
        filtered.append((gz_url, month))

    # sort chronologically
    filtered.sort(key=lambda x: x[1])
    print(f"Processing {len(filtered)} months\n")

    total_new = 0
    for gz_url, month in filtered:
        if gz_url in done:
            print(f"  {month}  already done — skip")
            continue

        print(f"  {month}  downloading...", end=" ", flush=True)
        try:
            rows = parse_gz(gz_url)
        except Exception as e:
            print(f"ERROR — {e}")
            continue

        write_month(rows, month)
        total_new += len(rows)
        mark_done(gz_url)
        print(f"{len(rows)} entries  ->  {csv_path(month)}")

        time.sleep(DELAY)

    print(f"\nDone. {total_new} total rows written to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
