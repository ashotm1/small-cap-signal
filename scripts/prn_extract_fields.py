"""
prn_extract_fields.py — fetch PRNewswire article pages and extract structured
fields.

PRN (Cision) is httpx-fetchable (no bot wall, unlike BusinessWire) and exposes a
JSON-LD NewsArticle — so this is the ANW-style httpx engine with BW-style
per-year output. Input is the already-filtered, classifier-tickered set, so it's
filter-first: fetch only the ~219k universe-tickered rows, not the 2.8M archive.

Structure (consistent 2010 -> 2026):
  * JSON-LD NewsArticle -> headline, datePublished (authoritative), dateModified
    (UNRELIABLE — many are a 2018 Cision mass-migration timestamp), description
    (lede), image. NB: no articleBody — body comes from the page.
  * og/name meta        -> og:title / og:description (lede) / og:image / keywords
  * section.release-body -> the FULL body (opens with the dateline, then prose +
    any financial tables).
  * dateline            -> "CITY, ST , <date> /PRNewswire/ --" at the body head.

Output columns are namespaced prn_* so they never collide with the passthrough
columns (input carries datetime/issuer/url/company/ticker/catalyst). Depends on
one input column: `url`.

Input:  data/prn_signal_filtered.csv (default; override with --input)
Output: per-year files data/prn_articles/prn_<year>_articles.csv (default, routed
        by each row's `datetime` year), or a single file via --output.

Each run tees output to logs/prn_extract_<ts>.log. Logs every non-success
explicitly, survives per-page parse errors, append-safe resume across files.
"""
import argparse
import asyncio
import csv
import glob
import html as _html  # avoid shadowing by extract_fields' `html` parameter
import json
import os
import random
import re
import sys
import time

import httpx
from bs4 import BeautifulSoup

# Bodies (with financial tables) can exceed the default 128 KB per-field cap.
_limit = sys.maxsize
while True:
    try:
        csv.field_size_limit(_limit)
        break
    except OverflowError:
        _limit //= 10

INPUT_CSV  = "data/prn_signal_filtered.csv"
OUTPUT_DIR = "data/prn_articles"   # per-year outputs land here (default)
LOG_DIR    = "logs"

EXTRACTED_FIELDS = [
    "prn_headline",        # JSON-LD headline / og:title / h1
    "prn_description",     # og:description (lede)
    "prn_date_published",  # JSON-LD datePublished (authoritative, ISO+TZ)
    "prn_date_modified",   # JSON-LD dateModified — UNRELIABLE (2018 migration)
    "prn_keywords",        # meta keywords (issuer + industry tags)
    "prn_image",           # JSON-LD image / og:image
    "prn_dateline",        # CITY, ST before "/PRNewswire/"
    "prn_tickers",         # EXCHANGE:SYM from body (input also carries ticker)
    "article_body",        # section.release-body full text
    "article_body_len",
    "http_status",
]

# Ticker in body lede: "(NYSE: ADS)", "(TSX, NYSE: BCE)", "[OTC: NIHDQ]".
# Colon-required + uppercase symbol (keeps prose like "Nasdaq under" out).
_TICKER_RE = re.compile(
    r"(?P<exchange>NYSE American|NYSE Arca|NYSE MKT|"
    r"NASDAQ Global Select Market|NASDAQ Global Market|NASDAQ Capital Market|"
    r"NASDAQ|NYSE|OTCQB|OTCQX|OTC Pink|OTCMKTS|OTCBB|OTC|"
    r"TSX Venture|TSXV|TSX|CBOE|CSE|AMEX|NEO)"
    r"\s*:\s*(?P<ticker>[A-Z][A-Z.]{0,5})\b",
    re.IGNORECASE)

# Trailing "<Mon> D, YYYY" date inside the dateline prefix, to strip to location.
_DL_DATE = re.compile(r"\s*,?\s*[A-Z][a-z]{2,8}\.?\s+\d{1,2},?\s*\d{4}\s*$")

_LINE_SEP_FIX = {0x2028: " ", 0x2029: " ", 0x85: " "}


def _clean_row(row: dict) -> dict:
    return {k: (v.translate(_LINE_SEP_FIX) if isinstance(v, str) else v)
            for k, v in row.items()}


def _stringify(v):
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        return "|".join(_stringify(x) for x in v if x not in (None, ""))
    if isinstance(v, dict):
        return v.get("name", "") or v.get("@id", "") or v.get("url", "") or ""
    return str(v)


def _first_jsonld_newsarticle(html: str) -> dict | None:
    for m in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html, re.DOTALL):
        try:
            data = json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            continue
        for it in (data if isinstance(data, list) else [data]):
            if isinstance(it, dict) and it.get("@type") == "NewsArticle":
                return it
    return None


def _meta(soup: BeautifulSoup) -> dict:
    out = {}
    for m in soup.find_all("meta"):
        key = m.get("property") or m.get("name") or m.get("itemprop")
        val = m.get("content")
        if key and val is not None and key not in out:
            out[key] = val
    return out


def _parse_tickers(text: str) -> str:
    seen = []
    for m in _TICKER_RE.finditer(text):
        tk = m.group("ticker")
        if not tk.isupper():
            continue
        exch = m.group("exchange").upper()
        exch = ("NASDAQ" if exch.startswith("NASDAQ")
                else "NYSE" if exch.startswith("NYSE") else exch)
        norm = f"{exch}:{tk}"
        if norm not in seen:
            seen.append(norm)
    return "|".join(seen)


def _parse_dateline(body: str) -> str:
    m = re.match(r"^\s*(.{2,90}?)\s*/PRNewswire/", body)
    if not m:
        return ""
    return _DL_DATE.sub("", m.group(1)).strip(" , ")


def extract_fields(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    meta = _meta(soup)
    ld = _first_jsonld_newsarticle(html) or {}

    headline = _stringify(ld.get("headline")) or meta.get("og:title", "")
    if not headline:
        h1 = soup.find("h1")
        headline = h1.get_text(" ", strip=True) if h1 else ""

    body_el = soup.find("section", class_="release-body") or \
        soup.find("article", class_="news-release")
    body = body_el.get_text(" ", strip=True) if body_el else ""

    # JSON-LD fields can be HTML-double-encoded (json.loads doesn't decode
    # entities like BeautifulSoup does for the body/og fields).
    return {
        "prn_headline":       _html.unescape(headline),
        "prn_description":    _html.unescape(meta.get("og:description", "") or _stringify(ld.get("description"))),
        "prn_date_published": _stringify(ld.get("datePublished")),
        "prn_date_modified":  _stringify(ld.get("dateModified")),
        "prn_keywords":       _html.unescape(meta.get("keywords", "")),
        "prn_image":          _stringify(ld.get("image")) or meta.get("og:image", ""),
        "prn_dateline":       _parse_dateline(body),
        "prn_tickers":        _parse_tickers(body),
        "article_body":       body,
        "article_body_len":   str(len(body)),
    }


class _Tee:
    """Writes to multiple streams so every print also hits the run log file."""
    def __init__(self, *streams):
        self._streams = streams

    def write(self, s):
        for st in self._streams:
            st.write(s)

    def flush(self):
        for st in self._streams:
            st.flush()


async def fetch_one(client: httpx.AsyncClient, url: str, attempts: int = 3) -> tuple[int, str]:
    """Fetch URL with bounded retry. Returns (status, body) or (0, '') on hard fail."""
    delay = 1.0
    for _ in range(attempts):
        try:
            r = await client.get(url, timeout=20.0, follow_redirects=True)
            if r.status_code == 200:
                return 200, r.text
            if r.status_code in (429, 500, 502, 503, 504):
                await asyncio.sleep(delay + random.uniform(0, 0.5))
                delay *= 2
                continue
            return r.status_code, ""
        except (httpx.RequestError, httpx.HTTPError):
            await asyncio.sleep(delay + random.uniform(0, 0.5))
            delay *= 2
    return 0, ""


async def worker(queue, client, get_writer, lock, args, state):
    while True:
        row = await queue.get()
        if row is None:
            queue.task_done()
            return
        url = row["url"]
        if not url.startswith("http"):
            status, html = 0, ""
        else:
            status, html = await fetch_one(client, url)
        if status == 200:
            try:
                fields = extract_fields(html)
            except Exception as e:  # one bad page must not kill the run
                status = "parse_err"
                fields = {k: "" for k in EXTRACTED_FIELDS if k != "http_status"}
                print(f"  PARSE-ERR url=...{url[-55:]}: {type(e).__name__}: {e}", flush=True)
        else:
            fields = {k: "" for k in EXTRACTED_FIELDS if k != "http_status"}
        fields["http_status"] = str(status)
        out_row = _clean_row({**row, **fields})
        async with lock:
            fh, writer = get_writer(row)
            writer.writerow(out_row)
            fh.flush()   # durable per row — flush is cheap vs the network fetch
            state["written"] += 1
            if status != 200:
                state["errors"] += 1
                print(f"  ERR status={status} {state['written']}/{state['total']} "
                      f"url=...{url[-55:]}", flush=True)
            elif state["written"] % 200 == 0:
                rate = state["written"] / max(time.time() - state["t0"], 1)
                print(f"  written={state['written']}/{state['total']} "
                      f"({rate:.1f}/s, {state['errors']} err)", flush=True)
        await asyncio.sleep(random.uniform(args.delay_min, args.delay_max))
        queue.task_done()


async def main_async(args):
    if not os.path.exists(args.input):
        print(f"missing input: {args.input}")
        sys.exit(1)
    with open(args.input, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        in_fields = reader.fieldnames
        rows = list(reader)
    if "url" not in (in_fields or []):
        print(f"input {args.input} has no 'url' column (cols: {in_fields})")
        sys.exit(1)
    print(f"input rows: {len(rows)}")

    single = args.output is not None
    if single:
        existing = [args.output] if os.path.exists(args.output) else []
    else:
        os.makedirs(args.out_dir, exist_ok=True)
        existing = sorted(glob.glob(os.path.join(args.out_dir, "prn_*.csv")))

    done_urls = set()
    for path in existing:
        with open(path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                done_urls.add(r["url"])
    if done_urls:
        print(f"resuming — {len(done_urls)} URLs already done")

    todo = [r for r in rows if r["url"] not in done_urls]
    if args.limit:
        todo = todo[: args.limit]
    print(f"to fetch: {len(todo)}")
    if not todo:
        return

    fieldnames = list(in_fields) + EXTRACTED_FIELDS
    writers = {}  # key -> (fh, writer); one per year unless --output (single)

    def get_writer(row):
        key = "_single" if single else (row.get("datetime", "")[:4] or "unknown")
        if key not in writers:
            path = args.output if single else os.path.join(
                args.out_dir, f"prn_{key}_articles.csv")
            new = not os.path.exists(path) or os.path.getsize(path) == 0
            fh = open(path, "a", newline="", encoding="utf-8")
            w = csv.DictWriter(fh, fieldnames=fieldnames)
            if new:
                w.writeheader()
            writers[key] = (fh, w)
            if not single:
                print(f"  -> writing {path}", flush=True)
        return writers[key]

    state = {"written": 0, "errors": 0, "total": len(todo), "t0": time.time()}
    lock = asyncio.Lock()
    queue: asyncio.Queue = asyncio.Queue()
    for row in todo:
        await queue.put(row)
    for _ in range(args.workers):
        await queue.put(None)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }
    async with httpx.AsyncClient(headers=headers, timeout=20.0) as client:
        tasks = [asyncio.create_task(worker(queue, client, get_writer, lock, args, state))
                 for _ in range(args.workers)]
        await asyncio.gather(*tasks)

    for fh, _ in writers.values():
        fh.close()
    elapsed = time.time() - state["t0"]
    print(f"\ndone. wrote {state['written']} rows ({state['errors']} errors) in "
          f"{elapsed:.1f}s ({state['written']/max(elapsed,1):.2f} rows/s)")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input",  default=INPUT_CSV)
    p.add_argument("--output", default=None,
                   help="single output file; omit for per-year split into --out-dir")
    p.add_argument("--out-dir", dest="out_dir", default=OUTPUT_DIR,
                   help="per-year output dir (default data/prn_articles)")
    p.add_argument("--workers", type=int, default=6)
    p.add_argument("--delay-min", type=float, default=0.3)
    p.add_argument("--delay-max", type=float, default=1.0)
    p.add_argument("--limit", type=int, default=None,
                   help="only fetch first N URLs (for testing)")
    args = p.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(
        LOG_DIR, f"prn_extract_{time.strftime('%Y%m%d_%H%M%S')}.log")
    log_file = open(log_path, "a", encoding="utf-8", buffering=1)
    sys.stdout = _Tee(sys.stdout, log_file)
    sys.stderr = _Tee(sys.stderr, log_file)
    print(f"[log] {log_path}", flush=True)

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
