"""
anw_extract_fields.py — fetch ACCESS Newswire article pages and extract
structured fields.

Unlike GlobeNewswire, ANW pages carry NO JSON-LD. Everything structured lives
in <meta og:*>/<meta name=*> tags + <h1>, and the body is in a single
<div class="articlecopy">. The template is identical across the whole archive
(2007 -> 2026; the site re-migrated all history into one layout), so one
extractor covers every month — no era branching.

Output columns are namespaced (anw_*) so they never collide with passthrough
columns from the input (which already carries date/language/industry/url and,
once the upstream filter exists, classifier columns too). The extractor depends
on exactly one input column: `url`. Everything else is passed through verbatim.

Fields split into two groups:
  * structurally given  — read straight off meta tags / designated divs
  * parsed (best-effort) — dateline, tickers, source company; only in body text,
                           may be blank. See the parser helpers for the rules.

By default this processes EVERY monthly sitemap CSV in data/anw/ (the job the
old run_all_anw_extract.py did, now built in — no subprocess double-hop),
writing one output per month to data/anw_articles/anw_<month>_articles.csv.
Restrict with --from/--to (YYYY-MM, like anw_scraper.py) or repoint --folder.
Pass --input/--output instead to run a single consolidated file (e.g. a
signal-filtered set). Each run tees all output to logs/anw_extract_<ts>.log.

Usage:
    python scripts/anw_extract_fields.py                       # all months in data/anw/
    python scripts/anw_extract_fields.py --from 2024-01        # 2024-01 onward
    python scripts/anw_extract_fields.py --from 2023-01 --to 2023-12
    python scripts/anw_extract_fields.py --limit 50            # first 50 per month (testing)
    python scripts/anw_extract_fields.py --input data/anw_signal_filtered.csv \
                                         --output data/anw_signal_articles.csv

Append-safe: skips URLs already present in each output. A month that errors is
logged and skipped; the run continues. Concurrency-limited fetches with jittered
delay + bounded retries.
"""
import argparse
import asyncio
import csv
import glob
import os
import random
import re
import sys
import time
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

# Article bodies can exceed the default 128 KB per-field cap. Raise it as high
# as the platform allows (sys.maxsize overflows the csv module's C long on
# Windows, so back off until it's accepted).
_limit = sys.maxsize
while True:
    try:
        csv.field_size_limit(_limit)
        break
    except OverflowError:
        _limit //= 10

INPUT_DIR  = "data/anw"           # default folder of monthly sitemap CSVs
OUTPUT_DIR = "data/anw_articles"  # per-month outputs land here
LOG_DIR    = "logs"
INPUT_CSV  = "data/anw_signal_filtered.csv"   # --input single-file mode default
OUTPUT_CSV = "data/anw_signal_articles.csv"   # --output single-file mode default

_MONTH_RE = re.compile(r"anw_(\d{4}-\d{2})\.csv$")

# Input columns are passed through; we append these (all namespaced anw_*):
EXTRACTED_FIELDS = [
    # --- structurally given ---
    "anw_title",            # og:title / h1 — the real, untruncated title
    "anw_description",      # og:description — lede
    "anw_author",           # og:article:author — distributor, NOT always issuer
    "anw_published_time",   # og:article:published_time -> ISO (has intraday time)
    "anw_topic",            # og:article:tag (absent on pre-2014 articles)
    "anw_keywords",         # meta name=keywords (present ~half the time)
    "anw_locale",           # og:locale
    "anw_image",            # og:image
    # --- parsed (best-effort, may be blank) ---
    "anw_dateline",         # "CITY, ST" from body prefix (multi-format)
    "anw_tickers",          # pipe-joined (EXCHANGE:SYM) matches from body
    "anw_source_company",   # body "SOURCE:" line, fallback author
    # --- body + meta ---
    "article_body",
    "article_body_len",
    "http_status",
]


# ---------------------------------------------------------------------------
# parsers for fields ANW does not expose structurally
# ---------------------------------------------------------------------------

# Date/wire markers used to confirm a leading "Place" really is a dateline.
_MONTHS = (r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?")
_WIRE   = r"(?:Accesswire|ACCESS\s+Newswire|ACCESSWIRE)"

# Dateline / location. Tried in order; all anchored at the body start.
_DATELINE_PATTERNS = [
    # Modern + 2009: "ROCHESTER, MN /", "FOSTER CITY, CA /", "CHICAGO, IL - ".
    # City words then a 2-letter US/CA state.
    re.compile(r"^\s*([A-Z][A-Za-z.\-&']*(?:\s+[A-Z][A-Za-z.\-&']*)*,\s+[A-Z]{2})\b"),
    # Date-first (2013 NTG): "September 3, 2013, Toronto, Ontario." -> "Toronto, Ontario".
    re.compile(r"^\s*[A-Z][a-z]+\s+\d{1,2},\s+\d{4},\s+"
               r"([A-Z][a-zA-Z]+(?:,\s+[A-Z][a-zA-Z]+)?)"),
    # Place-first with a full-word region (Canadian PRs):
    # "Val-d'Or, Quebec, September 3..." / "Vancouver, British Columbia - September 3...".
    # Require a date/wire marker right after to avoid grabbing a company name.
    re.compile(r"^\s*([A-Z][A-Za-z.\-']+(?:\s+[A-Z][A-Za-z.\-']+)*"
               r",\s+[A-Z][A-Za-z.\-']+(?:\s+[A-Z][A-Za-z.\-']+){0,2})"
               r"\s*[-,(]\s*(?=" + _WIRE + r"|" + _MONTHS + r"|\d{1,2}[,\s])"),
]

# Exchange + symbol. The separator is ALWAYS the colon — any hyphen (e.g.
# "TSX-V") belongs to the exchange name. Longer aliases precede shorter so
# "NYSE American"/"TSX VENTURE" win over "NYSE"/"TSX". Lookbehind keeps it from
# matching inside a word; uppercase-leading symbol keeps it out of prose.
_EXCH = (r"NASDAQ|NYSE\s+American|NYSE\s+MKT|NYSE\s+ARCA|NYSE|"
         r"TSX[\s.\-]?VENTURE|TSX[\s.\-]?V|TSXV|TSX|"
         r"OTCQB|OTCQX|OTC\s*PINK|OTCMKTS|OTC\s*BB|OTCBB|OTC|"
         r"CSE|CBOE|AMEX|NEO")
_TICKER_RE = re.compile(
    r"(?<![A-Za-z])(" + _EXCH + r")\s*:\s*([A-Z][A-Z0-9.]{0,8})(?=[)\s,;.])")

# Trailing "SOURCE: <issuer>" line (newline-joined body text).
_SOURCE_RE = re.compile(r'(?im)^\s*SOURCE:\s*(.+?)\s*$')


def _parse_dateline(text: str) -> str:
    head = text[:200]
    for pat in _DATELINE_PATTERNS:
        m = pat.search(head)
        if m:
            return m.group(1).strip()
    return ""


def _parse_tickers(text: str) -> str:
    seen = []
    for m in _TICKER_RE.finditer(text):
        exch = re.sub(r"\s+", " ", m.group(1)).upper().strip()
        # Collapse the TSX Venture aliases (TSX VENTURE / TSX-V / TSX.V) -> TSXV.
        exch = re.sub(r"^TSX[ .\-]?(?:VENTURE|V)$", "TSXV", exch)
        norm = f"{exch}:{m.group(2)}"
        if norm not in seen:
            seen.append(norm)
    return "|".join(seen)


def _parse_source(text: str) -> str:
    m = _SOURCE_RE.search(text)
    return m.group(1).strip() if m else ""


def _to_iso(s: str) -> str:
    """ANW publish time is 'MM/DD/YYYY HH:MM:SS'; return ISO, or raw on failure."""
    s = (s or "").strip()
    if not s:
        return ""
    try:
        return datetime.strptime(s, "%m/%d/%Y %H:%M:%S").isoformat()
    except ValueError:
        return s


# ---------------------------------------------------------------------------
# field extraction
# ---------------------------------------------------------------------------

def _extract_meta(soup: BeautifulSoup) -> dict:
    """Return {property_or_name: content}, first occurrence wins.

    First-wins matters: ANW emits the article <meta name=description> before a
    second site-boilerplate one. (We read og:description anyway, which is
    single-occurrence, but keep the rule for any name= reads.)
    """
    out = {}
    for m in soup.find_all("meta"):
        key = m.get("property") or m.get("name") or m.get("itemprop")
        val = m.get("content")
        if key and val is not None and key not in out:
            out[key] = val
    return out


def extract_fields(html: str) -> dict:
    """Parse an ANW article page into the EXTRACTED_FIELDS schema."""
    soup = BeautifulSoup(html, "html.parser")
    meta = _extract_meta(soup)

    title = (meta.get("og:title") or "").strip()
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else ""

    body_el = soup.find(class_="articlecopy")
    # Newline-joined copy preserves line structure for the parsers (dateline at
    # the head, SOURCE on its own line); space-joined copy is what we store, to
    # match the GNW article_body convention.
    body_lines = body_el.get_text("\n", strip=True) if body_el else ""
    body       = body_el.get_text(" ",  strip=True) if body_el else ""

    author = meta.get("og:article:author", "") or meta.get("author", "")

    return {
        "anw_title":          title,
        "anw_description":    meta.get("og:description", "") or meta.get("description", ""),
        "anw_author":         author,
        "anw_published_time": _to_iso(meta.get("og:article:published_time", "")),
        "anw_topic":          meta.get("og:article:tag", ""),
        "anw_keywords":       meta.get("keywords", ""),
        "anw_locale":         meta.get("og:locale", ""),
        "anw_image":          meta.get("og:image", ""),
        "anw_dateline":       _parse_dateline(body_lines),
        "anw_tickers":        _parse_tickers(body_lines),
        "anw_source_company": _parse_source(body_lines) or author,
        "article_body":       body,
        "article_body_len":   str(len(body)),
    }


# ---------------------------------------------------------------------------
# fetch + row plumbing (mirrors gnw_extract_fields.py)
# ---------------------------------------------------------------------------

# Scraped HTML occasionally carries "unusual" line terminators (LS/PS/NEL).
# Map each to a space — 1:1, so field lengths (and article_body_len) are kept.
_LINE_SEP_FIX = {0x2028: " ", 0x2029: " ", 0x85: " "}


def _clean_row(row: dict) -> dict:
    return {k: (v.translate(_LINE_SEP_FIX) if isinstance(v, str) else v)
            for k, v in row.items()}


async def fetch_one(client: httpx.AsyncClient, url: str, attempts: int = 3) -> tuple[int, str]:
    """Fetch URL with bounded retry. Returns (status, body) or (0, '') on hard fail."""
    delay = 1.0
    for i in range(attempts):
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


async def worker(queue: asyncio.Queue, client, writer, lock, args, state):
    while True:
        row = await queue.get()
        if row is None:
            queue.task_done()
            return
        url = row["url"]
        # A few raw-sitemap rows carry relative (foreign-language) URLs like
        # "/viewarticle?id=...&lang=es"; httpx raises InvalidURL on those.
        # Treat as a hard miss instead of crashing the worker.
        if not url.startswith("http"):
            status, html = 0, ""
        else:
            status, html = await fetch_one(client, url)
        if status == 200:
            try:
                fields = extract_fields(html)
            except Exception as e:  # one malformed page must not kill the run
                status = "parse_err"
                fields = {k: "" for k in EXTRACTED_FIELDS if k != "http_status"}
                print(f"  [{state['label']}] PARSE-ERR url=...{url[-55:]}: "
                      f"{type(e).__name__}: {e}", flush=True)
        else:
            fields = {k: "" for k in EXTRACTED_FIELDS if k != "http_status"}
        fields["http_status"] = str(status)
        out_row = _clean_row({**row, **fields})
        async with lock:
            writer.writerow(out_row)
            state["fh"].flush()   # durable per row — flush is cheap vs the network fetch
            state["written"] += 1
            if status != 200:                       # log EVERY failure, not every 5th
                state["errors"] += 1
                print(f"  [{state['label']}] ERR status={status} "
                      f"{state['written']}/{state['total']} url=...{url[-55:]}", flush=True)
            elif state["written"] % 100 == 0:
                rate = state["written"] / max(time.time() - state["t0"], 1)
                print(f"  [{state['label']}] written={state['written']}/{state['total']} "
                      f"({rate:.2f}/s, {state['errors']} err)", flush=True)
        await asyncio.sleep(random.uniform(args.delay_min, args.delay_max))
        queue.task_done()


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


async def process_file(client, in_csv, out_csv, args, run):
    """Extract one CSV -> out_csv. Append-safe; accumulates into run totals."""
    label = os.path.basename(in_csv)
    with open(in_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        in_fields = reader.fieldnames
        rows = list(reader)
    if "url" not in (in_fields or []):
        print(f"  [{label}] no 'url' column (cols: {in_fields}) — skip")
        return

    done = set()
    if os.path.exists(out_csv):
        with open(out_csv, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                done.add(r["url"])

    todo = [r for r in rows if r["url"] not in done]
    if args.limit:
        todo = todo[: args.limit]
    if not todo:
        print(f"  [{label}] {len(rows)} rows, all done — skip")
        return
    print(f"  [{label}] {len(rows)} rows, {len(done)} done, fetching {len(todo)}")

    fieldnames = list(in_fields) + EXTRACTED_FIELDS
    write_header = not os.path.exists(out_csv) or os.path.getsize(out_csv) == 0
    fh = open(out_csv, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(fh, fieldnames=fieldnames)
    if write_header:
        writer.writeheader()

    state = {"written": 0, "errors": 0, "total": len(todo), "fh": fh,
             "label": label, "t0": time.time()}
    lock = asyncio.Lock()
    queue: asyncio.Queue = asyncio.Queue()
    for row in todo:
        await queue.put(row)
    for _ in range(args.workers):
        await queue.put(None)
    tasks = [asyncio.create_task(worker(queue, client, writer, lock, args, state))
             for _ in range(args.workers)]
    await asyncio.gather(*tasks)

    fh.close()
    run["written"] += state["written"]
    run["errors"] += state["errors"]
    run["files"] += 1
    print(f"  [{label}] wrote {state['written']} ({state['errors']} errors)  ->  {out_csv}")


def _build_jobs(args):
    """Return [(in_csv, out_csv), ...]: a single --input, or all months in --folder."""
    if args.input:
        return [(args.input, args.output)]
    os.makedirs(args.out_dir, exist_ok=True)
    jobs = []
    for path in sorted(glob.glob(os.path.join(args.folder, "anw_*.csv"))):
        m = _MONTH_RE.search(os.path.basename(path))
        if not m:
            continue
        month = m.group(1)
        if args.from_month and month < args.from_month:
            continue
        if args.to_month and month > args.to_month:
            continue
        jobs.append((path, os.path.join(args.out_dir, f"anw_{month}_articles.csv")))
    return jobs


async def main_async(args):
    jobs = _build_jobs(args)
    if not jobs:
        print(f"no monthly CSVs in {args.folder} for range "
              f"[{args.from_month or '...'} .. {args.to_month or '...'}]")
        return
    print(f"jobs: {len(jobs)}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }
    run = {"written": 0, "errors": 0, "files": 0}
    t0 = time.time()
    async with httpx.AsyncClient(headers=headers, timeout=20.0) as client:
        for in_csv, out_csv in jobs:
            if not os.path.exists(in_csv):
                print(f"  missing input: {in_csv} — skip")
                continue
            try:
                await process_file(client, in_csv, out_csv, args, run)
            except Exception as e:
                print(f"  ERROR on {in_csv}: {type(e).__name__}: {e} — continuing")
    elapsed = time.time() - t0
    print(f"\ndone. {run['files']} file(s), wrote {run['written']} new rows "
          f"({run['errors']} errors) in {elapsed:.1f}s "
          f"({run['written']/max(elapsed,1):.2f} rows/s)")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--folder", default=INPUT_DIR,
                   help="folder of monthly anw_*.csv (default data/anw)")
    p.add_argument("--out-dir", dest="out_dir", default=OUTPUT_DIR,
                   help="per-month output dir (default data/anw_articles)")
    p.add_argument("--from", dest="from_month", help="start month YYYY-MM (inclusive)")
    p.add_argument("--to",   dest="to_month",   help="end month YYYY-MM (inclusive)")
    p.add_argument("--input",  default=None,
                   help="single CSV to process instead of the folder")
    p.add_argument("--output", default=OUTPUT_CSV,
                   help="output for --input single-file mode")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--delay-min", type=float, default=0.3)
    p.add_argument("--delay-max", type=float, default=1.0)
    p.add_argument("--limit", type=int, default=None,
                   help="only fetch first N URLs per file (for testing)")
    args = p.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(
        LOG_DIR, f"anw_extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    log_file = open(log_path, "a", encoding="utf-8", buffering=1)
    sys.stdout = _Tee(sys.stdout, log_file)
    sys.stderr = _Tee(sys.stderr, log_file)
    print(f"[log] {log_path}", flush=True)

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
