"""
bw_extract_fields.py — fetch BusinessWire article pages and extract structured
fields.

BW is behind Akamai Bot Manager: plain HTTP gets 403. Like bw_scraper.py, this
attaches Playwright to a real warmed Chrome over CDP (persisted Akamai cookies
in the isolated profile carry the challenge). It fetches with a SINGLE tab,
serially, with randomized human-paced delays — at equal throughput that reads
as less bot-like than several tabs firing in lockstep bursts from one session.

Structure (consistent 2021 -> 2026, one re-migrated template):
  * JSON-LD NewsArticle  -> headline, datePublished/Modified (ISO+TZ), author
                            (contact, NOT the issuer), image.
                            NB: its articleBody is truncated (~160 ch) — useless
                            as the body.
  * og/twitter meta      -> og:title / og:description (lede) / og:image.
  * #bw-release-story div -> the FULL body (opens with the dateline, ends with
                            the contact footer).
  * #bw-release-subhead   -> sub-headline.
  * dateline             -> "LOCATION--( BUSINESS WIRE )--" at the body head;
                            100% reliable in sampling.

Output columns are namespaced bw_* so they never collide with passthrough
columns (input already has datetime/ticker/exchange/title/url). Depends on one
input column: `url`.

Input:  data/bw_signal_filtered.csv (default; override with --input)
Output: per-year files data/bw_articles/bw_<year>_articles.csv (default, routed by
        each row's `datetime` year), or a single file via --output.

Each run tees all output to logs/bw_extract_<ts>.log. Logs every non-success
explicitly and survives per-page parse errors. A run of consecutive Akamai
blocks self-aborts WITHOUT writing those rows (so a rerun retries them) to avoid
burning the warmed cookies.

Append-safe: skips URLs already present across the output file(s).
"""
import argparse
import asyncio
import csv
import glob
import json
import os
import random
import re
import sys
import time

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# Reuse the scraper's Chrome/CDP helpers + ticker regex (keeps them in sync).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scraper"))
import bw_scraper as bws  # noqa: E402

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

INPUT_CSV   = "data/bw_signal_filtered.csv"
OUTPUT_DIR  = "data/bw_articles"   # per-year outputs land here (default)
LOG_DIR     = "logs"
DEBUG_PORT  = 9222
PROFILE     = r"C:\bw-chrome-profile"

EXTRACTED_FIELDS = [
    "bw_headline",         # JSON-LD headline / og:title / h1
    "bw_subhead",          # #bw-release-subhead
    "bw_description",      # og:description (lede)
    "bw_date_published",   # JSON-LD datePublished (ISO+TZ)
    "bw_date_modified",    # JSON-LD dateModified
    "bw_author",           # JSON-LD author.name — contact, NOT the issuer
    "bw_image",            # JSON-LD image / og:image
    "bw_dateline",         # LOCATION from "--( BUSINESS WIRE )--"
    "bw_tickers",          # EXCHANGE:SYM from body (input also carries ticker)
    "article_body",        # #bw-release-story full text
    "article_body_len",
    "nav_status",
]

BLOCK_MARKERS = ("Access Denied", "Pardon the interruption", "Reference&#32;#",
                 "Reference #", "errors.edgesuite.net")

# "TYLER, Texas--( BUSINESS WIRE )--" at the very start of the body.
_DATELINE_RE = re.compile(r"^\s*(.{2,60}?)\s*--\s*\(\s*BUSINESS WIRE\s*\)\s*--",
                          re.IGNORECASE)

# Scraped HTML occasionally carries "unusual" line terminators (LS/PS/NEL).
_LINE_SEP_FIX = {0x2028: " ", 0x2029: " ", 0x85: " "}


def _clean_row(row: dict) -> dict:
    return {k: (v.translate(_LINE_SEP_FIX) if isinstance(v, str) else v)
            for k, v in row.items()}


def _stringify(v):
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        return "|".join(_stringify(x) for x in v)
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


# Body-text ticker matcher. Unlike the scraper's listing regex, this REQUIRES a
# colon (prose like "Nasdaq under the symbol" must not match) and the symbol
# must be genuinely uppercase (guards "Nasdaq: under review").
_BODY_TICKER_RE = re.compile(
    r"(?P<exchange>NYSE American|NYSE Arca|NYSE MKT|"
    r"NASDAQ Global Select Market|NASDAQ Global Market|NASDAQ Capital Market|"
    r"NASDAQ GSM|NASDAQ GS|NASDAQ CM|NASDAQ|NYSE|"
    r"OTCQB|OTCQX|OTC Pink|OTCMKTS|OTC|TSX Venture|TSXV|TSX|CBOE|CSE)"
    r"\s*:\s*(?P<ticker>[A-Z][A-Z.]{0,5})\b",
    re.IGNORECASE)


def _parse_tickers(text: str) -> str:
    seen = []
    for m in _BODY_TICKER_RE.finditer(text):
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


def extract_fields(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    meta = _meta(soup)
    ld = _first_jsonld_newsarticle(html) or {}

    headline = _stringify(ld.get("headline")) or meta.get("og:title", "")
    if not headline:
        h1 = soup.find("h1")
        headline = h1.get_text(" ", strip=True) if h1 else ""

    subhead_el = soup.find(id="bw-release-subhead")
    subhead = subhead_el.get_text(" ", strip=True) if subhead_el else ""

    body_el = soup.find(id="bw-release-story") or soup.find(class_="bw-release-body")
    body = body_el.get_text(" ", strip=True) if body_el else ""

    dl = _DATELINE_RE.match(body)
    dateline = dl.group(1).strip() if dl else ""

    return {
        "bw_headline":       headline,
        "bw_subhead":        subhead,
        "bw_description":    meta.get("og:description", "") or meta.get("description", ""),
        "bw_date_published": _stringify(ld.get("datePublished")),
        "bw_date_modified":  _stringify(ld.get("dateModified")),
        "bw_author":         _stringify(ld.get("author")),
        "bw_image":          _stringify(ld.get("image")) or meta.get("og:image", ""),
        "bw_dateline":       dateline,
        "bw_tickers":        _parse_tickers(body),
        "article_body":      body,
        "article_body_len":  str(len(body)),
    }


async def fetch_one(page, url: str) -> tuple[str, str]:
    """Navigate one URL. Returns (nav_status, html). 'blocked'/'0' on trouble."""
    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except Exception:
        return "0", ""
    await page.wait_for_timeout(random.randint(700, 1500))
    # Mimic a human reading the article. Reuse the scraper's proven mix of
    # interpolated mouse moves + incremental scrolls + pauses — smooth, not the
    # single abrupt wheel jump this used to do.
    try:
        await bws.simulate_human(page)
    except Exception:
        pass
    html = await page.content()
    status = str(resp.status) if resp else "200"
    if any(mk in html[:4000] for mk in BLOCK_MARKERS):
        return "blocked", html
    return status, html


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

    # Output: per-year split into --out-dir (default), or a single --output file.
    single = args.output is not None
    if single:
        existing = [args.output] if os.path.exists(args.output) else []
    else:
        os.makedirs(args.out_dir, exist_ok=True)
        existing = sorted(glob.glob(os.path.join(args.out_dir, "bw_*.csv")))

    done_urls = set()
    for path in existing:
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                done_urls.add(row["url"])
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

    def writer_for(row):
        key = "_single" if single else (row.get("datetime", "")[:4] or "unknown")
        if key not in writers:
            path = args.output if single else os.path.join(
                args.out_dir, f"bw_{key}_articles.csv")
            new = not os.path.exists(path) or os.path.getsize(path) == 0
            fh = open(path, "a", newline="", encoding="utf-8")
            w = csv.DictWriter(fh, fieldnames=fieldnames)
            if new:
                w.writeheader()
            writers[key] = (fh, w)
        return writers[key]

    try:
        launched = bws.ensure_chrome(args.debug_port, args.chrome_profile, args.chrome_exe)
    except Exception as e:
        print(f"Could not start/find Chrome on CDP port {args.debug_port}: {e}")
        sys.exit(1)

    written = errors = consec_block = 0
    t0 = time.time()
    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp(f"http://127.0.0.1:{args.debug_port}")
        except Exception as e:
            print(f"Failed to connect via CDP on port {args.debug_port}: {e}")
            sys.exit(1)
        ctx = browser.contexts[0] if browser.contexts else await browser.new_context()
        await bws._cleanup_existing_pages(ctx, launched)
        page = await ctx.new_page()

        for row in todo:
            url = row["url"]
            if not url.startswith("http"):
                status, html = "0", ""
            else:
                status, html = await fetch_one(page, url)

            # A block is SESSION state, not a property of the URL: don't write the
            # row, so a rerun retries it after re-warming. Counts toward the abort.
            if status == "blocked":
                consec_block += 1
                print(f"  BLOCKED ({consec_block}) url=...{url[-55:]}", flush=True)
                if consec_block >= args.block_abort:
                    print(f"\n{consec_block} consecutive blocks — Akamai flagged the "
                          f"session. Stopping to preserve cookies. Re-warm Chrome "
                          f"(visit a BW page manually) and rerun to resume.")
                    break
                continue
            consec_block = 0

            if status == "200" and html:
                try:
                    fields = extract_fields(html)
                except Exception as e:  # one bad page must not kill a 10-day run
                    status = "parse_err"
                    fields = {k: "" for k in EXTRACTED_FIELDS if k != "nav_status"}
                    print(f"  PARSE-ERR url=...{url[-55:]}: {type(e).__name__}: {e}",
                          flush=True)
            else:
                fields = {k: "" for k in EXTRACTED_FIELDS if k != "nav_status"}
            fields["nav_status"] = status

            fh, w = writer_for(row)
            w.writerow(_clean_row({**row, **fields}))
            written += 1
            if status != "200":                     # log EVERY failure, not every 5th
                errors += 1
                fh.flush()
                print(f"  ERR nav={status} {written}/{len(todo)} url=...{url[-55:]}",
                      flush=True)
            elif written % 5 == 0:
                fh.flush()
                print(f"  written={written}/{len(todo)} last=200 url=...{url[-55:]}",
                      flush=True)
            await asyncio.sleep(random.uniform(args.delay_min, args.delay_max))

        await page.close()

    for fh, _ in writers.values():
        fh.close()
    elapsed = time.time() - t0
    print(f"\ndone. wrote {written} rows ({errors} errors) in {elapsed:.1f}s "
          f"({written/max(elapsed,1):.2f} rows/s)")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input",  default=INPUT_CSV)
    p.add_argument("--output", default=None,
                   help="single output file; omit for per-year split into --out-dir")
    p.add_argument("--out-dir", dest="out_dir", default=OUTPUT_DIR,
                   help="per-year output dir (default data/bw_articles)")
    p.add_argument("--limit", type=int, default=None, help="fetch first N (testing)")
    p.add_argument("--delay-min", type=float, default=1.5)
    p.add_argument("--delay-max", type=float, default=4.0)
    p.add_argument("--block-abort", type=int, default=3,
                   help="stop after this many consecutive Akamai blocks")
    p.add_argument("--debug-port", type=int, default=DEBUG_PORT)
    p.add_argument("--chrome-profile", default=PROFILE)
    p.add_argument("--chrome-exe", default=None)
    args = p.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(
        LOG_DIR, f"bw_extract_{time.strftime('%Y%m%d_%H%M%S')}.log")
    log_file = open(log_path, "a", encoding="utf-8", buffering=1)
    sys.stdout = bws._Tee(sys.stdout, log_file)
    sys.stderr = bws._Tee(sys.stderr, log_file)
    print(f"[log] {log_path}", flush=True)

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
