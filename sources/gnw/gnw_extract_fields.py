"""
gnw_extract_fields.py — fetch GlobeNewswire article pages and extract
structured fields (JSON-LD NewsArticle + selected <meta> tags).

Input:  data/gnw_signal_filtered.csv (URLs + original gnw_news fields)
Output: data/gnw_signal_articles.csv (input fields + extracted fields)

Append-safe: skip URLs already in output. Concurrency-limited fetches with
jittered delay + bounded retries.
"""
import argparse
import asyncio
import csv
import json
import os
import random
import re
import sys
import time

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

INPUT_CSV  = "data/gnw_signal_filtered.csv"
OUTPUT_CSV = "data/gnw_signal_articles.csv"

# Original gnw_signal_filtered fields are passed through; we append these:
EXTRACTED_FIELDS = [
    "ld_headline",
    "ld_description",
    "ld_dateline",
    "ld_date_published",
    "ld_date_modified",
    "ld_in_language",
    "ld_keywords",
    "ld_article_section",
    "ld_author_name",
    "ld_source_org",
    "ld_location",
    "meta_keywords",
    "meta_author",
    "meta_dc_date_issued",
    "article_body",
    "article_body_len",
    "http_status",
]

_JSONLD_RE = re.compile(
    r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
    re.DOTALL,
)
_META_RE = re.compile(r'<meta\s+([^>]+?)/?>', re.IGNORECASE)
_ATTR_RE = re.compile(r'(\w[\w\-:.]*)\s*=\s*"([^"]*)"')


def _extract_meta(html: str) -> dict:
    """Return {name_or_property: content} from <meta> tags."""
    out = {}
    for m in _META_RE.finditer(html):
        attrs = dict(_ATTR_RE.findall(m.group(1)))
        key = attrs.get("name") or attrs.get("property") or attrs.get("itemprop")
        val = attrs.get("content")
        if key and val and key not in out:
            out[key] = val
    return out


def _first_jsonld_newsarticle(html: str) -> dict | None:
    """Return the first JSON-LD block of @type NewsArticle, or None."""
    for m in _JSONLD_RE.finditer(html):
        try:
            data = json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError):
            continue
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("@type") == "NewsArticle":
                    return item
        elif isinstance(data, dict) and data.get("@type") == "NewsArticle":
            return data
    return None


def _stringify(v):
    """Coerce list/dict/None to a flat string for CSV cells."""
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        return "|".join(_stringify(x) for x in v)
    if isinstance(v, dict):
        return v.get("name", "") or v.get("@id", "") or ""
    return str(v)


# Scraped HTML occasionally carries "unusual" line terminators (LS/PS/NEL).
# They corrupt nothing but trip editors' "unusual line terminator" warnings.
# Map each to a space — 1:1, so field lengths (and article_body_len) are kept.
_LINE_SEP_FIX = {0x2028: " ", 0x2029: " ", 0x85: " "}


def _clean_row(row: dict) -> dict:
    return {k: (v.translate(_LINE_SEP_FIX) if isinstance(v, str) else v)
            for k, v in row.items()}


def _extract_body(html: str) -> str:
    """Return text of the marked itemprop='articleBody' container, or ''."""
    soup = BeautifulSoup(html, "html.parser")
    el = soup.find(attrs={"itemprop": "articleBody"})
    if not el:
        return ""
    return el.get_text(" ", strip=True)


def extract_fields(html: str) -> dict:
    """Parse a GNW article page into the EXTRACTED_FIELDS schema."""
    meta = _extract_meta(html)
    ld = _first_jsonld_newsarticle(html) or {}
    body = _extract_body(html)
    return {
        "ld_headline":          _stringify(ld.get("headline")),
        "ld_description":       _stringify(ld.get("description")),
        "ld_dateline":          _stringify(ld.get("dateline")),
        "ld_date_published":    _stringify(ld.get("datePublished")),
        "ld_date_modified":     _stringify(ld.get("dateModified")),
        "ld_in_language":       _stringify(ld.get("inLanguage")),
        "ld_keywords":          _stringify(ld.get("keywords")),
        "ld_article_section":   _stringify(ld.get("articleSection")),
        "ld_author_name":       _stringify(ld.get("author")),
        "ld_source_org":        _stringify(ld.get("sourceOrganization")),
        "ld_location":          _stringify(ld.get("locationCreated")),
        "meta_keywords":        meta.get("keywords", ""),
        "meta_author":          meta.get("author", ""),
        "meta_dc_date_issued":  meta.get("DC.date.issued", ""),
        "article_body":         body,
        "article_body_len":     str(len(body)),
    }


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
        status, html = await fetch_one(client, url)
        if status == 200:
            fields = extract_fields(html)
        else:
            fields = {k: "" for k in EXTRACTED_FIELDS if k != "http_status"}
        fields["http_status"] = str(status)
        out_row = _clean_row({**row, **fields})
        async with lock:
            writer.writerow(out_row)
            state["written"] += 1
            if state["written"] % 5 == 0:
                state["fh"].flush()
                print(f"  written={state['written']}/{state['total']} last_status={status} url=...{url[-60:]}", flush=True)
        await asyncio.sleep(random.uniform(args.delay_min, args.delay_max))
        queue.task_done()


async def main_async(args):
    if not os.path.exists(INPUT_CSV):
        print(f"missing input: {INPUT_CSV}")
        sys.exit(1)

    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        in_fields = reader.fieldnames
        rows = list(reader)
    print(f"input rows: {len(rows)}")

    done_urls = set()
    if os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                done_urls.add(row["url"])
        print(f"resuming — already have {len(done_urls)} URLs in {OUTPUT_CSV}")

    todo = [r for r in rows if r["url"] not in done_urls]
    if args.limit:
        todo = todo[: args.limit]
    print(f"to fetch: {len(todo)}")
    if not todo:
        return

    fieldnames = list(in_fields) + EXTRACTED_FIELDS
    write_header = not os.path.exists(OUTPUT_CSV) or os.path.getsize(OUTPUT_CSV) == 0
    fh = open(OUTPUT_CSV, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(fh, fieldnames=fieldnames)
    if write_header:
        writer.writeheader()

    state = {"written": 0, "total": len(todo), "fh": fh}
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
    t0 = time.time()
    async with httpx.AsyncClient(headers=headers, timeout=20.0) as client:
        workers = [
            asyncio.create_task(worker(queue, client, writer, lock, args, state))
            for _ in range(args.workers)
        ]
        await asyncio.gather(*workers)

    fh.close()
    elapsed = time.time() - t0
    print(f"\ndone. wrote {state['written']} new rows in {elapsed:.1f}s "
          f"({state['written']/max(elapsed,1):.2f} rows/s)")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--delay-min", type=float, default=0.3)
    p.add_argument("--delay-max", type=float, default=1.0)
    p.add_argument("--limit", type=int, default=None,
                   help="only fetch first N URLs (for testing)")
    args = p.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
