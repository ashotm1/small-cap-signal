"""
prn_classifier.py — Classify PRN CSV rows: title, company, ticker, exchange, catalyst tags.

Pipeline (cheapest first):
  url    → title (strip slug)
  title  → company name guess (text before first PR action verb)
  name   → (ticker, exchange) via ticker_universe.csv lookup
  ticker → catalyst tags (only if listed on NYSE/NASDAQ; else 'unlisted')

Usage:
  python scripts/prn_classifier.py
  python scripts/prn_classifier.py --input-dir data/prn_data --output data/prn_classified.csv
"""
import argparse
import bisect
import csv
import glob
import os
import re
import urllib.parse

from pr_detection import classify_catalyst

_URL_TAIL = re.compile(r"-(\d+)\.html?$", re.IGNORECASE)

# Verbs that follow the company name in PR headlines.
_TITLE_VERB = re.compile(
    r"\b(announces?|reports?|launches?|introduces?|completes?|acquires?|appoints?|"
    r"names?|elects?|provides?|releases?|enters?|expands?|files?|receives?|wins?|"
    r"awards?|signs?|secures?|achieves?|unveils?|delivers?|posts?|to\s+acquire|"
    r"to\s+merge|to\s+present|to\s+report|sets?|raises?|priced?|closes?)\b",
    re.IGNORECASE,
)

# Legal-entity suffixes only — do not strip industry words like "Therapeutics".
_LEGAL_SUFFIX = re.compile(
    r"\b(?:common\s+stock|class\s+[a-z]|"
    r"inc|corp|corporation|company|co|ltd|limited|llc|plc|sa|nv|ag|holdings?)\b\.?",
    re.IGNORECASE,
)
_PUNCT = re.compile(r"[^\w\s]")


def title_from_url(url: str) -> str | None:
    """Convert PRN URL slug to a headline-like string. None if unparseable."""
    path = urllib.parse.urlparse(url).path
    slug = path.rstrip("/").rsplit("/", 1)[-1]
    slug = _URL_TAIL.sub("", slug)
    if not slug or slug == path.rstrip("/"):
        return None
    return slug.replace("-", " ")


def company_from_title(title: str) -> str | None:
    """Return text before the first PR action verb, or None."""
    if not title:
        return None
    m = _TITLE_VERB.search(title)
    if not m:
        return None
    name = title[: m.start()].strip().rstrip(",")
    return name or None


def _normalize(name: str) -> str:
    """Lowercase, strip punctuation and legal-entity suffixes for matching."""
    name = name.lower()
    name = _LEGAL_SUFFIX.sub(" ", name)
    name = _PUNCT.sub(" ", name)
    return " ".join(name.split())


def build_ticker_index(ticker_universe_path: str) -> tuple[dict[str, tuple[str, str]], list[str]]:
    """Read ticker_universe.csv → (index, sorted_keys).

    index       — {normalized_name: (ticker, exchange)}
    sorted_keys — sorted list of index keys for O(log N) prefix lookup

    On duplicate names the first row wins (CSV is sorted by ticker, so this
    is deterministic; fine-tune later if collisions matter).
    """
    index: dict[str, tuple[str, str]] = {}
    with open(ticker_universe_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = _normalize(row.get("name", ""))
            if key and key not in index:
                index[key] = (row["ticker"], row.get("primary_exchange", ""))
    return index, sorted(index)


def lookup_ticker(
    name: str,
    index: dict[str, tuple[str, str]],
    sorted_keys: list[str],
) -> tuple[str, str] | None:
    """Lookup name → (ticker, exchange). Tries exact then O(log N) prefix match."""
    if not name:
        return None
    key = _normalize(name)
    if not key:
        return None
    if key in index:
        return index[key]
    prefix = key + " "
    pos = bisect.bisect_left(sorted_keys, prefix)
    if pos < len(sorted_keys) and sorted_keys[pos].startswith(prefix):
        return index[sorted_keys[pos]]
    return None


_LISTED_EXCHANGES = {"XNAS", "XNYS", "XASE"}  # Polygon MIC codes for NASDAQ, NYSE, NYSE American


def classify_row(
    url: str,
    index: dict[str, tuple[str, str]],
    sorted_keys: list[str],
) -> dict:
    """Full classify pipeline for one PRN URL."""
    title = title_from_url(url)
    name = company_from_title(title) if title else None
    hit = lookup_ticker(name, index, sorted_keys) if name else None
    ticker = hit[0] if hit else None
    exchange = hit[1] if hit else None
    is_listed = exchange in _LISTED_EXCHANGES if exchange else False
    catalyst = str(classify_catalyst(title)) if (is_listed and title) else str(["unlisted"])
    return {
        "title": title,
        "company": name,
        "ticker": ticker,
        "exchange": exchange,
        "catalyst": catalyst,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="data/prn_data")
    parser.add_argument("--output", default="data/prn_classified.csv")
    parser.add_argument("--ticker-universe", default="data/ticker_universe.csv")
    args = parser.parse_args()

    input_files = sorted(glob.glob(os.path.join(args.input_dir, "prn_*.csv")))
    if not input_files:
        print(f"No prn_*.csv files found in {args.input_dir}")
        raise SystemExit(1)

    # Append-safe: skip URLs already classified
    done_urls: set[str] = set()
    if os.path.exists(args.output):
        with open(args.output, encoding="utf-8") as f:
            done_urls = {row["url"] for row in csv.DictReader(f)}
        print(f"Resuming — {len(done_urls)} URLs already classified")

    print(f"Building ticker index from {args.ticker_universe}...")
    index, sorted_keys = build_ticker_index(args.ticker_universe)
    print(f"  {len(index)} tickers loaded")

    write_header = not os.path.exists(args.output)
    total_written = 0

    with open(args.output, "a", newline="", encoding="utf-8") as f_out:
        writer = None

        for path in input_files:
            with open(path, encoding="utf-8") as f_in:
                reader = csv.DictReader(f_in)
                batch_written = 0
                for row in reader:
                    url = row.get("url", "")
                    if not url or url in done_urls:
                        continue
                    result = classify_row(url, index, sorted_keys)
                    out_row = {
                        "datetime": row.get("datetime", ""),
                        "issuer": row.get("issuer", ""),
                        "url": url,
                        "company": result["company"],
                        "ticker": result["ticker"],
                        "catalyst": result["catalyst"],
                    }
                    if writer is None:
                        writer = csv.DictWriter(f_out, fieldnames=list(out_row.keys()))
                    if write_header:
                        writer.writeheader()
                        write_header = False
                    writer.writerow(out_row)
                    done_urls.add(url)
                    batch_written += 1
                total_written += batch_written
            print(f"  {os.path.basename(path)}: {batch_written} rows")

    print(f"\nDone. {total_written} rows written to {args.output}")
