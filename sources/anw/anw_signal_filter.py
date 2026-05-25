"""
anw_signal_filter.py — build data/anw_signal_filtered.csv from the monthly ANW
sitemap CSVs in data/anw/.

ANW carries no scraped title or ticker — only (date, language, industry, url).
The headline lives in the URL slug (lowercased, truncated to ~60 chars), so we
recover a best-effort title from the slug and filter on it, dropping a row
unless BOTH:

  1. its slug-title is NOT a law-firm litigation solicitation
     (deadline alert / class action / "<firm> reminds ..."), AND
  2. its slug-title classifies as a price-signal catalyst
     (see pr_detection.SIGNAL_CATALYSTS).

The spam test reuses bw_signal_filter._SPAM_RE — the canonical cross-source
litigation filter — so the two sources stay in lockstep.

Two differences from the bw filter, both inherent to ANW:
  * No ticker column exists yet, so bw's ticker-universe filter cannot run here.
    Tickers are parsed downstream from the article body by anw_extract_fields.py;
    the universe filter belongs after that, not here.
  * Slugs are truncated, so a cut-off catalyst keyword falls through to "other",
    which is a SIGNAL tag (kept). Net effect on ANW: spam removal does the heavy
    lifting at this stage and the catalyst filter is a light touch; the real
    catalyst pass runs post-extraction on the untruncated og:title. The
    recall-gate bias (keep when unsure) is deliberate.

Output is one consolidated CSV — the --input default of anw_extract_fields.py —
with the original columns passed through verbatim plus two appended for
inspection: slug_title and catalyst. Rebuilt from scratch each run (like the bw
filter); no append/resume because it is pure CPU, no network.

Usage:
    python scripts/anw_signal_filter.py                  # all months in data/anw/
    python scripts/anw_signal_filter.py --from 2024-01   # 2024-01 onward
    python scripts/anw_signal_filter.py --from 2023-01 --to 2023-12
"""
import argparse
import csv
import glob
import os
import re
import urllib.parse

from sources.bw.bw_signal_filter import is_spam
from regex.catalysts import classify_catalyst, is_signal

csv.field_size_limit(10**7)

INPUT_DIR = "data/anw"
OUT       = "data/anw_signal_filtered.csv"

_MONTH_RE = re.compile(r"anw_(\d{4}-\d{2})\.csv$")
# Trailing "-<article id>" appended to every ANW slug, e.g. "...-and-gros-799157".
_SLUG_ID  = re.compile(r"-\d+$")


def title_from_url(url: str) -> str | None:
    """Recover a best-effort headline from an ANW URL slug. None if unparseable.

    The last path segment is "<hyphenated-headline>-<numeric id>"; drop the id
    and turn hyphens into spaces. Works for both layouts — 2007 (.../en/<slug>)
    and modern (.../en/<industry>/<slug>) — since we only take the last segment.
    Slugs are lowercased and truncated upstream, so the result is partial — fine
    for keyword spam/catalyst matching.
    """
    path = urllib.parse.urlparse(url).path.rstrip("/")
    slug = _SLUG_ID.sub("", path.rsplit("/", 1)[-1])
    slug = urllib.parse.unquote(slug)      # decode %xx (e.g. %22 -> ", accents)
    if not re.search(r"[A-Za-z]", slug):   # pure-id leftover, no headline words
        return None
    return slug.replace("-", " ")


def _iter_inputs(folder, from_month, to_month):
    for path in sorted(glob.glob(os.path.join(folder, "anw_*.csv"))):
        m = _MONTH_RE.search(os.path.basename(path))
        if not m:
            continue
        month = m.group(1)
        if from_month and month < from_month:
            continue
        if to_month and month > to_month:
            continue
        yield path


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--folder", default=INPUT_DIR,
                   help="folder of monthly anw_*.csv (default data/anw)")
    p.add_argument("--from", dest="from_month", help="start month YYYY-MM (inclusive)")
    p.add_argument("--to",   dest="to_month",   help="end month YYYY-MM (inclusive)")
    p.add_argument("--output", default=OUT, help=f"output CSV (default {OUT})")
    args = p.parse_args()

    inputs = list(_iter_inputs(args.folder, args.from_month, args.to_month))
    if not inputs:
        print(f"no monthly CSVs in {args.folder} for range "
              f"[{args.from_month or '...'} .. {args.to_month or '...'}]")
        raise SystemExit(1)

    out_cols = ["date", "language", "industry", "url", "slug_title", "catalyst"]
    total = no_title = spam = no_signal = kept = 0
    spam_samples, kept_samples = [], []

    with open(args.output, "w", encoding="utf-8", newline="") as f_out:
        w = csv.DictWriter(f_out, fieldnames=out_cols, extrasaction="ignore")
        w.writeheader()

        for path in inputs:
            with open(path, encoding="utf-8", newline="") as f_in:
                rows = list(csv.DictReader(f_in))
            file_kept = 0
            for r in rows:
                total += 1
                title = title_from_url((r.get("url") or "").strip())
                if not title:
                    no_title += 1
                    continue
                if is_spam(title):
                    spam += 1
                    if len(spam_samples) < 20:
                        spam_samples.append(title)
                    continue
                tags = classify_catalyst(title)
                if not is_signal(tags):
                    no_signal += 1
                    continue
                w.writerow({**r, "slug_title": title, "catalyst": str(tags)})
                kept += 1
                file_kept += 1
                if len(kept_samples) < 20:
                    kept_samples.append(title)
            print(f"  {os.path.basename(path)}: {len(rows):>6,} rows -> {file_kept:>5,} kept")

    print(f"\ntotal anw rows        : {total:,}")
    print(f"  dropped unparseable slug : {no_title:,}")
    print(f"  dropped law-firm spam    : {spam:,}")
    print(f"  dropped no-signal        : {no_signal:,}")
    print(f"  KEPT (-> {args.output}) : {kept:,}")
    print(f"\nspam as % of (spam+kept): {spam/max(spam+kept,1):.1%}")
    print("\n--- up to 20 sample EXCLUDED (spam) titles ---")
    for t in spam_samples:
        print(f"  {t[:90]}")
    print("\n--- up to 20 sample KEPT titles ---")
    for t in kept_samples:
        print(f"  {t[:90]}")


if __name__ == "__main__":
    main()
