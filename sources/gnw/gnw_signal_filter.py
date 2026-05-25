"""
gnw_signal_filter.py — select the ML-eligible GNW subset from gnw_classified.csv.

Keeps rows where: catalyst includes an author-SIGNAL tag, ticker present,
datetime >= cutoff (Polygon price-data window).

Input:  data/gnw_classified.csv  (datetime, source, url, title, ticker, exchange, catalyst)
Output: data/gnw_signal_filtered.csv  (same cols + signal_tags)

Usage:
  python scripts/gnw_signal_filter.py
  python scripts/gnw_signal_filter.py --cutoff 2021-05-19
"""
import argparse
import ast
import csv
from collections import Counter

INPUT_CSV  = "data/gnw_classified.csv"
OUTPUT_CSV = "data/gnw_signal_filtered.csv"

# Author-defined SIGNAL catalyst tags (pr_detection.py).
SIGNAL = {"biotech", "private_placement", "collaboration", "m&a",
          "new_product", "contract", "crypto_treasury"}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--cutoff", default="2021-05-19", help="min datetime (YYYY-MM-DD)")
    args = p.parse_args()

    counts = Counter()
    total = kept = 0
    out_rows = []
    with open(INPUT_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        in_fields = reader.fieldnames
        for row in reader:
            total += 1
            if not (row.get("ticker") or "").strip():
                continue
            if (row.get("datetime") or "")[:10] < args.cutoff:
                continue
            try:
                cats = set(ast.literal_eval(row["catalyst"]))
            except (ValueError, SyntaxError):
                continue
            hits = cats & SIGNAL
            if not hits:
                continue
            kept += 1
            for h in hits:
                counts[h] += 1
            out_rows.append({**row, "signal_tags": ",".join(sorted(hits))})

    fieldnames = list(in_fields) + ["signal_tags"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out_rows)

    print(f"input rows:  {total:,}")
    print(f"kept:        {kept:,}  (signal + ticker + datetime>={args.cutoff})")
    print(f"-> {OUTPUT_CSV}")
    print("per signal tag:")
    for tag, n in counts.most_common():
        print(f"  {n:>6,}  {tag}")


if __name__ == "__main__":
    main()
