"""
classify_exhibits.py — Classify EX-99 exhibits as press releases using heuristics only.

Reads data/8k_ex99.csv, fetches each EX-99, classifies using heuristics,
extracts title, and applies regex catalyst tagging.
Saves results to data/ex_99_classified.csv with is_pr and catalyst columns.
Skips earnings-only filings (item 2.02 with no real signal items — 7.01 Reg FD not counted).

Rate: BATCH_SIZE=10 per BATCH_INTERVAL=1.0s → exactly 10 req/s.
Append-safe: skips ex99_urls already present in the output CSV.
"""
import asyncio
import os
import re
import time

import httpx
import pandas as pd

from sec.edgar import fetch_html
from sec.pr_detect import analyze_heuristics, classify_heuristic, extract_title, is_earnings
from regex.catalysts import classify_catalyst

BATCH_SIZE = 10
BATCH_INTERVAL = 1.0
INPUT_CSV = "data/8k_ex99.csv"
OUTPUT_CSV = "data/ex_99_classified.csv"


async def _fetch_and_classify(client, row):
    url = row["ex99_url"]
    html = await fetch_html(client, url)
    if html is None:
        return {**row.to_dict(), "H1": None, "H2": None, "H3": None,
                "H4": None, "H5": None, "H6": None,
                "heuristic": None, "is_pr": False, "title": None, "catalyst": str(["other"])}

    signals = analyze_heuristics(html)
    heuristic = classify_heuristic(signals)

    if heuristic in {None, "H6", "combined"} and is_earnings(html):
        heuristic = "earnings"

    is_pr = heuristic is not None and heuristic != "earnings"
    title = None
    if is_pr or heuristic == "earnings":
        title = extract_title(html) or None

    catalyst = classify_catalyst(title) if title else ["other"]
    label = f"PR [{heuristic}]" if is_pr else "not PR    "
    print(f"  {label} | {title} | {', '.join(catalyst)}", flush=True)

    return {**row.to_dict(), **signals, "heuristic": heuristic, "is_pr": is_pr,
            "title": title, "catalyst": str(catalyst)}


async def _run(df, fetched_urls):
    write_header = not os.path.exists(OUTPUT_CSV)

    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            pending = [row for _, row in batch.iterrows() if row["ex99_url"] not in fetched_urls]

            if not pending:
                continue

            print(f"\n=== BATCH {batch_num} ({len(pending)} exhibits) ===", flush=True)
            t_start = time.monotonic()

            results = await asyncio.gather(*[_fetch_and_classify(client, row) for row in pending])

            pd.DataFrame(results).to_csv(OUTPUT_CSV, mode="a", header=write_header, index=False)
            write_header = False
            fetched_urls.update(r["ex99_url"] for r in results)

            elapsed = time.monotonic() - t_start
            remaining = BATCH_INTERVAL - elapsed
            if remaining > 0 and i + BATCH_SIZE < len(df):
                await asyncio.sleep(remaining)


def main():
    df = pd.read_csv(INPUT_CSV)
    df = df[df["ex99_url"].notna() & (df["ex99_url"] != "")].reset_index(drop=True)
    before = len(df)
    _has_202        = df["items"].fillna("").str.contains(r"\b2\.02\b", regex=True)
    _has_real_signal = df["items"].fillna("").str.contains(r"\b(?:8\.01|1\.01|2\.01|3\.02|5\.01)\b", regex=True)
    df = df[~(_has_202 & ~_has_real_signal)].reset_index(drop=True)
    print(f"Loaded {len(df)} EX-99 exhibits ({before - len(df)} earnings-only filings excluded)", flush=True)

    if os.path.exists(OUTPUT_CSV):
        existing = pd.read_csv(OUTPUT_CSV, usecols=["ex99_url"])
        fetched_urls = set(existing["ex99_url"])
        print(f"  {len(fetched_urls)} exhibits already classified — skipping", flush=True)
    else:
        fetched_urls = set()

    asyncio.run(_run(df, fetched_urls))
    print(f"\nDone. Exhibits saved to {OUTPUT_CSV}", flush=True)


if __name__ == "__main__":
    main()
