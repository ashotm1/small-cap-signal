"""
fix_announces_titles.py — Re-extract titles for rows where title ends with 'Announces'.

Finds rows in ex_99_classified.csv where title ends with 'Announces[s]',
re-fetches the EX-99 HTML, runs extract_title, and updates the CSV in-place.

Usage:
  python utils/fix_announces_titles.py
  python utils/fix_announces_titles.py --dry-run   # print fixes without saving
"""
import argparse
import asyncio
import time

import sys
import os

import httpx
import pandas as pd

from sec.pr_detect import extract_title
from sec.edgar import fetch_html

INPUT_CSV     = "data/ex_99_classified.csv"
BATCH_SIZE    = 10
BATCH_INTERVAL = 1.0  # seconds between batches (SEC rate limit)


async def run(dry_run: bool):
    df = pd.read_csv(INPUT_CSV)

    mask = df["title"].notna() & df["title"].str.contains(r"\bAnnounces?\s*$", case=False, regex=True)
    targets = df[mask].copy()
    print(f"Found {len(targets)} rows with truncated 'Announces' titles")

    updates = {}  # ex99_url → new_title

    async with httpx.AsyncClient(timeout=30, headers={"User-Agent": "research fix@fix.com"}) as client:
        urls = targets["ex99_url"].tolist()
        for i in range(0, len(urls), BATCH_SIZE):
            batch_urls = urls[i:i + BATCH_SIZE]
            t_start = time.monotonic()

            htmls = await asyncio.gather(*[fetch_html(client, url) for url in batch_urls])

            for url, html in zip(batch_urls, htmls):
                old_title = df.loc[df["ex99_url"] == url, "title"].iloc[0]
                if html is None:
                    print(f"  fetch failed  | {url[-50:]}")
                    continue
                new_title = extract_title(html)
                if new_title and new_title != old_title:
                    updates[url] = new_title
                    print(f"  FIXED | {old_title!r}")
                    print(f"      → {new_title!r}")
                else:
                    print(f"  no change     | {old_title!r}")

            elapsed = time.monotonic() - t_start
            remaining = BATCH_INTERVAL - elapsed
            if remaining > 0 and i + BATCH_SIZE < len(urls):
                await asyncio.sleep(remaining)

    print(f"\n{len(updates)} titles updated")

    if dry_run:
        print("Dry run — no changes saved.")
        return

    for url, new_title in updates.items():
        df.loc[df["ex99_url"] == url, "title"] = new_title

    df.to_csv(INPUT_CSV, index=False)
    print(f"Saved to {INPUT_CSV}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print fixes without saving")
    args = parser.parse_args()
    asyncio.run(run(args.dry_run))


if __name__ == "__main__":
    main()
