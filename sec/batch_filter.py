"""
batch_filter.py — Check which 8-K filings have EX-99 exhibits.
Reads data/8k.csv, fetches filing index pages, saves rows with EX-99 URLs,
acceptance_dt, and 8-K item numbers to data/8k_ex99.csv.

Rate: BATCH_SIZE=10 per BATCH_INTERVAL=1.0s → exactly 10 req/s.
Append-safe: skips index URLs already present in the output CSV.
"""
import asyncio
import os
import time
import httpx
import pandas as pd
from sec.edgar import fetch_index, SEC_ARCHIVES

BATCH_SIZE = 10
BATCH_INTERVAL = 1.0
INPUT_CSV = "data/8k.csv"
OUTPUT_CSV = "data/8k_ex99.csv"


async def _process_filing(client, row):
    company = row["Company Name"]
    cik = row["CIK"]
    date_filed = row["Date Filed"]
    index_url = SEC_ARCHIVES + row["File Name"].replace(".txt", "-index.html")

    data = await fetch_index(client, index_url)
    ex99_urls = data["ex99_urls"]

    if not ex99_urls:
        print(f"  no ex-99  | {company}", flush=True)
        return [{
            "cik": cik,
            "company": company,
            "date_filed": date_filed,
            "index_url": index_url,
            "ex99_url": "",
            "acceptance_dt": data["acceptance_dt"],
            "items": ",".join(data["items"]),
        }]

    print(f"  {len(ex99_urls)} ex-99   | {company}", flush=True)
    return [
        {
            "cik": cik,
            "company": company,
            "date_filed": date_filed,
            "index_url": index_url,
            "ex99_url": url,
            "acceptance_dt": data["acceptance_dt"],
            "items": ",".join(data["items"]),
        }
        for url in ex99_urls
    ]


async def _run(df, fetched_index_urls):
    write_header = not os.path.exists(OUTPUT_CSV)

    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            pending = [
                row for _, row in batch.iterrows()
                if (SEC_ARCHIVES + row["File Name"].replace(".txt", "-index.html"))
                not in fetched_index_urls
            ]

            if not pending:
                continue

            print(f"\n=== BATCH {batch_num} ({len(pending)} filings) ===", flush=True)
            t_start = time.monotonic()
            results = await asyncio.gather(*[_process_filing(client, row) for row in pending])

            rows_out = [item for sublist in results for item in sublist]
            if rows_out:
                pd.DataFrame(rows_out).to_csv(
                    OUTPUT_CSV, mode="a", header=write_header, index=False
                )
                write_header = False

            elapsed = time.monotonic() - t_start
            remaining = BATCH_INTERVAL - elapsed
            if remaining > 0 and i + BATCH_SIZE < len(df):
                await asyncio.sleep(remaining)


def main():
    df = pd.read_csv(INPUT_CSV)
    print(f"Loaded {len(df)} 8-K filings", flush=True)

    if os.path.exists(OUTPUT_CSV):
        existing = pd.read_csv(OUTPUT_CSV, usecols=["index_url"])
        fetched_index_urls = set(existing["index_url"])
        print(f"  {len(fetched_index_urls)} index URLs already processed — skipping", flush=True)
    else:
        fetched_index_urls = set()

    asyncio.run(_run(df, fetched_index_urls))
    print(f"\nDone. Output in {OUTPUT_CSV}", flush=True)


if __name__ == "__main__":
    main()
