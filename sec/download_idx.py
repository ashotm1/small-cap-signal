"""
download_idx.py — Download SEC EDGAR daily index files.
Fetches form.YYYYMMDD.idx files from the EDGAR daily-index archive.
Skips weekends and holidays (SEC returns 403/404 for those dates).
Append-safe: skips files already present in the idx/ directory.
"""
import argparse
import asyncio
import os
import time
import httpx
from datetime import date, timedelta

from sec.edgar import HEADERS
IDX_DIR = "idx"
BATCH_SIZE = 10
BATCH_INTERVAL = 1.0  # seconds between batches → 10 req/s


def _quarter(d: date) -> str:
    return f"QTR{(d.month - 1) // 3 + 1}"


def _url(d: date) -> str:
    return (
        f"https://www.sec.gov/Archives/edgar/daily-index/"
        f"{d.year}/{_quarter(d)}/form.{d.strftime('%Y%m%d')}.idx"
    )


async def _download_one(client: httpx.AsyncClient, d: date) -> None:
    date_str = d.strftime("%Y%m%d")
    local_path = os.path.join(IDX_DIR, f"form.{date_str}.idx")

    if os.path.exists(local_path):
        print(f"[EXISTS ] {local_path}", flush=True)
        return

    r = await client.get(_url(d), headers=HEADERS)

    if r.status_code in (403, 404):
        print(f"[SKIPPED] {date_str} (weekend/holiday)", flush=True)
        return

    if r.status_code != 200:
        print(f"[ ERROR ] {date_str} status={r.status_code}", flush=True)
        return

    with open(local_path, "w", encoding="utf-8") as f:
        f.write(r.text)

    print(f"[  OK   ] {local_path}", flush=True)


async def _download_all(dates: list) -> None:
    os.makedirs(IDX_DIR, exist_ok=True)
    async with httpx.AsyncClient() as client:
        for i in range(0, len(dates), BATCH_SIZE):
            batch = dates[i:i + BATCH_SIZE]
            t_start = time.monotonic()
            await asyncio.gather(*[_download_one(client, d) for d in batch])
            elapsed = time.monotonic() - t_start
            remaining = BATCH_INTERVAL - elapsed
            if remaining > 0 and i + BATCH_SIZE < len(dates):
                await asyncio.sleep(remaining)


def _parse_args():
    parser = argparse.ArgumentParser(description="Download SEC EDGAR daily index files")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", help="Single date: YYYY-MM-DD")
    group.add_argument("--from", dest="date_from", help="Start of date range: YYYY-MM-DD")
    group.add_argument("--days", type=int, help="Last N calendar days")
    parser.add_argument("--to", dest="date_to", help="End of range for --from (default: today)")
    return parser.parse_args()


def main():
    args = _parse_args()
    today = date.today()

    if args.date:
        dates = [date.fromisoformat(args.date)]
    elif args.days:
        dates = [today - timedelta(days=i) for i in range(args.days - 1, -1, -1)]
    else:
        start = date.fromisoformat(args.date_from)
        end = date.fromisoformat(args.date_to) if args.date_to else today
        dates = []
        d = start
        while d <= end:
            dates.append(d)
            d += timedelta(days=1)

    asyncio.run(_download_all(dates))


if __name__ == "__main__":
    main()
