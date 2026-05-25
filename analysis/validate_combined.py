"""
validate_combined.py — Validate "combined" heuristic PRs against LLM classifier.
Fetches EX-99 HTML for rows where heuristic == "combined", runs LLM on each,
and reports agreement stats.
"""
import sys
import os

import argparse
import asyncio
import time
import httpx
import pandas as pd
from sec.edgar import fetch_html
from sec.pr_detect import classify_llm

INPUT_CSV = "data/ex_99_classified.csv"
BATCH_SIZE = 10
BATCH_INTERVAL = 1.0   # seconds between SEC batches  → 10 req/s
LLM_INTERVAL = 1.2     # seconds between LLM calls    → 50 RPM


async def _run(df):
    results = []

    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]
            print(f"\n[{i + 1}-{min(i + BATCH_SIZE, len(df))}/{len(df)}]", flush=True)

            # Fetch all HTML concurrently (SEC rate: 10/s)
            t_start = time.monotonic()
            htmls = await asyncio.gather(*[
                fetch_html(client, row["ex99_url"]) for _, row in batch.iterrows()
            ])
            elapsed = time.monotonic() - t_start
            remaining = BATCH_INTERVAL - elapsed
            if remaining > 0 and i + BATCH_SIZE < len(df):
                await asyncio.sleep(remaining)

            # Call LLM sequentially (Anthropic rate: 50 RPM)
            for (_, row), html in zip(batch.iterrows(), htmls):
                if html is None:
                    continue
                llm_result = await classify_llm(html)
                llm_is_pr = llm_result is not None
                print(
                    f"  {'AGREE  ' if llm_is_pr else 'DISAGREE'} | {row['company'][:50]}",
                    flush=True,
                )
                results.append({
                    "company": row["company"],
                    "ex99_url": row["ex99_url"],
                    "heuristic": row["heuristic"],
                    "llm_is_pr": llm_is_pr,
                })
                await asyncio.sleep(LLM_INTERVAL)

    return results


def main(input_csv=INPUT_CSV):
    df = pd.read_csv(input_csv)
    combined = df[df["heuristic"] == "combined"].reset_index(drop=True)
    print(f"Found {len(combined)} 'combined' PRs — sending to LLM...")

    results = asyncio.run(_run(combined))

    if not results:
        print("No results.")
        return

    out = pd.DataFrame(results)
    agreed = out["llm_is_pr"].sum()
    disagreed = (~out["llm_is_pr"]).sum()
    total = len(out)

    print(f"\n{'─' * 50}")
    print(f"  Total validated:  {total}")
    print(f"  LLM agrees (PR):  {agreed}  ({agreed/total*100:.1f}%)")
    print(f"  LLM disagrees:    {disagreed}  ({disagreed/total*100:.1f}%)")
    disagreements = out[~out["llm_is_pr"]]
    if not disagreements.empty:
        out_path = "data/combined_disagreements.csv"
        disagreements.to_csv(out_path, index=False)
        print(f"\n  Disagreements saved to {out_path}")
        for _, row in disagreements.iterrows():
            print(f"    {row['company'][:60]}")
            print(f"    {row['ex99_url']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=INPUT_CSV, help="Input CSV path")
    args = parser.parse_args()
    main(args.input)
