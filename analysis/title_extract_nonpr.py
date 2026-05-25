"""
title_extract_nonpr.py — Batch title extraction on borderline non-PR rows.

Takes 321 two-signal + 679 one-signal non-PR rows, fetches HTML, submits to
Haiku batch for title extraction. Used to audit is_pr=False misclassifications.

Results saved to data/title_extract_nonpr.csv.

Usage:
  python utils/title_extract_nonpr.py --submit
  python utils/title_extract_nonpr.py --status
  python utils/title_extract_nonpr.py --collect
"""
import argparse
import asyncio
import json
import os
import re
import sys
import time

import httpx
import pandas as pd
from anthropic import Anthropic

from sec.edgar import fetch_html

# ── Config ─────────────────────────────────────────────────────────────────────
INPUT_CSV   = "data/ex_99_classified.csv"
OUTPUT_CSV  = "data/title_extract_nonpr.csv"
STATE_FILE  = "data/title_extract_nonpr_state.json"
MODEL       = "claude-haiku-4-5-20251001"
MAX_TOKENS  = 50
BATCH_SIZE  = 10
BATCH_INTERVAL = 1.0
SAMPLE_SIZE = 1000

_SYSTEM = ("Extract the press release title from this excerpt. "
           "Return only the title text, nothing else. "
           "If no clear title is present, return 'unknown'.")

_SKIP_TOKENS = re.compile(
    r"^(EX-\d+\.\d+|Exhibit|\S+\.html?|\d+\.\d+|\d+)$", re.IGNORECASE
)

_client = Anthropic()


# ── Helpers ────────────────────────────────────────────────────────────────────

def _build_sample() -> pd.DataFrame:
    df = pd.read_csv(INPUT_CSV)
    non_pr = df[df["is_pr"] == False].copy()
    signal_cols = ["H1", "H2", "H3", "H4", "H5", "H6"]
    non_pr["signals_fired"] = non_pr[signal_cols].sum(axis=1)

    two_sig = non_pr[non_pr["signals_fired"] == 2]
    one_sig = non_pr[non_pr["signals_fired"] == 1]

    n_two = min(len(two_sig), 321)
    n_one = SAMPLE_SIZE - n_two

    sample = pd.concat([
        two_sig.sample(n_two, random_state=42),
        one_sig.sample(min(len(one_sig), n_one), random_state=42),
    ]).reset_index(drop=True)

    print(f"Sample: {n_two} two-signal + {len(sample) - n_two} one-signal = {len(sample)} rows")
    return sample


def _extract_excerpt(html: str) -> str | None:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    raw = " ".join(soup.stripped_strings)
    raw = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", raw)
    words = [w for w in raw.split() if w]
    while words and _SKIP_TOKENS.match(words[0]):
        words.pop(0)
    if not words:
        return None
    return " ".join(words[:100])


# ── Submit ─────────────────────────────────────────────────────────────────────

async def _fetch_excerpts(sample: pd.DataFrame) -> dict:
    excerpts = {}
    async with httpx.AsyncClient(timeout=30) as client:
        urls = sample["ex99_url"].tolist()
        for i in range(0, len(urls), BATCH_SIZE):
            batch = urls[i:i + BATCH_SIZE]
            t_start = time.monotonic()
            htmls = await asyncio.gather(*[fetch_html(client, url) for url in batch])
            for url, html in zip(batch, htmls):
                if html is None:
                    excerpts[url] = None
                    print(f"  fetch failed  | {url[-50:]}", flush=True)
                else:
                    excerpt = _extract_excerpt(html)
                    excerpts[url] = excerpt
                    print(f"  fetched       | {url[-50:]}", flush=True)
            elapsed = time.monotonic() - t_start
            remaining = BATCH_INTERVAL - elapsed
            if remaining > 0 and i + BATCH_SIZE < len(urls):
                await asyncio.sleep(remaining)
    return excerpts


async def run_submit():
    if os.path.exists(STATE_FILE):
        print(f"State file exists: {STATE_FILE}. Run --collect or delete to resubmit.")
        return

    sample = _build_sample()
    print(f"\nFetching HTML excerpts ({len(sample)} rows)...")
    excerpts = await _fetch_excerpts(sample)

    requests = []
    id_to_url = {}
    for i, (url, excerpt) in enumerate(excerpts.items()):
        if not excerpt:
            continue
        custom_id = str(i)
        id_to_url[custom_id] = url
        requests.append({
            "custom_id": custom_id,
            "params": {
                "model": MODEL,
                "max_tokens": MAX_TOKENS,
                "temperature": 0,
                "system": _SYSTEM,
                "messages": [{"role": "user", "content": excerpt}],
            },
        })

    print(f"\nSubmitting {len(requests)} requests to Anthropic batch API...")
    batch = _client.messages.batches.create(requests=requests)
    print(f"  Batch ID: {batch.id}  Status: {batch.processing_status}")

    # Save sample metadata alongside state
    sample_meta = sample[["ex99_url", "H1", "H2", "H3", "H4", "H5", "H6",
                           "signals_fired", "heuristic", "company", "items"]].copy()
    sample_meta.to_csv(OUTPUT_CSV, index=False)
    print(f"Sample metadata saved → {OUTPUT_CSV}")

    state = {"batch_id": batch.id, "id_to_url": id_to_url}
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)
    print(f"State saved → {STATE_FILE}")


# ── Status ─────────────────────────────────────────────────────────────────────

def run_status():
    if not os.path.exists(STATE_FILE):
        print(f"No state file at {STATE_FILE}.")
        return
    with open(STATE_FILE) as f:
        state = json.load(f)
    batch = _client.messages.batches.retrieve(state["batch_id"])
    c = batch.request_counts
    total = c.processing + c.succeeded + c.errored + c.canceled + c.expired
    done = total - c.processing
    print(f"Batch {batch.id}  {batch.processing_status}")
    print(f"  {done}/{total} done  ({c.succeeded} ok, {c.errored} err, {c.processing} processing)")


# ── Collect ────────────────────────────────────────────────────────────────────

def run_collect():
    if not os.path.exists(STATE_FILE):
        print(f"No state file at {STATE_FILE}.")
        return
    with open(STATE_FILE) as f:
        state = json.load(f)

    batch = _client.messages.batches.retrieve(state["batch_id"])
    if batch.processing_status != "ended":
        print(f"Not ready yet: {batch.processing_status}")
        return

    id_to_url = state["id_to_url"]
    results_df = pd.read_csv(OUTPUT_CSV)
    results_df["llm_title"] = None

    succeeded = failed = unknown = 0
    for result in _client.messages.batches.results(state["batch_id"]):
        url = id_to_url.get(result.custom_id)
        if result.result.type != "succeeded":
            failed += 1
            continue
        raw = result.result.message.content[0].text.strip()
        title = None if raw.lower() == "unknown" else raw
        mask = results_df["ex99_url"] == url
        results_df.loc[mask, "llm_title"] = title
        if title:
            print(f"  TITLE  | {title[:70]}", flush=True)
            succeeded += 1
        else:
            print(f"  none   | {url[-50:]}", flush=True)
            unknown += 1

    results_df.to_csv(OUTPUT_CSV, index=False)
    os.remove(STATE_FILE)
    print(f"\nDone. {succeeded} titles found, {unknown} unknown, {failed} failed.")
    print(f"Results → {OUTPUT_CSV}")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--submit",  action="store_true")
    parser.add_argument("--status",  action="store_true")
    parser.add_argument("--collect", action="store_true")
    args = parser.parse_args()

    if args.submit:
        asyncio.run(run_submit())
    elif args.status:
        run_status()
    elif args.collect:
        run_collect()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
