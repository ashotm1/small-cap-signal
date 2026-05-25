"""
classify_catalyst_llm.py — LLM catalyst classification for unclassified press releases.

For rows where is_pr=True and catalyst=['other'], fetches the first ~150 words
of the EX-99 content and uses Claude Sonnet (batch API) to classify the catalyst.
Updates the catalyst column in-place in data/ex_99_classified.csv.

Usage:
  python scripts/classify_catalyst_llm.py --submit-batch   # fetch snippets, submit batch job
  python scripts/classify_catalyst_llm.py --collect-batch  # collect results, update CSV
  python scripts/classify_catalyst_llm.py --status         # check batch progress
"""
import argparse
import ast
import asyncio
import json
import os
import re
import time

import httpx
import pandas as pd
from anthropic import Anthropic

from sec.edgar import fetch_html

# ── Config ─────────────────────────────────────────────────────────────────────
INPUT_CSV        = "data/ex_99_classified.csv"
BATCH_STATE_FILE = "data/llm_classifier_batch.json"
BATCH_SIZE       = 10
BATCH_INTERVAL   = 1.0
LLM_INTERVAL     = 1.2
SNIPPET_WORDS    = 150
MAX_TOKENS       = 20
MODEL            = "claude-sonnet-4-6"

_VALID_TAGS = {
    "biotech", "private_placement", "collaboration", "m&a",
    "new_product", "contract", "crypto_treasury", "earnings",
    "other", "unclear", "issue",
}

_SYSTEM = """You are a financial press release classifier.

Given the opening text of a press release, output exactly one of these categories.
If you are less than 90% confident, output 'unclear'.

biotech — drug trials, FDA approvals, clinical data, biotech milestones
private_placement — private placement of shares or warrants
collaboration — strategic partnerships, licensing deals, co-development agreements
m&a — mergers, acquisitions, divestitures, tender offers
new_product — product launches, new technology introductions
contract — contract wins, orders received, grants awarded
crypto_treasury — bitcoin/crypto treasury adoption, BTC/ETH holdings announcements
earnings — financial results, quarterly/annual earnings, revenue reports
other — primary category is clear but does not match any of the listed categories above
unclear — could match one of the above but not enough information to determine which
issue — title is boilerplate, truncated mid sentence, or not a real headline

If multiple categories could apply, choose the most specific one.
Reply with the category name only. No punctuation, no explanation."""

_anthropic_sync = Anthropic()


# ── Helpers ────────────────────────────────────────────────────────────────────

def _extract_snippet(html: str, max_words: int = SNIPPET_WORDS) -> str:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    text = " ".join(soup.stripped_strings)
    text = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", text)
    words = text.split()
    skip = re.compile(r"^(EX-\d+\.\d+|Exhibit|\S+\.html?|\d+\.\d+|\d+)$", re.IGNORECASE)
    while words and skip.match(words[0]):
        words.pop(0)
    return " ".join(words[:max_words])


MALFORMED_CSV = "data/llm_malformed.csv"

def _parse_tag(raw: str, url: str = "") -> str:
    tag = raw.strip().lower().rstrip(".")
    if tag not in _VALID_TAGS:
        row = pd.DataFrame([{"raw_output": raw, "ex99_url": url}])
        write_header = not os.path.exists(MALFORMED_CSV)
        row.to_csv(MALFORMED_CSV, mode="a", header=write_header, index=False)
        return "other"
    return tag


def _load_pending(sample: int = 0) -> pd.DataFrame:
    df = pd.read_csv(INPUT_CSV)

    def is_other(val):
        try:
            tags = ast.literal_eval(val) if isinstance(val, str) else val
        except Exception:
            tags = [val]
        return tags == ["other"]

    mask = (df["is_pr"] == True) & df["catalyst"].apply(is_other)
    pending = df[mask].reset_index(drop=True)
    if sample and len(pending) > sample:
        pending = pending.sample(sample, random_state=42).reset_index(drop=True)
    print(f"Loaded {len(pending)} 'other' PR rows pending LLM classification")
    return pending


def _apply_updates(updates: dict):
    """updates: {ex99_url: new_tag}. Rewrites catalyst + catalyst_source in-place."""
    df = pd.read_csv(INPUT_CSV)
    if "catalyst_source" not in df.columns:
        df["catalyst_source"] = None

    for url, tag in updates.items():
        mask = df["ex99_url"] == url
        df.loc[mask, "catalyst"] = str([tag])
        df.loc[mask, "catalyst_source"] = "llm"

    df.to_csv(INPUT_CSV, index=False)
    print(f"Updated {len(updates)} rows in {INPUT_CSV}")


# ── Batch submit mode ──────────────────────────────────────────────────────────

async def _fetch_all_snippets(df: pd.DataFrame) -> dict:
    snippets = {}
    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]
            t_start = time.monotonic()
            htmls = await asyncio.gather(*[
                fetch_html(client, row["ex99_url"]) for _, row in batch.iterrows()
            ])
            for (_, row), html in zip(batch.iterrows(), htmls):
                url = row["ex99_url"]
                if html is None:
                    snippets[url] = None
                    print(f"  fetch failed  | {row['company']}", flush=True)
                else:
                    text = _extract_snippet(html)
                    snippets[url] = text if text else None
                    print(f"  fetched       | {row['company']}", flush=True)
            elapsed = time.monotonic() - t_start
            remaining = BATCH_INTERVAL - elapsed
            if remaining > 0 and i + BATCH_SIZE < len(df):
                await asyncio.sleep(remaining)
    return snippets


def _submit_batch(snippets: dict) -> tuple[str, dict]:
    """Returns (batch_id, id_to_url mapping)."""
    requests = []
    id_to_url = {}  # custom_id (index str) → ex99_url
    for i, (url, snippet) in enumerate(snippets.items()):
        if not snippet:
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
                "messages": [{"role": "user", "content": snippet}],
            },
        })
    print(f"\nSubmitting {len(requests)} requests to Anthropic Batch API...", flush=True)
    batch = _anthropic_sync.messages.batches.create(requests=requests)
    print(f"  Batch ID: {batch.id}", flush=True)
    print(f"  Status:   {batch.processing_status}", flush=True)
    return batch.id, id_to_url


async def run_submit_batch(sample: int = 0):
    df = _load_pending(sample)
    if df.empty:
        print("Nothing to process.")
        return
    if os.path.exists(BATCH_STATE_FILE):
        print(f"State file already exists at {BATCH_STATE_FILE}.")
        print("Run --collect-batch or delete the state file to resubmit.")
        return

    print(f"\nFetching snippets from SEC ({len(df)} PRs)...")
    snippets = await _fetch_all_snippets(df)

    batch_id, id_to_url = _submit_batch(snippets)

    state = {"batch_id": batch_id, "id_to_url": id_to_url}
    with open(BATCH_STATE_FILE, "w") as f:
        json.dump(state, f)
    print(f"\nState saved to {BATCH_STATE_FILE}")
    print("Run --collect-batch when ready.")


# ── Run mode (submit + poll + collect) ────────────────────────────────────────

POLL_INTERVAL = 30  # seconds between status checks

async def run_full(sample: int = 0):
    """Submit batch, poll until done, then collect. Blocks until complete."""
    await run_submit_batch(sample)

    if not os.path.exists(BATCH_STATE_FILE):
        return  # nothing was submitted (empty or already exists)

    with open(BATCH_STATE_FILE) as f:
        state = json.load(f)
    batch_id = state["batch_id"]

    print(f"\nPolling batch {batch_id} every {POLL_INTERVAL}s...", flush=True)
    while True:
        batch = _anthropic_sync.messages.batches.retrieve(batch_id)
        c = batch.request_counts
        total = c.processing + c.succeeded + c.errored + c.canceled + c.expired
        done = total - c.processing
        print(f"  {batch.processing_status}  {done}/{total}", flush=True)
        if batch.processing_status == "ended":
            break
        time.sleep(POLL_INTERVAL)

    run_collect_batch()


# ── Status + collect ───────────────────────────────────────────────────────────

def run_status():
    if not os.path.exists(BATCH_STATE_FILE):
        print(f"No state file at {BATCH_STATE_FILE}.")
        return
    with open(BATCH_STATE_FILE) as f:
        state = json.load(f)
    batch = _anthropic_sync.messages.batches.retrieve(state["batch_id"])
    c = batch.request_counts
    total = c.processing + c.succeeded + c.errored + c.canceled + c.expired
    done = total - c.processing
    print(f"Batch {batch.id}  {batch.processing_status}")
    print(f"  {done}/{total} done  ({c.succeeded} ok, {c.errored} err, {c.processing} processing)")


def run_collect_batch():
    if not os.path.exists(BATCH_STATE_FILE):
        print(f"No state file at {BATCH_STATE_FILE}.")
        return
    with open(BATCH_STATE_FILE) as f:
        state = json.load(f)

    batch = _anthropic_sync.messages.batches.retrieve(state["batch_id"])
    print(f"Batch {state['batch_id']}: {batch.processing_status}")
    if batch.processing_status != "ended":
        print("Not ready yet. Try again later.")
        return

    id_to_url = state["id_to_url"]
    updates = {}
    succeeded = failed = 0
    for result in _anthropic_sync.messages.batches.results(state["batch_id"]):
        url = id_to_url.get(result.custom_id, result.custom_id)
        if result.result.type == "succeeded":
            tag = _parse_tag(result.result.message.content[0].text, url)
            updates[url] = tag
            print(f"  {tag:20s} | {url[:60]}", flush=True)
            succeeded += 1
        else:
            print(f"  failed        | {url[:60]}", flush=True)
            failed += 1

    _apply_updates(updates)
    print(f"\nDone. {succeeded} succeeded, {failed} failed.")
    os.remove(BATCH_STATE_FILE)
    print("State file removed.")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="LLM catalyst classifier for 'other' PR rows"
    )
    parser.add_argument("--run", action="store_true",
                        help="Submit batch, poll until done, then collect (blocking)")
    parser.add_argument("--submit-batch", action="store_true",
                        help="Fetch snippets and submit Anthropic batch job")
    parser.add_argument("--collect-batch", action="store_true",
                        help="Collect batch results and update CSV")
    parser.add_argument("--status", action="store_true",
                        help="Check batch job progress")
    parser.add_argument("--sample", type=int, default=0, metavar="N",
                        help="Limit to N random 'other' rows (for testing)")
    args = parser.parse_args()

    if args.run:
        asyncio.run(run_full(args.sample))
    elif args.submit_batch:
        asyncio.run(run_submit_batch(args.sample))
    elif args.collect_batch:
        run_collect_batch()
    elif args.status:
        run_status()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
