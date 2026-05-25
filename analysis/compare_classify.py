"""
compare_classify.py — Compare 4 classification approaches on 200 sample 'other' PR titles.

Approaches:
  sep     — 200 batch requests, 1 title per request
  10comb  — 20 batch requests, 10 titles per request
  50comb  —  4 batch requests, 50 titles per request
  1call   — 1 real-time API call, all 200 titles in one prompt

All share the same 200-row sample (data/compare_sample.csv).
Results written to data/compare_results.csv with columns per mode.

Usage:
  python utils/compare_classify.py --build-sample
  python utils/compare_classify.py --submit sep|10comb|50comb
  python utils/compare_classify.py --collect sep|10comb|50comb
  python utils/compare_classify.py --status  sep|10comb|50comb
  python utils/compare_classify.py --run-1call
"""
import argparse
import ast
import json
import os
import sys

import pandas as pd
from anthropic import Anthropic

# ── Config ─────────────────────────────────────────────────────────────────────
CLASSIFIED_CSV  = "data/ex_99_classified.csv"
SAMPLE_CSV      = "data/compare_sample.csv"
RESULTS_CSV     = "data/compare_results.csv"
STATE_DIR       = "data"
SAMPLE_SIZE     = 200
MODEL           = "claude-sonnet-4-6"
MAX_TOKENS_SEP  = 20
MAX_TOKENS_COMB = 1024  # multi-row needs more room

_VALID_TAGS = {
    "biotech", "private_placement", "collaboration", "m&a",
    "new_product", "contract", "crypto_treasury", "earnings",
    "other", "unclear", "issue",
}

_SYSTEM = """You are a financial press release classifier.

Given the title of a press release, output exactly one of these categories.
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
issue — title is boilerplate, truncated, or not a real headline

If multiple categories could apply, choose the most specific one.
Reply with the category name only. No punctuation, no explanation."""

_SYSTEM_MULTI = """You are a financial press release classifier.

For each numbered title below, output the category on a new line as: N. category
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
issue — title is boilerplate, truncated, or not a real headline

Output exactly one line per title in the same order. No extra text."""

_client = Anthropic()

MODE_CHUNKS = {"sep": 1, "10comb": 10, "50comb": 50}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _state_path(mode: str) -> str:
    return os.path.join(STATE_DIR, f"compare_{mode}_state.json")


def _parse_tag(raw: str) -> str:
    tag = raw.strip().lower().rstrip(".")
    return tag if tag in _VALID_TAGS else None


def _parse_multi(raw: str, n: int) -> list[str | None]:
    """Parse N-line numbered response into list of tags."""
    tags = [None] * n
    for line in raw.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if "." in line:
            parts = line.split(".", 1)
            try:
                idx = int(parts[0].strip()) - 1
                if 0 <= idx < n:
                    tags[idx] = _parse_tag(parts[1].strip())
            except ValueError:
                pass
    return tags


def _load_sample() -> pd.DataFrame:
    if not os.path.exists(SAMPLE_CSV):
        print(f"Sample not found. Run --build-sample first.")
        sys.exit(1)
    return pd.read_csv(SAMPLE_CSV)


def _load_results() -> pd.DataFrame:
    if os.path.exists(RESULTS_CSV):
        df = pd.read_csv(RESULTS_CSV)
        for mode in ("sep", "10comb", "50comb", "1call"):
            for col in (f"tag_{mode}", f"raw_{mode}"):
                if col not in df.columns:
                    df[col] = None
                df[col] = df[col].astype(object)
        return df
    sample = _load_sample()
    df = sample[["row_num", "ex99_url", "title"]].copy()
    for mode in ("sep", "10comb", "50comb", "1call"):
        df[f"tag_{mode}"] = None
        df[f"raw_{mode}"] = None
    return df


def _save_results(df: pd.DataFrame):
    df.to_csv(RESULTS_CSV, index=False)


# ── Build sample ───────────────────────────────────────────────────────────────

def build_sample():
    df = pd.read_csv(CLASSIFIED_CSV)

    def is_other(val):
        try:
            tags = ast.literal_eval(val) if isinstance(val, str) else val
        except Exception:
            tags = [val]
        return tags == ["other"]

    mask = (df["is_pr"] == True) & df["catalyst"].apply(is_other) & df["title"].notna()
    pool = df[mask].reset_index(drop=True)
    print(f"Pool: {len(pool)} rows with is_pr=True, catalyst=other, title present")

    sample = pool.sample(min(SAMPLE_SIZE, len(pool)), random_state=42).reset_index(drop=True)
    sample.insert(0, "row_num", range(1, len(sample) + 1))
    sample.to_csv(SAMPLE_CSV, index=False)
    print(f"Sample saved: {len(sample)} rows → {SAMPLE_CSV}")


# ── Submit ─────────────────────────────────────────────────────────────────────

def submit(mode: str):
    state_path = _state_path(mode)
    if os.path.exists(state_path):
        print(f"State file exists: {state_path}. Run --collect or delete to resubmit.")
        return

    sample = _load_sample()
    chunk_size = MODE_CHUNKS[mode]
    rows = sample.to_dict("records")
    requests = []
    id_map = {}  # custom_id → (start_row_num, chunk_size)

    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        custom_id = str(i)
        id_map[custom_id] = [r["row_num"] for r in chunk]

        if chunk_size == 1:
            content = chunk[0]["title"]
            system  = _SYSTEM
            max_tok = MAX_TOKENS_SEP
        else:
            lines   = "\n".join(f"[{j+1}] {r['title']}" for j, r in enumerate(chunk))
            content = lines
            system  = _SYSTEM_MULTI
            max_tok = MAX_TOKENS_COMB

        requests.append({
            "custom_id": custom_id,
            "params": {
                "model": MODEL,
                "max_tokens": max_tok,
                "temperature": 0,
                "system": system,
                "messages": [{"role": "user", "content": content}],
            },
        })

    print(f"Submitting {len(requests)} requests (mode={mode})...")
    batch = _client.messages.batches.create(requests=requests)
    print(f"  Batch ID: {batch.id}  Status: {batch.processing_status}")

    state = {"batch_id": batch.id, "id_map": id_map, "mode": mode}
    with open(state_path, "w") as f:
        json.dump(state, f)
    print(f"State saved → {state_path}")


# ── Status ─────────────────────────────────────────────────────────────────────

def status(mode: str):
    state_path = _state_path(mode)
    if not os.path.exists(state_path):
        print(f"No state file for mode '{mode}'.")
        return
    with open(state_path) as f:
        state = json.load(f)
    batch = _client.messages.batches.retrieve(state["batch_id"])
    c = batch.request_counts
    total = c.processing + c.succeeded + c.errored + c.canceled + c.expired
    print(f"Batch {batch.id}  {batch.processing_status}  {total - c.processing}/{total}")


# ── Collect ────────────────────────────────────────────────────────────────────

def collect(mode: str):
    state_path = _state_path(mode)
    if not os.path.exists(state_path):
        print(f"No state file for mode '{mode}'.")
        return
    with open(state_path) as f:
        state = json.load(f)

    batch = _client.messages.batches.retrieve(state["batch_id"])
    if batch.processing_status != "ended":
        print(f"Not ready yet: {batch.processing_status}")
        return

    id_map   = state["id_map"]
    chunk_size = MODE_CHUNKS[mode]
    results_df = _load_results()

    for result in _client.messages.batches.results(state["batch_id"]):
        row_nums = id_map.get(result.custom_id, [])
        if not row_nums:
            continue

        if result.result.type != "succeeded":
            for rn in row_nums:
                mask = results_df["row_num"] == rn
                results_df.loc[mask, f"raw_{mode}"] = "API_ERROR"
            continue

        raw = result.result.message.content[0].text.strip()

        if chunk_size == 1:
            tag = _parse_tag(raw)
            rn  = row_nums[0]
            mask = results_df["row_num"] == rn
            results_df.loc[mask, f"tag_{mode}"] = tag
            results_df.loc[mask, f"raw_{mode}"] = raw
            print(f"  [{rn:>3}] {tag or 'PARSE_FAIL':20s} | {raw}")
        else:
            tags = _parse_multi(raw, len(row_nums))
            for rn, tag in zip(row_nums, tags):
                mask = results_df["row_num"] == rn
                results_df.loc[mask, f"tag_{mode}"] = tag
                results_df.loc[mask, f"raw_{mode}"] = raw if tag is None else None
                print(f"  [{rn:>3}] {str(tag or 'PARSE_FAIL'):20s}")

    _save_results(results_df)
    os.remove(state_path)
    print(f"\nResults saved → {RESULTS_CSV}")
    print(f"State file removed.")


# ── Single real-time call ──────────────────────────────────────────────────────

def run_1call():
    sample = _load_sample()
    rows = sample.to_dict("records")
    lines = "\n".join(f"[{r['row_num']}] {r['title']}" for r in rows)

    print(f"Sending 1 real-time API call with {len(rows)} titles...")
    response = _client.messages.create(
        model=MODEL,
        max_tokens=2048,
        temperature=0,
        system=_SYSTEM_MULTI,
        messages=[{"role": "user", "content": lines}],
    )
    raw = response.content[0].text.strip()
    print(f"Response received ({len(raw.splitlines())} lines)")

    # Build per-row line map from response
    line_map = {}
    for line in raw.strip().splitlines():
        line = line.strip()
        if "." in line:
            parts = line.split(".", 1)
            try:
                idx = int(parts[0].strip())
                line_map[idx] = parts[1].strip()
            except ValueError:
                pass

    tags = _parse_multi(raw, len(rows))
    results_df = _load_results()

    for r, tag in zip(rows, tags):
        rn = r["row_num"]
        raw_line = line_map.get(rn, None)
        mask = results_df["row_num"] == rn
        results_df.loc[mask, "tag_1call"] = tag
        results_df.loc[mask, "raw_1call"] = raw_line
        print(f"  [{rn:>3}] {str(tag or 'PARSE_FAIL'):20s} | {raw_line}")

    _save_results(results_df)
    print(f"\nResults saved → {RESULTS_CSV}")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-sample", action="store_true")
    parser.add_argument("--submit",    metavar="MODE", choices=["sep", "10comb", "50comb"])
    parser.add_argument("--collect",   metavar="MODE", choices=["sep", "10comb", "50comb"])
    parser.add_argument("--status",    metavar="MODE", choices=["sep", "10comb", "50comb"])
    parser.add_argument("--run-1call", action="store_true")
    args = parser.parse_args()

    if args.build_sample:
        build_sample()
    elif args.submit:
        submit(args.submit)
    elif args.collect:
        collect(args.collect)
    elif args.status:
        status(args.status)
    elif args.run_1call:
        run_1call()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
