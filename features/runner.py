"""
features/runner.py — per-category structured feature extraction from article
bodies, via the Anthropic Batch API.

This is the runner. It is generic over the catalyst category: it loads the
matching FeatureSchema from features/schemas/, filters the input rows to that
category, and asks the model to extract that category's typed fields from each
article body into one wide, namespaced output row. To work on a new category
(e.g. crypto_treasury) you add a schema module — this file does not change.

Design:
  * One body per request — NOT multiple bodies per prompt. Full attention per
    document is what gives good recall on a ~25-field schema with strict null
    discipline.
  * The shared prefix (system prompt + field definitions) is cached with a 5-min
    TTL. Because batch requests run concurrently, the first ones can't read a
    cache the others are still writing, so we PRE-WARM the cache with one
    max_tokens=0 request right before submitting.
  * Plain JSON-mode (not strict structured output): strict structured output caps
    nullable unions at 16 and our schemas exceed that. JSON is a prompt rule;
    _parse_features is tolerant. "Not stated" -> null; the prompt forbids guessing.
  * effort / thinking are intentionally NOT set, so the same runner works on
    Sonnet 4.6 and Haiku 4.5 unchanged — that's the A/B knob for the fill-rate pass.

Usage:
  python -m features.runner --category private_placement --run
  python -m features.runner --category private_placement --submit-batch
  python -m features.runner --category private_placement --collect-batch
  python -m features.runner --category private_placement --status
  # A/B a cheaper model on a sample:
  python -m features.runner --category private_placement --model claude-haiku-4-5 --sample 150 --run
"""
import argparse
import json
import os
import re
import sys
import time

import pandas as pd
from anthropic import Anthropic

from features.base import get_schema
import features.schemas  # registers all category schemas (requires private repo)
from regex.catalysts import classify_catalyst

# ── Config ────────────────────────────────────────────────────────────────────
DEFAULT_INPUT   = "data/gnw_signal_articles.csv"
DEFAULT_MODEL   = "claude-sonnet-4-6"   # null discipline + enum disambiguation;
                                        # A/B against claude-haiku-4-5 via --model
MAX_TOKENS      = 1500
MAX_BODY_CHARS  = 6000                   # bodies front-load the deal terms
MIN_BODY_CHARS  = 300                    # skip stubs/failed fetches
POLL_INTERVAL   = 30                     # seconds between batch status checks

ID_COLS = ["datetime", "ticker", "exchange", "url", "title"]

_client = Anthropic()


def _paths(category: str, output: str | None):
    out = output or f"data/features_{category}.csv"
    state = f"data/features_{category}_batch.json"
    return out, state


def _clean_body(text: str, max_chars: int) -> str:
    return " ".join(str(text).split())[:max_chars]


def _cached_system(schema) -> list:
    """System prompt as the shared prefix. 5-min TTL, NOT 1h: in a concurrent
    batch most requests can't read a sibling's in-flight write and write their
    own copy, so writes dominate — and a 1h write is 2x base vs 1.25x for 5-min.
    With a small prefix like this, caching is marginal either way (the 50% batch
    discount is the real lever); 5-min keeps it from going net-negative."""
    return [{
        "type": "text",
        "text": schema.system_prompt(),
        "cache_control": {"type": "ephemeral"},   # 5-min default
    }]


# ── Load + filter input ───────────────────────────────────────────────────────
def _load_pending(category, input_csv, done_urls, sample, limit, max_body):
    """Stream the (large) input CSV in chunks, keep rows of this category that
    have a real body and aren't already done. Returns a DataFrame with ID_COLS
    plus a cleaned 'body' column."""
    want = ID_COLS + ["catalyst", "article_body"]
    kept = []
    for chunk in pd.read_csv(input_csv, dtype=str, chunksize=20000):
        if "article_body" not in chunk.columns:
            raise SystemExit(f"{input_csv} has no 'article_body' column (cols: {list(chunk.columns)})")
        cols = [c for c in want if c in chunk.columns]
        sub = chunk[cols].copy()

        if "catalyst" in sub.columns:
            mask = sub["catalyst"].fillna("").str.contains(category, regex=False)
        else:  # no precomputed tags — classify from the title
            mask = sub.get("title", pd.Series("", index=sub.index)).fillna("").map(
                lambda t: category in classify_catalyst(t))
        sub = sub[mask]

        sub = sub[sub["article_body"].fillna("").str.len() >= MIN_BODY_CHARS]
        if done_urls and "url" in sub.columns:
            sub = sub[~sub["url"].isin(done_urls)]
        if not sub.empty:
            kept.append(sub)

    if not kept:
        return pd.DataFrame(columns=ID_COLS + ["body"])
    df = pd.concat(kept, ignore_index=True)
    df["body"] = df["article_body"].map(lambda b: _clean_body(b, max_body))
    for c in ID_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[ID_COLS + ["body"]]
    if sample and len(df) > sample:
        df = df.sample(sample, random_state=42).reset_index(drop=True)
    if limit:
        df = df.head(limit)
    return df


def _done_urls(output_csv) -> set:
    if not os.path.exists(output_csv):
        return set()
    done = set()
    for chunk in pd.read_csv(output_csv, dtype=str,
                             usecols=["url", "_extract_status"], chunksize=50000):
        succeeded = chunk["_extract_status"] == "succeeded"
        done.update(chunk.loc[succeeded, "url"].dropna())
    return done


# ── Cache pre-warm ────────────────────────────────────────────────────────────
def _prewarm_cache(schema, model):
    """Write the shared-prefix cache once before the concurrent batch runs.
    max_tokens=0 prefills + returns immediately; it cannot carry output_config
    or live inside a batch, so it's a plain standalone call."""
    try:
        msg = _client.messages.create(
            model=model,
            max_tokens=0,
            system=_cached_system(schema),
            messages=[{"role": "user", "content": "warmup"}],
        )
        u = msg.usage
        print(f"  prewarm: cache_write={u.cache_creation_input_tokens} "
              f"cache_read={u.cache_read_input_tokens}", flush=True)
    except Exception as e:
        # Non-fatal — the batch still runs, just with weaker cache reads.
        print(f"  prewarm skipped ({type(e).__name__}: {e})", flush=True)


# ── Submit ────────────────────────────────────────────────────────────────────
def _build_requests(df, schema, model):
    # Plain JSON-mode (not output_config.format strict schema): structured
    # outputs caps union/nullable params at 16 and our schemas have more, so we
    # ask for JSON in the prompt and parse tolerantly. null discipline is a
    # prompt rule; _parse_features handles fences / missing keys.
    system = _cached_system(schema)
    requests, id_to_row = [], {}
    for i, row in df.iterrows():
        cid = str(i)
        id_to_row[cid] = {c: row.get(c, "") for c in ID_COLS}
        requests.append({
            "custom_id": cid,
            "params": {
                "model": model,
                "max_tokens": MAX_TOKENS,
                "system": system,
                "messages": [{"role": "user", "content": row["body"]}],
            },
        })
    return requests, id_to_row


def run_submit_batch(args):
    schema = get_schema(args.category)
    out_csv, state_file = _paths(args.category, args.output)
    if os.path.exists(state_file):
        print(f"State file exists at {state_file}. Run --collect-batch or delete it to resubmit.")
        return

    df = _load_pending(args.category, args.input, _done_urls(out_csv),
                       args.sample, args.limit, args.max_body_chars)
    print(f"{args.category}: {len(df)} rows pending extraction")
    if df.empty:
        return

    print(f"prewarming cache (model={args.model})...", flush=True)
    _prewarm_cache(schema, args.model)

    requests, id_to_row = _build_requests(df, schema, args.model)
    print(f"submitting {len(requests)} requests to the Batch API...", flush=True)
    batch = _client.messages.batches.create(requests=requests)
    print(f"  batch id: {batch.id}  status: {batch.processing_status}", flush=True)

    with open(state_file, "w") as f:
        json.dump({"batch_id": batch.id, "model": args.model,
                   "schema_version": schema.version, "id_to_row": id_to_row}, f)
    print(f"state saved to {state_file}. Run --collect-batch when ended.")


# ── Status + collect ──────────────────────────────────────────────────────────
def _load_state(state_file):
    if not os.path.exists(state_file):
        print(f"No state file at {state_file}.")
        return None
    with open(state_file) as f:
        return json.load(f)


def run_status(args):
    _, state_file = _paths(args.category, args.output)
    state = _load_state(state_file)
    if not state:
        return
    b = _client.messages.batches.retrieve(state["batch_id"])
    c = b.request_counts
    total = c.processing + c.succeeded + c.errored + c.canceled + c.expired
    print(f"batch {b.id}  {b.processing_status}  "
          f"{total - c.processing}/{total} done "
          f"({c.succeeded} ok, {c.errored} err, {c.processing} processing)")


def _parse_features(message, schema) -> tuple[dict, str]:
    """Parse the model's JSON object, tolerating code fences. Missing keys ->
    None via schema.namespaced."""
    text = next((b.text for b in message.content if b.type == "text"), "")
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.MULTILINE).strip()
    try:
        raw = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return {}, "no_json"
    if not isinstance(raw, dict):
        return {}, "no_json"
    return schema.namespaced(raw), "succeeded"


def run_collect_batch(args):
    schema = get_schema(args.category)
    out_csv, state_file = _paths(args.category, args.output)
    state = _load_state(state_file)
    if not state:
        return

    b = _client.messages.batches.retrieve(state["batch_id"])
    print(f"batch {b.id}: {b.processing_status}")
    if b.processing_status != "ended":
        print("Not ready yet. Try again later.")
        return

    id_to_row = state["id_to_row"]
    empty = {c: None for c in schema.column_names()}
    fieldnames = ID_COLS + schema.column_names() + ["_schema_version", "_model", "_extract_status"]

    rows, ok, bad = [], 0, 0
    for result in _client.messages.batches.results(state["batch_id"]):
        meta = id_to_row.get(result.custom_id, {})
        base = {c: meta.get(c, "") for c in ID_COLS}
        if result.result.type == "succeeded":
            feats, status = _parse_features(result.result.message, schema)
            rows.append({**base, **empty, **feats,
                         "_schema_version": state["schema_version"],
                         "_model": state["model"], "_extract_status": status})
            ok += 1
        else:
            rows.append({**base, **empty,
                         "_schema_version": state["schema_version"],
                         "_model": state["model"],
                         "_extract_status": result.result.type})
            bad += 1

    out_df = pd.DataFrame(rows, columns=fieldnames)
    write_header = not os.path.exists(out_csv) or os.path.getsize(out_csv) == 0
    out_df.to_csv(out_csv, mode="a", header=write_header, index=False)
    print(f"wrote {len(rows)} rows to {out_csv} ({ok} ok, {bad} failed)")
    os.remove(state_file)
    print("state file removed.")


def run_full(args):
    run_submit_batch(args)
    _, state_file = _paths(args.category, args.output)
    state = _load_state(state_file)
    if not state:
        return
    print(f"polling batch {state['batch_id']} every {POLL_INTERVAL}s...", flush=True)
    while True:
        b = _client.messages.batches.retrieve(state["batch_id"])
        c = b.request_counts
        total = c.processing + c.succeeded + c.errored + c.canceled + c.expired
        print(f"  {b.processing_status}  {total - c.processing}/{total}", flush=True)
        if b.processing_status == "ended":
            break
        time.sleep(POLL_INTERVAL)
    run_collect_batch(args)


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description="Per-category structured feature extraction (Batch API)")
    p.add_argument("--category", required=True, help="catalyst category, e.g. private_placement")
    p.add_argument("--input", default=DEFAULT_INPUT, help="input CSV with article_body (+ catalyst)")
    p.add_argument("--output", default=None, help="output CSV (default data/features_<category>.csv)")
    p.add_argument("--model", default=DEFAULT_MODEL, help="extraction model")
    p.add_argument("--sample", type=int, default=0, help="limit to N random rows (testing / fill-rate)")
    p.add_argument("--limit", type=int, default=0, help="cap to first N pending rows")
    p.add_argument("--max-body-chars", type=int, default=MAX_BODY_CHARS, dest="max_body_chars")
    p.add_argument("--run", action="store_true", help="submit, poll, collect (blocking)")
    p.add_argument("--submit-batch", action="store_true")
    p.add_argument("--collect-batch", action="store_true")
    p.add_argument("--status", action="store_true")
    args = p.parse_args()

    if args.run:
        run_full(args)
    elif args.submit_batch:
        run_submit_batch(args)
    elif args.collect_batch:
        run_collect_batch(args)
    elif args.status:
        run_status(args)
    else:
        p.print_help()


if __name__ == "__main__":
    main()
