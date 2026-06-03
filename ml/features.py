"""
ml/features.py — turn extracted facts into a modeling table.

This is the code-side feature-engineering layer that sits BETWEEN extraction
(features/runner.py -> data/features_<category>.csv) and the model. It is the
E2 (join-time enrichments) + E3 (code-derived features) layer from
notes/design_review_2026-05-10 that was specified but never built.

Three jobs, all re-runnable without LLM cost and fully auditable:

  1. attach_market()  — point-in-time market_cap / shares_outstanding from
     ticker_details, with a FRESHNESS GUARD: a 2021 event must not borrow a
     2026 market cap (that is both wrong and forward-looking). Stale joins are
     nulled, not silently used.

  2. attach_labels()  — event-anchored forward returns, computed from the raw
     bars via market.fetch_market_data.compute_changes (same semantics as the
     rest of the project: p0 = the 1-min bar just before the news minute).
     COVERAGE IS GATED on the price fetch — bars currently cover ~2024-04+,
     so most historical rows get label_status != ok. That is expected.

  3. engineer()       — the economic transforms. The headline point: raw dollar
     amounts are scale-confounded ($10M is trivial for a $5B co, existential
     for a $20M co) and XGBoost approximates ratios poorly, so the
     economically-linear features (dilution, discount-to-market, moneyness)
     are constructed HERE, explicitly, not left for the tree to discover.

Generic over category via the schema registry (enum one-hots, bool->int,
applicable masks fall out of the FieldSpec types); the per-category economic
ratios live in DERIVERS[category]. Add a category = add a schema + a deriver.

Usage:
  python -m ml.features --category private_placement
  python -m ml.features --category private_placement --no-labels   # skip bar join
"""
import argparse
import os
import numpy as np
import pandas as pd

from features.base import get_schema
import features.schemas  # noqa: F401  (registers category schemas)
from market.fetch_market_data import compute_changes, _OFFSETS_MS

_ET = "America/New_York"

FEATURES_CSV = "data/features_{cat}.csv"
OUT_CSV      = "data/ml_{cat}.csv"
DETAILS_CSV  = "data/prices/ticker_details.csv"
BARS_CSV     = "data/prices/price_bars.csv"
DAILY_CSV    = "data/prices/daily_bars.csv"

ID_COLS      = ["datetime", "ticker", "exchange", "url", "title"]
MKTCAP_FRESH_DAYS = 45   # max |event - details| gap before market cap is too stale to trust


# ── 1. load + coerce (schema-driven) ──────────────────────────────────────────
def load_extracted(category: str) -> pd.DataFrame:
    """Succeeded rows only, with pp_ fields coerced to numbers/bools per schema."""
    schema = get_schema(category)
    df = pd.read_csv(FEATURES_CSV.format(cat=category), dtype=str)
    df = df[df["_extract_status"] == "succeeded"].reset_index(drop=True)
    df["ticker"]   = df["ticker"].str.upper()
    df["dt"]       = pd.to_datetime(df["datetime"], errors="coerce")
    df["date_str"] = df["dt"].dt.strftime("%Y-%m-%d")

    for fs in schema.fields:
        col = f"{schema.prefix}_{fs.name}"
        if col not in df.columns:
            continue
        if fs.dtype in ("number", "integer"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif fs.dtype == "boolean":
            df[col] = df[col].map({"True": 1, "False": 0, "true": 1, "false": 0})
    return df, schema


# ── 2. market join (point-in-time, freshness-guarded) ─────────────────────────
def attach_market(df: pd.DataFrame) -> pd.DataFrame:
    """As-of join nearest ticker_details row per ticker; null it when too stale.

    ticker_details has a few snapshots per ticker (from whenever a fetch ran).
    For an event we want the snapshot nearest in time; a large gap means the
    market cap reflects a different reality than the event saw -> drop it rather
    than feed the model a leaky/wrong denominator."""
    td = pd.read_csv(DETAILS_CSV, dtype=str)
    td["ticker"]  = td["ticker"].str.upper()
    td["d_dt"]    = pd.to_datetime(td["date_str"], errors="coerce")
    td["mc"]      = pd.to_numeric(td["market_cap"], errors="coerce")
    td["shrs"]    = pd.to_numeric(td["weighted_shares_outstanding"], errors="coerce")
    td = td.dropna(subset=["d_dt"]).sort_values("d_dt")

    out = df.copy()
    mc, shrs, gap = [], [], []
    by_tkr = {t: g for t, g in td.groupby("ticker")}
    for _, row in out.iterrows():
        g = by_tkr.get(row["ticker"])
        if g is None or pd.isna(row["dt"]):
            mc.append(np.nan); shrs.append(np.nan); gap.append(np.nan); continue
        i = (g["d_dt"] - row["dt"]).abs().values.argmin()
        gd = abs((g["d_dt"].iloc[i] - row["dt"]).days)
        mc.append(g["mc"].iloc[i]); shrs.append(g["shrs"].iloc[i]); gap.append(gd)

    out["market_cap_asof"] = mc
    out["shares_out_asof"] = shrs
    out["mktcap_gap_days"] = gap
    fresh = out["mktcap_gap_days"] <= MKTCAP_FRESH_DAYS
    out["mktcap_fresh"]    = fresh.astype("Int64")
    # leakage guard: only keep market fields when the snapshot is contemporaneous
    out.loc[~fresh, ["market_cap_asof", "shares_out_asof"]] = np.nan
    return out


# ── 3. label join (event-anchored returns from raw bars) ──────────────────────
def _bars_by_key(path: str, keys: set) -> dict:
    """Load only the (ticker, date_str) groups we need; return {key: [bar,...]}."""
    out: dict = {}
    cols = ["ticker", "date_str", "t", "c"]
    for chunk in pd.read_csv(path, usecols=cols, chunksize=200000):
        chunk["ticker"] = chunk["ticker"].str.upper()
        chunk["k"] = list(zip(chunk["ticker"], chunk["date_str"]))
        sub = chunk[chunk["k"].isin(keys)]
        for k, g in sub.groupby("k"):
            recs = g[["t", "c"]].to_dict("records")
            out.setdefault(k, []).extend(recs)
    for k in out:
        out[k].sort(key=lambda b: b["t"])
    return out


def attach_labels(df: pd.DataFrame) -> pd.DataFrame:
    """Forward returns via the project's own compute_changes. Coverage gated on
    bar availability (label_status='ok' only where bars exist for the event)."""
    out = df.copy()
    keys = set(zip(out["ticker"], out["date_str"]))
    intraday = _bars_by_key(BARS_CSV, keys)
    daily    = _bars_by_key(DAILY_CSV, keys)

    label_cols = [f"ret_{l}" for l in _OFFSETS_MS]
    rows = []
    for _, row in out.iterrows():
        k = (row["ticker"], row["date_str"])
        bars = intraday.get(k)
        rec = {"price_t0": np.nan, **{c: np.nan for c in label_cols}, "label_status": "no_bars"}
        if bars and pd.notna(row["dt"]):
            acc = row["dt"].tz_localize(_ET).isoformat()
            ch = compute_changes(bars, acc, daily=daily.get(k))
            rec["price_t0"] = ch["price_t0"]
            for l in _OFFSETS_MS:
                rec[f"ret_{l}"] = ch[f"change_{l}_pct"]
            rec["label_status"] = "ok" if ch["price_t0"] is not None else "no_price"
        rows.append(rec)
    return pd.concat([out, pd.DataFrame(rows, index=out.index)], axis=1)


# ── 4. engineer: generic schema-driven + per-category economic ratios ─────────
def _generic(df: pd.DataFrame, schema) -> pd.DataFrame:
    """Falls straight out of the FieldSpec types: enum one-hots, bool->int kept,
    string presence flags. The enum one-hots are also what distinguishes
    'not applicable' from 'missing' for the conditional numeric fields."""
    p = schema.prefix
    f = pd.DataFrame(index=df.index)
    for fs in schema.fields:
        col = f"{p}_{fs.name}"
        if col not in df.columns:
            continue
        if fs.dtype == "enum":
            for val in fs.enum:
                f[f"f_{fs.name}_{val}"] = (df[col] == val).astype("Int64")
        elif fs.dtype == "boolean":
            f[f"f_{fs.name}"] = df[col].astype("Int64")
        elif fs.dtype == "string":
            f[f"f_has_{fs.name}"] = df[col].notna().astype("Int64")
    return f


def engineer(df: pd.DataFrame, schema) -> pd.DataFrame:
    f = _generic(df, schema)
    if schema.deriver is not None:
        f = pd.concat([f, schema.deriver(df, schema)], axis=1)
    return f


# ── orchestrate ───────────────────────────────────────────────────────────────
def build(category: str, labels: bool = True) -> pd.DataFrame:
    df, schema = load_extracted(category)
    df = attach_market(df)
    if labels:
        df = attach_labels(df)
    feats = engineer(df, schema)

    keep = ID_COLS + ["date_str", "market_cap_asof", "shares_out_asof",
                      "mktcap_gap_days", "mktcap_fresh"]
    if labels:
        keep += ["price_t0"] + [f"ret_{l}" for l in _OFFSETS_MS] + ["label_status"]
    keep = [c for c in keep if c in df.columns]
    raw = [c for c in df.columns if c.startswith(schema.prefix + "_")]
    return pd.concat([df[keep], feats, df[raw]], axis=1)


def _report(table: pd.DataFrame, category: str, labels: bool):
    n = len(table)
    print(f"\n=== ml_{category}: {n} rows ===")
    fresh = int(table["mktcap_fresh"].fillna(0).sum())
    print(f"market_cap fresh (<= {MKTCAP_FRESH_DAYS}d gap): {fresh}/{n}")
    if labels and "label_status" in table:
        print("label coverage:")
        print(table["label_status"].value_counts().to_string())

    fcols = [c for c in table.columns if c.startswith("f_")]
    print(f"\nengineered feature fill rate ({len(fcols)} features):")
    fill = table[fcols].notna().mean().sort_values(ascending=False)
    for c, v in fill.items():
        print(f"  {v*100:5.1f}%  {c}")

    ratios = ["f_dilution_proceeds", "f_dilution_shares", "f_discount_to_market",
              "f_conv_moneyness", "f_financing_harshness"]
    ratios = [c for c in ratios if c in table.columns]
    print("\nkey economic ratios (describe, non-null):")
    with pd.option_context("display.width", 120):
        print(table[ratios].describe().round(3).to_string())


def main():
    ap = argparse.ArgumentParser(description="Build the ML modeling table from extracted features")
    ap.add_argument("--category", required=True)
    ap.add_argument("--no-labels", action="store_true", help="skip the bar/label join")
    ap.add_argument("--output", default=None)
    args = ap.parse_args()

    table = build(args.category, labels=not args.no_labels)
    out = args.output or OUT_CSV.format(cat=args.category)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    table.to_csv(out, index=False)
    _report(table, args.category, labels=not args.no_labels)
    print(f"\nwrote {len(table)} rows x {len(table.columns)} cols -> {out}")


if __name__ == "__main__":
    main()
