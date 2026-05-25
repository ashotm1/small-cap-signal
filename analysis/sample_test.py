"""
sample_test.py — Build a stratified sample and compare haiku vs sonnet extraction quality.

Usage:
  python utils/sample_test.py --build-sample          # build data/sample_200.csv
  python utils/sample_test.py --compare               # diff haiku vs sonnet outputs

Extraction (uses extract_features.py batch mode):
  python scripts/extract_features.py --input data/sample_200.csv --output data/sample_haiku.csv --submit-batch
  python scripts/extract_features.py --input data/sample_200.csv --output data/sample_haiku.csv --collect-batch

  python scripts/extract_features.py --input data/sample_200.csv --output data/sample_sonnet.csv --model claude-sonnet-4-6 --submit-batch
  python scripts/extract_features.py --input data/sample_200.csv --output data/sample_sonnet.csv --model claude-sonnet-4-6 --collect-batch
"""
import argparse
import os

import pandas as pd

# ── Config ─────────────────────────────────────────────────────────────────────
INPUT_CSV    = "data/ex_99_classified.csv"
SAMPLE_CSV   = "data/sample_200.csv"
PER_BUCKET   = 30
TOTAL        = 240
SIGNAL_CATALYSTS = {
    "biotech", "private_placement", "collaboration", "m&a",
    "new_product", "contract", "crypto_treasury",
}

SCORE_FIELDS = [
    "commitment_level", "significance_score", "specificity_score", "hype_score",
]
BOOL_FIELDS = [
    "has_named_partner", "is_dilutive", "milestone_guidance",
    "has_quantified_impact", "is_restatement",
]


# ── Sample builder ─────────────────────────────────────────────────────────────

def build_sample() -> pd.DataFrame:
    df = pd.read_csv(INPUT_CSV)
    df = df[df["is_pr"] == True].reset_index(drop=True)

    def parse_catalyst(x):
        if isinstance(x, list):
            return x
        if isinstance(x, str):
            try:
                import ast
                return ast.literal_eval(x)
            except Exception:
                return [x]
        return ["other"]

    df["catalyst"] = df["catalyst"].apply(parse_catalyst)
    df["primary_catalyst"] = df["catalyst"].apply(lambda x: x[0] if x else "other")
    df = df[df["catalyst"].apply(lambda tags: any(t in SIGNAL_CATALYSTS for t in tags))].reset_index(drop=True)

    sample = (
        df.groupby(df["primary_catalyst"], group_keys=False)
        .apply(lambda g: g.sample(min(len(g), PER_BUCKET), random_state=42))
        .reset_index(drop=True)
    )
    sample = sample.sample(min(len(sample), TOTAL), random_state=42).reset_index(drop=True)
    sample["primary_catalyst"] = sample["catalyst"].apply(
        lambda tags: next((t for t in tags if t in SIGNAL_CATALYSTS), tags[0] if tags else "other")
    )
    sample.to_csv(SAMPLE_CSV, index=False)

    print(f"Sample built: {len(sample)} PRs")
    print(sample["primary_catalyst"].value_counts().to_string())
    return sample



# ── Comparison ─────────────────────────────────────────────────────────────────

def run_compare():
    haiku_path  = "data/sample_haiku.csv"
    sonnet_path = "data/sample_sonnet.csv"

    if not os.path.exists(haiku_path) or not os.path.exists(sonnet_path):
        print("Both sample_haiku.csv and sample_sonnet.csv must exist. Run --model haiku and --model sonnet first.")
        return

    h = pd.read_csv(haiku_path).set_index("ex99_url")
    s = pd.read_csv(sonnet_path).set_index("ex99_url")
    common = h.index.intersection(s.index)
    h, s = h.loc[common], s.loc[common]

    print(f"\nComparing {len(common)} PRs\n")

    # Score field stats
    print("── Score field means ────────────────────────────────")
    print(f"{'Field':25s}  {'Haiku':>8}  {'Sonnet':>8}  {'Δ mean':>8}  {'Δ std':>8}")
    for field in SCORE_FIELDS:
        hv = pd.to_numeric(h[field], errors="coerce")
        sv = pd.to_numeric(s[field], errors="coerce")
        diff = (sv - hv).dropna()
        print(
            f"  {field:23s}  {hv.mean():8.2f}  {sv.mean():8.2f}"
            f"  {diff.mean():+8.2f}  {diff.std():8.2f}"
        )

    # Bool agreement
    print("\n── Bool field agreement ─────────────────────────────")
    for field in BOOL_FIELDS:
        hv = h[field].astype(str).str.lower()
        sv = s[field].astype(str).str.lower()
        agree = (hv == sv).sum()
        print(f"  {field:25s}  {agree}/{len(common)} agree ({agree/len(common)*100:.0f}%)")

    # Biggest divergences on score fields
    print("\n── Top 10 biggest divergences (|Δ commitment_level + Δ significance_score|) ──")
    h_scores = h[SCORE_FIELDS].apply(pd.to_numeric, errors="coerce")
    s_scores = s[SCORE_FIELDS].apply(pd.to_numeric, errors="coerce")
    delta = (s_scores - h_scores).abs().sum(axis=1).sort_values(ascending=False)

    for url in delta.head(10).index:
        title = h.loc[url, "title"] if "title" in h.columns else url
        row_h = h_scores.loc[url]
        row_s = s_scores.loc[url]
        print(f"\n  {str(title)[:70]}")
        for f in SCORE_FIELDS:
            print(f"    {f}: haiku={row_h[f]:.0f}  sonnet={row_s[f]:.0f}  Δ={row_s[f]-row_h[f]:+.0f}")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-sample", action="store_true",
                        help="Build sample_200.csv from ex_99_classified.csv")
    parser.add_argument("--compare", action="store_true",
                        help="Compare haiku vs sonnet outputs")
    args = parser.parse_args()

    if args.build_sample:
        build_sample()
    elif args.compare:
        run_compare()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
