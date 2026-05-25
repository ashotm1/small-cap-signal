"""
cleanup_classified.py — Post-processing cleanups for ex_99_classified.csv.
Run after classify_exhibits.py to fix known title extraction issues.
Each cleanup is a named, targeted function applied to a specific subset of rows.
"""
import ast
import re
import pandas as pd

INPUT_CSV  = "data/ex_99_classified.csv"
OUTPUT_CSV = "data/ex_99_classified.csv"

# ── Shared patterns (mirrors pr_detection.py) ────────────────────────────────

_SKIP_TOKENS = re.compile(r"^(EX-\d+\.\d+|Exhibit|\S+\.html?|\d+\.\d+|\d+)$", re.IGNORECASE)
_JUNK_LEAD   = re.compile(
    r"^(?:-\w+-|[a-z0-9]+(?:[_-][a-z0-9]+){1,}|[a-z]+\d+[a-z][a-z0-9]*)$",
    re.IGNORECASE,
)

def _strip_slug(title: str) -> str:
    """Strip leading EDGAR filename slug(s) and skip-tokens from a title string."""
    if not title:
        return title
    words = title.split()
    while words and (_JUNK_LEAD.match(words[0]) or _SKIP_TOKENS.match(words[0])):
        words = words[1:]
    return " ".join(words).strip()


# ── Catalyst helpers ──────────────────────────────────────────────────────────

def _has_catalyst(val, cat):
    try:
        tags = ast.literal_eval(str(val))
    except Exception:
        tags = [str(val)]
    return cat in tags


# ── Cleanups ──────────────────────────────────────────────────────────────────

def fix_pr_slug_prefixes(df: pd.DataFrame) -> pd.DataFrame:
    """Strip EDGAR filename slug prefixes from titles on all PR rows."""
    mask = df["is_pr"].astype(str).str.lower() == "true"

    before = df.loc[mask, "title"].copy()
    df.loc[mask, "title"] = df.loc[mask, "title"].fillna("").apply(_strip_slug)
    df.loc[mask, "title"] = df.loc[mask, "title"].replace("", None)

    changed = (before.fillna("") != df.loc[mask, "title"].fillna("")).sum()
    print(f"fix_pr_slug_prefixes: {changed} titles updated ({mask.sum()} PR rows checked)")
    return df


# ── Main ──────────────────────────────────────────────────────────────────────

CLEANUPS = [
    fix_pr_slug_prefixes,
]

def main():
    df = pd.read_csv(INPUT_CSV)
    print(f"Loaded {len(df)} rows from {INPUT_CSV}")

    for fn in CLEANUPS:
        df = fn(df)

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {len(df)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
