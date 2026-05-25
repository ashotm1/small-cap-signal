"""
stats.py — Print pipeline stats based on current CSV state.
"""
import os
import pandas as pd

DATA = "data"


def section(title):
    print(f"\n{'─' * 50}")
    print(f"  {title}")
    print(f"{'─' * 50}")


def load(filename, **kwargs):
    path = os.path.join(DATA, filename)
    if not os.path.exists(path):
        return None
    return pd.read_csv(path, **kwargs)


def main():
    # ── 8k.csv ────────────────────────────────────────
    df_8k = load("8k.csv")
    if df_8k is not None:
        section("8k.csv")
        print(f"  Total 8-K filings:  {len(df_8k)}")

    # ── 8k_ex99.csv ───────────────────────────────────
    df_ex99 = load("8k_ex99.csv")
    if df_ex99 is not None:
        section("8k_ex99.csv")
        has_ex99 = df_ex99["ex99_url"].notna() & (df_ex99["ex99_url"] != "")
        print(f"  Total rows:         {len(df_ex99)}")
        print(f"  Has EX-99:          {has_ex99.sum()}")
        print(f"  No EX-99:           {(~has_ex99).sum()}")

    # ── ex_99_classified.csv ──────────────────────────────────
    df_ex = load("ex_99_classified.csv")
    if df_ex is not None:
        section("ex_99_classified.csv — classified exhibits")
        total = len(df_ex)
        prs = df_ex["is_pr"].sum() if "is_pr" in df_ex.columns else 0
        print(f"  Total classified:   {total}")
        print(f"  Is PR:              {prs}  ({prs/total*100:.1f}%)")
        print(f"  Not PR:             {total - prs}  ({(total-prs)/total*100:.1f}%)")

        df_prs = df_ex[df_ex["is_pr"] == True] if "is_pr" in df_ex.columns else df_ex
        if "heuristic" in df_prs.columns:
            print("\n  By heuristic label:")
            for label, count in df_prs["heuristic"].value_counts().items():
                pct = count / len(df_prs) * 100
                print(f"    {label:<12} {count:>5}  ({pct:.1f}%)")

        heuristics = [h for h in ["H1", "H2", "H3", "H4", "H5", "H6"] if h in df_prs.columns]
        if heuristics:
            print("\n  Heuristic fire rates (PRs only):")
            for h in heuristics:
                fired = df_prs[h].sum()
                pct = fired / len(df_prs) * 100
                print(f"    {h}  fired {fired:>5}/{len(df_prs)}  ({pct:.1f}%)")

    # ── price_data.csv ────────────────────────────────
    df_prices = load("price_data.csv")
    if df_prices is not None:
        section("price_data.csv")
        print(f"  Total rows:         {len(df_prices)}")
        has_price = df_prices["price_t0"].notna()
        print(f"  Has price data:     {has_price.sum()}")
        print(f"  No price data:      {(~has_price).sum()}")

        for col in ["change_5m_pct", "change_30m_pct", "change_1h_pct", "change_4h_pct", "change_1d_pct"]:
            if col in df_prices.columns:
                s = df_prices[col].dropna()
                if len(s):
                    print(f"\n  {col}:")
                    print(f"    mean={s.mean():.3f}%  median={s.median():.3f}%  std={s.std():.3f}%")


if __name__ == "__main__":
    main()
