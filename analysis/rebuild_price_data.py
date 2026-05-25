"""
rebuild_price_data.py — Rebuild price_data.csv from existing bars without any API calls.

Reads backup_data.csv (previous price_data snapshot) + price_bars.csv,
recomputes price change columns using current compute_changes logic, writes price_data.csv.
"""
import pandas as pd
from market.fetch_market_data import compute_changes, _OFFSETS_MS

BACKUP_CSV = "data/backup_data.csv"
BARS_CSV   = "data/prices/price_bars.csv"
DAILY_CSV  = "data/prices/daily_bars.csv"
OUTPUT_CSV = "data/prices/price_data.csv"


def main():
    backup = pd.read_csv(BACKUP_CSV, low_memory=False)
    print(f"Loaded {len(backup)} rows from backup")

    print(f"Loading bars from {BARS_CSV}...")
    bars_df = pd.read_csv(BARS_CSV, on_bad_lines="skip")
    bars_map: dict = {}
    for key, grp in bars_df.groupby(["ticker", "date_str"]):
        bars_map[key] = grp[["t", "o", "h", "l", "c", "v"]].to_dict("records")
    print(f"  {len(bars_map)} (ticker, date_str) pairs in bars")

    print(f"Loading daily bars from {DAILY_CSV}...")
    daily_df = pd.read_csv(DAILY_CSV, on_bad_lines="skip")
    daily_map: dict = {}
    for key, grp in daily_df.groupby(["ticker", "date_str"]):
        daily_map[key] = grp[["t", "o", "h", "l", "c", "v"]].sort_values("t").to_dict("records")
    print(f"  {len(daily_map)} (ticker, date_str) pairs in daily")

    price_cols = ["price_t0"] + [f"change_{l}_pct" for l in _OFFSETS_MS]
    updated = 0

    for idx, row in backup.iterrows():
        ticker   = row.get("ticker")
        date_str = row.get("date_str")
        if pd.isna(ticker) or not ticker:
            continue
        bars = bars_map.get((ticker, date_str), [])
        if not bars:
            continue
        daily = daily_map.get((ticker, date_str), [])
        changes = compute_changes(bars, row.get("acceptance_dt"), daily=daily or None)
        for col in price_cols:
            backup.at[idx, col] = changes.get(col)
        updated += 1

    backup.to_csv(OUTPUT_CSV, index=False)
    print(f"\nWrote {len(backup)} rows to {OUTPUT_CSV}")
    print(f"  Updated price cols: {updated}")
    print(f"  With price_t0:      {backup['price_t0'].notna().sum()}")


if __name__ == "__main__":
    main()
