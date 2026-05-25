"""
clean_corrupt_bars.py — Remove corrupt (ticker, date_str) pairs from all price CSVs.

A pair is corrupt if any of its bars has t < 1e12 (not a valid 2024+ epoch ms timestamp).
Removes all rows for those pairs from price_bars.csv, daily_bars.csv,
ticker_details.csv, and price_data.csv so fetch_market_data.py re-fetches them cleanly.
"""
import pandas as pd

BARS_CSV    = "data/prices/price_bars.csv"
DAILY_CSV   = "data/prices/daily_bars.csv"
DETAILS_CSV = "data/prices/ticker_details.csv"
PRICE_CSV   = "data/prices/price_data.csv"


def main():
    bars = pd.read_csv(BARS_CSV, on_bad_lines="skip", low_memory=False)
    print(f"price_bars.csv: {len(bars)} rows")

    # Find pairs with any corrupt timestamp
    corrupt_mask = bars["t"] < 1_700_000_000_000
    corrupt_pairs = set(zip(bars.loc[corrupt_mask, "ticker"], bars.loc[corrupt_mask, "date_str"]))
    print(f"Corrupt (ticker, date_str) pairs: {len(corrupt_pairs)}")
    for p in sorted(corrupt_pairs):
        print(f"  {p}")

    if not corrupt_pairs:
        print("Nothing to clean.")
        return

    def drop_pairs(df, label):
        mask = [
            (row["ticker"], row["date_str"]) in corrupt_pairs
            for _, row in df.iterrows()
        ]
        dropped = sum(mask)
        clean = df[[not m for m in mask]]
        print(f"{label}: dropped {dropped} rows, kept {len(clean)}")
        return clean

    # Clean each file
    bars_clean = drop_pairs(bars, "price_bars.csv")
    bars_clean.to_csv(BARS_CSV, index=False)

    daily = pd.read_csv(DAILY_CSV, on_bad_lines="skip")
    drop_pairs(daily, "daily_bars.csv").to_csv(DAILY_CSV, index=False)

    details = pd.read_csv(DETAILS_CSV)
    drop_pairs(details, "ticker_details.csv").to_csv(DETAILS_CSV, index=False)

    import csv
    # price_data.csv has mixed schema — use csv module
    with open(PRICE_CSV, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    kept = [r for r in rows if (r.get("ticker"), r.get("date_str")) not in corrupt_pairs]
    dropped = len(rows) - len(kept)
    print(f"price_data.csv: dropped {dropped} rows, kept {len(kept)}")

    with open(PRICE_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept)

    print("\nDone. Run fetch_market_data.py to re-fetch the corrupt pairs.")


if __name__ == "__main__":
    main()
