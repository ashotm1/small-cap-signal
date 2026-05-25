import glob
import os
import re
import pandas as pd

IDX_DIR = "idx"
OUTPUT_DIR = "data"


def _parse_fixed(path):
    """Parse fixed-width IDX format."""
    rows = []
    with open(path, encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    # Data starts after the dashed separator line
    start = next(
        (i + 1 for i, line in enumerate(lines) if line.startswith("---")), 11
    )

    for line in lines[start:]:
        line = line.rstrip("\n")
        if not line.strip():
            continue
        parts = re.split(r"  +", line.strip())
        if len(parts) < 5:
            continue
        form, company, cik, date, filename = parts[0], parts[1], parts[2], parts[3], parts[4]
        rows.append({
            "CIK": cik,
            "Company Name": company,
            "Form Type": form,
            "Date Filed": date,
            "File Name": filename,
        })

    return pd.DataFrame(rows)


def parse_idx_file(path):
    """Parse a single IDX file. Returns DataFrame of 8-K rows only."""
    df = _parse_fixed(path)
    return df[df["Form Type"].str.strip() == "8-K"].copy()


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    output_csv = os.path.join(OUTPUT_DIR, "8k.csv")
    output_parquet = os.path.join(OUTPUT_DIR, "8k.parquet")

    if os.path.exists(output_csv):
        existing = pd.read_csv(output_csv, usecols=["idx_file"])
        processed = set(existing["idx_file"])
        print(f"{len(processed)} idx files already processed — skipping")
    else:
        processed = set()

    idx_files = sorted(glob.glob(os.path.join(IDX_DIR, "*.idx")))
    if not idx_files:
        print("No IDX files found in idx/")
        return

    write_header = not os.path.exists(output_csv)
    new_total = 0

    for path in idx_files:
        fname = os.path.basename(path)
        if fname in processed:
            continue
        df = parse_idx_file(path)
        df["idx_file"] = fname
        print(f"{fname} -> {len(df)} 8-K filings", flush=True)
        if not df.empty:
            df.to_csv(output_csv, mode="a", header=write_header, index=False)
            write_header = False
        new_total += len(df)

    if new_total == 0:
        print("Nothing new to process.")
    else:
        print(f"\n{new_total} new 8-K filings appended to {output_csv}")

    if not os.path.exists(output_csv):
        print("No data written — nothing to build parquet from.")
        return
    full = pd.read_csv(output_csv)
    full.to_parquet(output_parquet, index=False)
    print(f"Rebuilt {output_parquet} ({len(full)} total rows)")


if __name__ == "__main__":
    main()
