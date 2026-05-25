"""
pipeline.py — Run the full SEC EDGAR pipeline (secondary source) from index
download to PR classification. Steps run as `python -m <module>` from repo root.

Steps (in order):
  1. sec.download_idx          — download daily index files from EDGAR
  2. sec.parse_idx            — parse index files → data/8k.csv
  3. sec.batch_filter         — fetch filing index pages → data/8k_ex99.csv
  4. sec.classify_exhibits    — classify EX-99 exhibits → data/ex_99_classified.csv
  5. sec.classify_catalyst_llm — (optional --llm) LLM catalyst classify for 'other' rows
  6. market.fetch_market_data — (optional --market) fetch Polygon market data for signal rows

Each step is append-safe and skips already-processed rows.
Per-category feature extraction (features.runner) is run separately.

Usage:
  python pipeline.py --date-from 2022-01-01 --date-to 2025-12-31
  python pipeline.py --days 30 --llm --market
"""
import argparse
import subprocess
import sys


def run(cmd: list[str], label: str):
    print(f"\n{'='*60}", flush=True)
    print(f"  {label}", flush=True)
    print(f"{'='*60}", flush=True)
    result = subprocess.run([sys.executable] + cmd)
    if result.returncode != 0:
        print(f"\nPipeline failed at: {label} (exit code {result.returncode})")
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date-from", metavar="YYYY-MM-DD",
                       help="Start date for index download")
    group.add_argument("--days", type=int,
                       help="Download last N days of index files")
    parser.add_argument("--date-to", metavar="YYYY-MM-DD", default=None,
                        help="End date for index download (default: today)")
    parser.add_argument("--llm", action="store_true",
                        help="Run classify_catalyst_llm after exhibits (submits batch, polls, collects)")
    parser.add_argument("--market", action="store_true",
                        help="Run fetch_market_data after classification")
    args = parser.parse_args()

    # Step 1 — download index files
    dl_args = ["-m", "sec.download_idx"]
    if args.days:
        dl_args += ["--days", str(args.days)]
    else:
        dl_args += ["--date-from", args.date_from]
        if args.date_to:
            dl_args += ["--date-to", args.date_to]
    run(dl_args, "Step 1: sec.download_idx")

    # Steps 2-4 — no args needed, each reads from previous step's output
    run(["-m", "sec.parse_idx"],        "Step 2: sec.parse_idx")
    run(["-m", "sec.batch_filter"],     "Step 3: sec.batch_filter")
    run(["-m", "sec.classify_exhibits"], "Step 4: sec.classify_exhibits")

    if args.llm:
        run(["-m", "sec.classify_catalyst_llm", "--run"], "Step 5: sec.classify_catalyst_llm")

    if args.market:
        run(["-m", "market.fetch_market_data"], "Step 6: market.fetch_market_data")

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
