"""
inspect_excerpts.py — Show exactly what text is fed to the LLM for each row in combined_disagreements.csv.
"""
import sys
import os

import asyncio
import re
import httpx
import pandas as pd
from bs4 import BeautifulSoup
from sec.edgar import fetch_html, HEADERS

INPUT_CSV = "data/combined_disagreements.csv"
BATCH_SIZE = 10
BATCH_INTERVAL = 1.0

skip_patterns = re.compile(r"^(EX-\d+\.\d+|Exhibit|\S+\.html?|\d+\.\d+|\d+)$", re.IGNORECASE)


def extract_excerpt(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    raw = " ".join(soup.stripped_strings)
    raw = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", raw)
    words = [w for w in raw.split() if w]
    while words and skip_patterns.match(words[0]):
        words.pop(0)
    return " ".join(words[:100])


async def main():
    df = pd.read_csv(INPUT_CSV)
    print(f"Inspecting {len(df)} rows...\n{'=' * 70}\n")

    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]

            htmls = await asyncio.gather(*[
                fetch_html(client, row["ex99_url"]) for _, row in batch.iterrows()
            ])

            for (_, row), html in zip(batch.iterrows(), htmls):
                print(f"Company : {row['company']}")
                print(f"URL     : {row['ex99_url']}")
                if html is None:
                    print("EXCERPT : [FAILED TO FETCH]")
                else:
                    print(f"EXCERPT : {extract_excerpt(html)}")
                print()

            if i + BATCH_SIZE < len(df):
                await asyncio.sleep(BATCH_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
