"""
edgar.py — SEC EDGAR fetching utilities.
Provides functions to fetch EX-99.x exhibit URLs from filing index pages,
and to resolve CIK → ticker with a persistent disk cache.
"""
import json
import os
import re
from bs4 import BeautifulSoup

_SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT")
if not _SEC_USER_AGENT:
    raise EnvironmentError(
        "SEC_USER_AGENT env var is not set. "
        "Set it to 'Your Name your@email.com' as required by SEC EDGAR fair-access policy."
    )
HEADERS = {"User-Agent": _SEC_USER_AGENT}
CIK_CACHE_FILE = "data/cik_tickers.json"
SEC_BASE = "https://www.sec.gov"
SEC_ARCHIVES = "https://www.sec.gov/Archives/"

_EX99_TYPE = re.compile(r"^EX-99\.\d+$")
_ITEM_NUM = re.compile(r"Item\s+(\d+\.\d+)")


def parse_index(index_html):
    """
    Parse an EDGAR filing index page HTML.
    Returns a dict:
        ex99_urls     — list of absolute EX-99.x exhibit URLs
        acceptance_dt — acceptance datetime string e.g. "2026-03-27 17:25:29", or None
        items         — list of 8-K item numbers e.g. ["7.01", "8.01"]
    """
    soup = BeautifulSoup(index_html, "html.parser")

    # --- EX-99 URLs ---
    ex99_urls = []
    table = soup.find("table", {"summary": "Document Format Files"})
    if not table:
        header = next(
            (tag for tag in soup.find_all(["p", "h2", "h3"])
             if "Document Format Files" in tag.get_text()),
            None,
        )
        if header:
            table = header.find_next("table")

    if table:
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            doc_type = cells[3].get_text(strip=True)
            if not _EX99_TYPE.match(doc_type):
                continue
            a_tag = cells[2].find("a")
            if not a_tag:
                continue
            href = a_tag.get("href", "")
            if href.startswith("/ix?doc="):
                href = href[len("/ix?doc="):]
            if not href.startswith("http"):
                href = SEC_BASE + href
            ex99_urls.append(href)

    # --- Acceptance datetime and items ---
    acceptance_dt = None
    items = []
    for tag in soup.find_all("div", class_="infoHead"):
        label = tag.get_text(strip=True)
        info = tag.find_next_sibling("div", class_="info")
        if not info:
            continue
        if label == "Accepted":
            acceptance_dt = info.get_text(strip=True)
        elif label == "Items":
            items = _ITEM_NUM.findall(info.get_text(separator="\n"))

    return {"ex99_urls": ex99_urls, "acceptance_dt": acceptance_dt, "items": items}


async def fetch_index(client, index_url):
    """
    Fetch the EDGAR filing index page and return parsed index data.
    `client` is an httpx.AsyncClient instance.
    """
    try:
        r = await client.get(index_url, headers=HEADERS)
    except Exception:
        return {"ex99_urls": [], "acceptance_dt": None, "items": []}

    if r.status_code != 200:
        return {"ex99_urls": [], "acceptance_dt": None, "items": []}

    return parse_index(r.text)


def load_cik_cache() -> dict:
    """Load CIK → ticker cache from disk. Returns empty dict if not found."""
    if os.path.exists(CIK_CACHE_FILE):
        with open(CIK_CACHE_FILE) as f:
            return json.load(f)
    return {}


def save_cik_cache(cache: dict):
    """Persist CIK → ticker cache to disk."""
    with open(CIK_CACHE_FILE, "w") as f:
        json.dump(cache, f)


async def fetch_ticker(client, cik: int) -> str | None:
    """Resolve primary exchange ticker for a CIK via SEC submissions API."""
    url = f"https://data.sec.gov/submissions/CIK{cik:010d}.json"
    try:
        r = await client.get(url, headers=HEADERS)
        if r.status_code != 200:
            return None
        tickers = r.json().get("tickers", [])
        return tickers[0] if tickers else None
    except Exception:
        return None


async def fetch_html(client, url):
    """
    Fetch a URL and return the response text, or None on failure.
    `client` is an httpx.AsyncClient instance.
    """
    try:
        r = await client.get(url, headers=HEADERS)
    except Exception:
        return None

    return r.text if r.status_code == 200 else None
