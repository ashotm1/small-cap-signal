"""
detect_endpoints.py — Intercept all network requests a site makes while loading
and identify any JSON API endpoints vs plain HTML.

Launches a real Chrome browser via Playwright, navigates to each target site,
and logs every request grouped by type (json, html, other).

Usage:
    python scraper/detect_endpoints.py
    python scraper/detect_endpoints.py --site stocktitan
"""

import argparse
import json
from collections import defaultdict
from playwright.sync_api import sync_playwright

SITES = {
    "globenewswire": "https://www.globenewswire.com/",
    "businesswire":  "https://www.businesswire.com/news/home/",
    "accesswire":    "https://www.accesswire.com/newsroom",
    "prnewswire":    "https://www.prnewswire.com/news-releases/news-releases-list/",
    "stocktitan":    "https://www.stocktitan.net/news/live.html",
}


def _classify(url: str, resource_type: str, accept: str) -> str:
    url_lower = url.lower()
    if resource_type in ("xhr", "fetch"):
        if "json" in accept or ".json" in url_lower or "/api/" in url_lower or "/graphql" in url_lower:
            return "json"
        return "xhr_other"
    if resource_type == "document":
        return "html"
    if resource_type in ("stylesheet", "image", "font", "media", "websocket", "ping"):
        return "noise"
    return "other"


def sniff_site(site: str, url: str) -> dict:
    requests_by_type = defaultdict(list)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        def on_request(request):
            accept = request.headers.get("accept", "")
            kind = _classify(request.url, request.resource_type, accept)
            if kind != "noise":
                requests_by_type[kind].append({
                    "url": request.url,
                    "method": request.method,
                    "type": request.resource_type,
                    "accept": accept,
                })

        page.on("request", on_request)

        print(f"\n[{site}] loading {url} ...")
        try:
            page.goto(url, timeout=20000)
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception as e:
            print(f"  warning: {e}")

        browser.close()

    return dict(requests_by_type)


def print_site_report(site: str, requests_by_type: dict):
    print("\n" + "=" * 60)
    print(f"RESULT: {site}")
    print("=" * 60)

    json_reqs = requests_by_type.get("json", [])
    xhr_other = requests_by_type.get("xhr_other", [])
    html_reqs = requests_by_type.get("html", [])

    if json_reqs:
        print(f"\n  JSON endpoints found ({len(json_reqs)}) — scrape these directly:")
        for r in json_reqs:
            print(f"    [{r['method']}] {r['url']}")
    else:
        print(f"\n  no JSON endpoints — data is in HTML")

    if xhr_other:
        print(f"\n  other XHR/fetch requests ({len(xhr_other)}) — may be worth inspecting:")
        for r in xhr_other:
            print(f"    [{r['method']}] {r['url']}")

    if html_reqs:
        print(f"\n  HTML documents loaded ({len(html_reqs)}):")
        for r in html_reqs:
            print(f"    {r['url']}")

    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", choices=list(SITES.keys()), help="check one site only")
    args = parser.parse_args()

    targets = {args.site: SITES[args.site]} if args.site else SITES

    for site, url in targets.items():
        requests_by_type = sniff_site(site, url)
        print_site_report(site, requests_by_type)


if __name__ == "__main__":
    main()
