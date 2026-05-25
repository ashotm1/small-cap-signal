"""
inspect_request.py — Print the exact HTTP request sent and response received.

Shows everything at the HTTP application layer: request line, headers, body,
response status, response headers, and response body.

Usage:
    python scraper/inspect_request.py https://www.stocktitan.net/news/live.html
    python scraper/inspect_request.py https://www.stocktitan.net/news/live.html --method curl_cffi
    python scraper/inspect_request.py https://www.stocktitan.net/news/live.html --body-limit 500
"""

import argparse
import json

import requests as _requests

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def print_request(req, body_limit: int):
    print("\n" + "=" * 60)
    print("REQUEST SENT")
    print("=" * 60)
    print(f"{req.method} {req.url}")
    print()
    print("Headers:")
    for k, v in req.headers.items():
        print(f"  {k}: {v}")
    if req.body:
        body = req.body if isinstance(req.body, str) else req.body.decode("utf-8", errors="replace")
        print(f"\nBody ({len(body)} bytes):")
        print(f"  {body[:body_limit]}{'...' if len(body) > body_limit else ''}")
    else:
        print("\nBody: (none)")


def print_response(resp, body_limit: int):
    print("\n" + "=" * 60)
    print("RESPONSE RECEIVED")
    print("=" * 60)
    print(f"Status: {resp.status_code} {resp.reason if hasattr(resp, 'reason') else ''}")
    print()
    print("Headers:")
    for k, v in resp.headers.items():
        print(f"  {k}: {v}")

    body = resp.text
    print(f"\nBody ({len(body)} chars):")

    # try pretty-print if JSON
    try:
        parsed = json.loads(body)
        pretty = json.dumps(parsed, indent=2)
        print(pretty[:body_limit])
        if len(pretty) > body_limit:
            print(f"... ({len(pretty) - body_limit} more chars)")
    except Exception:
        print(body[:body_limit])
        if len(body) > body_limit:
            print(f"... ({len(body) - body_limit} more chars)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="URL to request")
    parser.add_argument("--method", choices=["requests", "curl_cffi"], default="requests",
                        help="fetch method to use (default: requests)")
    parser.add_argument("--body-limit", type=int, default=2000,
                        help="max chars to print for request/response body (default: 2000)")
    args = parser.parse_args()

    if args.method == "curl_cffi":
        if not HAS_CURL_CFFI:
            print("curl_cffi not installed — pip install curl_cffi")
            return
        resp = curl_requests.get(args.url, headers=HEADERS, impersonate="chrome124", timeout=15)
    else:
        resp = _requests.get(args.url, headers=HEADERS, timeout=15)

    print_request(resp.request, args.body_limit)
    print_response(resp, args.body_limit)


if __name__ == "__main__":
    main()
