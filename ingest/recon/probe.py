"""
probe.py — Rate probe each newswire target site.

Phase 1 — Method detection:
    Tries plain requests → curl_cffi → flags as needing Playwright.
    Stops at whichever method gets a clean response.

Phase 2 — Rate probing:
    Sends requests at a fixed delay for a full duration window (default 60s).
    If the full window passes clean, steps down to the next delay level.
    If any request in the window is blocked, stops immediately and records the wall.
    This mirrors how real rate limiters work (sliding window counters).

Usage:
    python scraper/probe.py
    python scraper/probe.py --site globenewswire
    python scraper/probe.py --site globenewswire --duration 30
    python scraper/probe.py --duration 60 --delays 4 3 2 1.5 1 0.5
    python scraper/probe.py --site stocktitan --method playwright --delays 2 1.5 1
"""

import argparse
import time
from dataclasses import dataclass, field

import requests as _requests

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

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

PROBE_URLS = {
    "globenewswire": "https://www.globenewswire.com/",
    "businesswire":  "https://www.businesswire.com/news/home/",
    "accesswire":    "https://www.accesswire.com/newsroom",
    "prnewswire":    "https://www.prnewswire.com/news-releases/news-releases-list/",
    "stocktitan":    "https://www.stocktitan.net/news/live.html",
}

DEFAULT_DELAYS = [4.0, 3.0, 2.0, 1.5, 1.0, 0.5]


@dataclass
class LevelResult:
    delay: float
    requests_sent: int
    duration_s: float
    blocked: bool
    block_status: int | None
    block_round: int | None


@dataclass
class ProbeResult:
    site: str
    method: str                        # requests / curl_cffi / playwright
    phase1_blocked: bool               # True if all methods failed in detection
    phase2_levels: list[LevelResult] = field(default_factory=list)

    @property
    def wall_delay(self) -> float | None:
        """The delay level at which we got blocked. None if never blocked."""
        for lvl in self.phase2_levels:
            if lvl.blocked:
                return lvl.delay
        return None

    @property
    def safe_delay(self) -> float | None:
        """Last delay level that completed cleanly."""
        clean = [lvl for lvl in self.phase2_levels if not lvl.blocked]
        return clean[-1].delay if clean else None


def _is_blocked(status: int, text: str) -> bool:
    if status in (429, 403, 503):
        return True
    text = text.lower()
    return any(k in text for k in (
        "captcha", "just a moment", "checking your browser",
        "access denied", "robot check",
    ))


def fetch_requests(url: str):
    resp = _requests.get(url, headers=HEADERS, timeout=15)
    return resp.status_code, resp.text, dict(resp.headers)


def fetch_curl(url: str):
    resp = curl_requests.get(url, headers=HEADERS, impersonate="chrome124", timeout=15)
    return resp.status_code, resp.text, dict(resp.headers)


_pw_context = None  # reused across calls within a probe run

def fetch_playwright(url: str):
    global _pw_context
    if _pw_context is None:
        raise RuntimeError("Playwright context not initialised — call init_playwright() first")
    page = _pw_context.new_page()
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=20_000)
        status = resp.status if resp else 0
        text = page.content()
        return status, text, {}
    finally:
        page.close()


def init_playwright() -> "contextmanager":
    """Start a stealth Playwright browser; returns the playwright instance to stop later."""
    global _pw_context
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    _pw_context = browser.new_context(
        user_agent=HEADERS["User-Agent"],
        locale="en-US",
        timezone_id="America/New_York",
        viewport={"width": 1280, "height": 800},
        extra_http_headers={
            "Accept-Language": HEADERS["Accept-Language"],
            "Accept-Encoding": HEADERS["Accept-Encoding"],
            "DNT": HEADERS["DNT"],
        },
    )
    # patch navigator.webdriver
    _pw_context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return pw, browser


def detect_method(site: str, url: str) -> tuple[str, bool]:
    """
    Returns (method, phase1_blocked).
    phase1_blocked=True means even Playwright is unconfirmed — all http methods failed.
    """
    print(f"\n[{site}] phase 1 — method detection")

    try:
        status, text, _ = fetch_requests(url)
        if not _is_blocked(status, text):
            print(f"  → requests works (status {status})")
            return "requests", False
        print(f"  → requests blocked (status {status})")
    except Exception as e:
        print(f"  → requests error: {e}")

    if HAS_CURL_CFFI:
        try:
            status, text, _ = fetch_curl(url)
            if not _is_blocked(status, text):
                print(f"  → curl_cffi works (status {status})")
                return "curl_cffi", False
            print(f"  → curl_cffi blocked (status {status})")
        except Exception as e:
            print(f"  → curl_cffi error: {e}")
    else:
        print("  → curl_cffi not installed, skipping (pip install curl_cffi)")

    if HAS_PLAYWRIGHT:
        try:
            pw, browser = init_playwright()
            status, text, _ = fetch_playwright(url)
            if not _is_blocked(status, text):
                print(f"  → playwright works (status {status})")
                return "playwright", False
            print(f"  → playwright blocked (status {status})")
            browser.close()
            pw.stop()
        except Exception as e:
            print(f"  → playwright error: {e}")
    else:
        print("  → playwright not installed, skipping (pip install playwright && playwright install chromium)")

    print("  → all methods failed")
    return "playwright", True


def probe_level(url: str, method: str, delay: float, duration: float) -> LevelResult:
    """
    Send requests at fixed delay for `duration` seconds.
    Stops immediately on first block.
    """
    if method == "curl_cffi":
        fetch = fetch_curl
    elif method == "playwright":
        fetch = fetch_playwright
    else:
        fetch = fetch_requests
    start = time.monotonic()
    sent = 0

    while True:
        elapsed = time.monotonic() - start
        if elapsed >= duration:
            break

        try:
            status, text, _ = fetch(url)
            sent += 1
            elapsed_now = time.monotonic() - start

            if _is_blocked(status, text):
                print(f"    req {sent:03d}  t={elapsed_now:.1f}s  status={status}  BLOCKED → stopping")
                return LevelResult(
                    delay=delay, requests_sent=sent,
                    duration_s=round(elapsed_now, 1),
                    blocked=True, block_status=status, block_round=sent,
                )

            print(f"    req {sent:03d}  t={elapsed_now:.1f}s  status={status}  ok")

        except Exception as e:
            sent += 1
            elapsed_now = time.monotonic() - start
            print(f"    req {sent:03d}  t={elapsed_now:.1f}s  error: {e}")

        time.sleep(delay)

    total = round(time.monotonic() - start, 1)
    print(f"  → level complete: {sent} requests over {total}s, no block")
    return LevelResult(
        delay=delay, requests_sent=sent,
        duration_s=total, blocked=False,
        block_status=None, block_round=None,
    )


def probe_site(site: str, url: str, duration: float, delays: list[float], force_method: str | None = None) -> ProbeResult:
    if force_method:
        print(f"\n[{site}] skipping phase 1 — method forced to {force_method}")
        method, phase1_blocked = force_method, False
    else:
        method, phase1_blocked = detect_method(site, url)

    if phase1_blocked:
        return ProbeResult(site=site, method=method, phase1_blocked=phase1_blocked)

    # for playwright forced via --method, init browser now (detect_method already inits it on auto-detect)
    pw_handle = None
    if method == "playwright" and _pw_context is None:
        if not HAS_PLAYWRIGHT:
            print("  playwright not installed — pip install playwright && playwright install chromium")
            return ProbeResult(site=site, method=method, phase1_blocked=True)
        pw_handle = init_playwright()

    print(f"\n[{site}] phase 2 — rate probing ({duration}s per level, delays: {delays})")

    levels = []
    try:
        for delay in delays:
            print(f"\n  testing delay={delay}s ...")
            level = probe_level(url, method, delay, duration)
            levels.append(level)
            if level.blocked:
                break
    finally:
        if pw_handle:
            pw_handle[1].close()
            pw_handle[0].stop()

    return ProbeResult(site=site, method=method, phase1_blocked=False, phase2_levels=levels)


def print_site_summary(r: ProbeResult):
    print("\n" + "=" * 60)
    print(f"RESULT: {r.site}")
    print("=" * 60)
    print(f"  phase 1 method:  {r.method}")

    if r.phase1_blocked:
        print(f"  phase 1 result:  BLOCKED — all methods failed, needs Playwright or site is down")
        print()
        return

    if r.phase1_blocked and r.method == "playwright":
        print(f"  phase 2:         skipped — all methods failed, Playwright may help manually")
        print()
        return

    if not r.phase2_levels:
        print(f"  phase 2:         no levels run")
        print()
        return

    print(f"  phase 2 levels:")
    for lvl in r.phase2_levels:
        if lvl.blocked:
            detail = f"BLOCKED at req {lvl.block_round} after {lvl.duration_s}s (status {lvl.block_status})"
        else:
            detail = f"clean — {lvl.requests_sent} reqs over {lvl.duration_s}s"
        print(f"    delay={lvl.delay}s  →  {detail}")

    if r.wall_delay is not None:
        print(f"\n  conclusion:  wall hit at {r.wall_delay}s delay")
        safe = r.safe_delay
        if safe:
            print(f"               safe floor is {safe}s (last clean level)")
        else:
            print(f"               blocked on first level tested — safe floor unknown, start slower")
    else:
        print(f"\n  conclusion:  no rate limit detected at any tested delay")
        print(f"               lowest tested: {r.phase2_levels[-1].delay}s")

    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", choices=list(PROBE_URLS.keys()), help="probe one site only")
    parser.add_argument("--duration", type=float, default=60.0,
                        help="seconds to sustain each delay level (default: 60)")
    parser.add_argument("--delays", type=float, nargs="+", default=DEFAULT_DELAYS,
                        help="delay levels to test in seconds, e.g. --delays 4 3 2 1.5 1")
    parser.add_argument("--method", choices=["requests", "curl_cffi", "playwright"], default=None,
                        help="force a specific fetch method, skipping phase 1 detection")
    args = parser.parse_args()

    targets = {args.site: PROBE_URLS[args.site]} if args.site else PROBE_URLS

    for site, url in targets.items():
        result = probe_site(site, url, duration=args.duration, delays=args.delays, force_method=args.method)
        print_site_summary(result)


if __name__ == "__main__":
    main()
