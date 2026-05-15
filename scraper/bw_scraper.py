"""
bw_scraper.py — Scrape BusinessWire newsroom via CDP-attached real browser,
with N tabs running concurrently in one Playwright async event loop.

BW is protected by Akamai Bot Manager. Headless / non-browser clients fail
its JS challenge. This scraper sidesteps that by attaching to a real Chrome
instance that you've already started — Akamai sees your real browser, not a
bot.

SETUP:
  - First-time only: the script auto-launches Chrome with the right flags
    when no CDP listener is found on --debug-port. It uses an isolated
    profile (default C:\bw-chrome-profile). On the first launch you may
    need to visit https://www.businesswire.com/newsroom?language=en&page=1
    once and let Akamai's challenge auto-solve; cookies persist in the
    profile, so subsequent runs reconnect without intervention.
  - Override with --chrome-exe / --chrome-profile / --debug-port if needed.
  - If you'd rather launch Chrome yourself, just do so before running the
    script — auto-launch is skipped when the port is already open.

Fields: date, time, datetime, ticker, exchange, source, title, url

Usage:
    python scraper/bw_scraper.py --probe                    # inspect page 1 structure
    python scraper/bw_scraper.py                            # default --parallelism 2
    python scraper/bw_scraper.py --parallelism 4
    python scraper/bw_scraper.py --from-page 200 --to-page 300

dup_stop semantics: with parallel workers, "consecutive all-duplicate pages"
becomes "consecutive page-completions with new=0 in finish order, reset by
any page-completion with new>0." Identical to serial behavior at --parallelism 1.
"""

import argparse
import asyncio
import csv
import math
import os
import random
import re
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

OUTPUT_CSV    = "data/bw_news.csv"
RUNS_CSV      = "data/bw_runs.csv"
LOG_DIR       = "logs"
BASE_URL      = "https://www.businesswire.com"

CSV_FIELDS  = ["datetime", "ticker", "exchange", "title", "url"]
RUNS_FIELDS = ["started_at", "from_page", "to_page", "total_pages", "duration"]

# from gnw_scraper.py — same exchange-ticker pattern
_TICKER_RE = re.compile(
    r"\(?(?P<exchange>NYSE American|NYSE Arca|NASDAQ GSM|NASDAQ CM|NASDAQ|NYSE|OTCQB|OTCQX)"
    r"(?:/(?:NYSE|NASDAQ|LSE))?[:\s]+(?P<ticker>[A-Z]{1,6})[;,\s)]*",
    re.IGNORECASE,
)

# BW date format: "May 11, 2026 at 12:17 AM ET"
_BW_DATETIME = re.compile(
    r"([A-Z][a-z]+ \d{1,2}, \d{4})\s+at\s+(\d{1,2}):(\d{2})\s*(AM|PM)\s*ET",
    re.IGNORECASE,
)


def parse_ticker(text: str) -> tuple:
    m = _TICKER_RE.search(text or "")
    if m:
        return m.group("ticker").upper(), m.group("exchange").upper()
    return "", ""


def parse_bw_datetime(text: str) -> str:
    """'May 11, 2026 at 12:17 AM ET' or 'Apr 16, 2026 at 8:30 AM ET' → '2026-05-11 00:17'."""
    if not text:
        return ""
    m = _BW_DATETIME.search(text)
    if not m:
        return ""
    for fmt in ("%B %d, %Y", "%b %d, %Y"):  # full month, then abbreviated
        try:
            d = datetime.strptime(m.group(1), fmt).strftime("%Y-%m-%d")
            break
        except ValueError:
            continue
    else:
        return ""
    hh = int(m.group(2)) % 12
    if m.group(4).upper() == "PM":
        hh += 12
    return f"{d} {hh:02d}:{m.group(3)}"


def parse_page(html: str) -> list:
    """Extract article rows from a BW newsroom page."""
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen = set()

    # Anchor: <a class="font-figtree" href="/news/home/...">
    for a in soup.select('a.font-figtree[href*="/news/home/"]'):
        url = a.get("href", "")
        if not url:
            continue
        if not url.startswith("http"):
            url = BASE_URL + url
        if url in seen:
            continue
        seen.add(url)

        # Title is the <h2> text inside the anchor
        h2 = a.find("h2")
        title = h2.get_text(strip=True) if h2 else a.get_text(strip=True)
        if not title:
            continue

        # Article row is the nearest <div> with class 'border-gray300'
        row = a.find_parent("div", class_="border-gray300")

        dt = ""
        ticker = exchange = ""
        if row:
            for span in row.find_all("span"):
                m = parse_bw_datetime(span.get_text(strip=True))
                if m:
                    dt = m
                    break
            preview = row.select_one(".rich-text")
            if preview:
                ticker, exchange = parse_ticker(preview.get_text(" ", strip=True))

        items.append({
            "datetime": dt,
            "ticker":   ticker,
            "exchange": exchange,
            "title":    title,
            "url":      url,
        })
    return items


# ---------------------------------------------------------------------------
# CSV I/O
# ---------------------------------------------------------------------------

def load_existing_rows() -> dict:
    """url → row dict from existing CSV."""
    rows: dict = {}
    if not os.path.exists(OUTPUT_CSV):
        return rows
    with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows[row["url"]] = row
    return rows


def write_all(rows: dict):
    """Atomic full rewrite via tmp + rename."""
    tmp = OUTPUT_CSV + ".tmp"
    with open(tmp, "w", newline="\n", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for row in rows.values():
            w.writerow({k: row.get(k, "") for k in CSV_FIELDS})
    os.replace(tmp, OUTPUT_CSV)


def load_runs() -> list:
    if not os.path.exists(RUNS_CSV):
        return []
    with open(RUNS_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_runs(runs: list):
    tmp = RUNS_CSV + ".tmp"
    with open(tmp, "w", newline="\n", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RUNS_FIELDS)
        w.writeheader()
        for r in runs:
            w.writerow({k: r.get(k, "") for k in RUNS_FIELDS})
    os.replace(tmp, RUNS_CSV)


def fmt_duration(seconds: float) -> str:
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}"


def _cdp_port_open(port: int, timeout: float = 0.5) -> bool:
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect(("127.0.0.1", port))
        s.close()
        return True
    except OSError:
        return False


def _find_chrome_exe() -> str | None:
    candidates = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return None


def ensure_chrome(debug_port: int, profile_dir: str, chrome_exe: str | None,
                  wait_secs: float = 15.0):
    """Connect to existing CDP if up; otherwise launch Chrome detached with the
    given debug port + profile dir and wait until the port is reachable."""
    if _cdp_port_open(debug_port):
        return
    exe = chrome_exe or _find_chrome_exe()
    if not exe:
        raise FileNotFoundError(
            "Chrome executable not found in standard locations. "
            "Pass --chrome-exe to override."
        )
    args = [
        exe,
        f"--remote-debugging-port={debug_port}",
        f"--user-data-dir={profile_dir}",
    ]
    print(f"  launching Chrome: {exe}")
    print(f"  profile: {profile_dir}   debug port: {debug_port}")
    flags = 0
    if sys.platform == "win32":
        flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    subprocess.Popen(args, creationflags=flags, close_fds=True)
    deadline = time.time() + wait_secs
    while time.time() < deadline:
        time.sleep(0.5)
        if _cdp_port_open(debug_port):
            return
    raise TimeoutError(f"Chrome didn't expose CDP on port {debug_port} within {wait_secs}s")


class _Tee:
    """Writes to multiple streams. Lets every existing `print` also hit a log file."""
    def __init__(self, *streams):
        self._streams = streams
    def write(self, s):
        for st in self._streams:
            st.write(s)
    def flush(self):
        for st in self._streams:
            st.flush()


# ---------------------------------------------------------------------------
# Async human-behavior + navigation helpers
# ---------------------------------------------------------------------------

_last_mouse_pos = [400, 400]


async def human_move(page, x: int, y: int, duration_ms: int | None = None):
    """Move mouse to (x, y) over duration_ms with human-like pacing.

    Real mouse moves take 200–500ms; default Playwright `.move(x, y, steps=N)`
    fires events instantly, which Akamai's behavioral model can spot.
    This interpolates with explicit sleeps between sub-moves.
    """
    if duration_ms is None:
        duration_ms = random.randint(200, 500)
    sx, sy = _last_mouse_pos
    steps = max(8, duration_ms // 25)
    per_step = max(1, duration_ms // steps)
    for i in range(1, steps + 1):
        t = i / steps
        await page.mouse.move(sx + (x - sx) * t, sy + (y - sy) * t)
        await page.wait_for_timeout(per_step)
    _last_mouse_pos[0], _last_mouse_pos[1] = x, y


async def simulate_human(page):
    """Variable mix of mouse moves, scrolls, and pauses. Number of actions per page
    is randomized to avoid the 'identical activity profile' bot signal."""
    try:
        n_actions = random.choices([0, 1, 2, 3, 4], weights=[10, 30, 35, 20, 5])[0]
        for _ in range(n_actions):
            action = random.choices(
                ["move", "scroll_down", "scroll_up", "pause"],
                weights=[40, 30, 10, 20],
            )[0]
            if action == "move":
                await human_move(page, random.randint(150, 1300), random.randint(150, 750))
            elif action == "scroll_down":
                await page.evaluate(f"window.scrollBy(0, {random.randint(100, 900)})")
            elif action == "scroll_up":
                await page.evaluate(f"window.scrollBy(0, -{random.randint(50, 400)})")
            # 'pause' is just the wait below
            await page.wait_for_timeout(random.randint(100, 700))

        # ~1/30 pages: a no-op click (real click event, doesn't navigate)
        if random.random() < 1 / 30:
            await page.evaluate("document.body.click()")
        # ~1/15 pages: a Tab keypress (fires focus + key events)
        if random.random() < 1 / 15:
            await page.keyboard.press("Tab")
    except Exception:
        pass


async def navigate(page, url: str, wid: int, page_n: int) -> bool:
    """One nav attempt: goto + spoof visibility + wait for anchors + wait for date
    hydration. Returns True on success, False on any error."""
    try:
        await page.goto(url, wait_until="commit", timeout=15000)
        await page.evaluate("""
            Object.defineProperty(document, 'hidden',          {configurable: true, get: () => false});
            Object.defineProperty(document, 'visibilityState', {configurable: true, get: () => 'visible'});
        """)
        await page.wait_for_selector('a[href*="/news/home/"]', timeout=10000)
        try:
            await page.wait_for_function(
                """() => {
                    const anchors = document.querySelectorAll('a[href*="/news/home/"]');
                    const re = / at \\d{1,2}:\\d{2}\\s*(AM|PM)\\s*ET/i;
                    const dated = [...document.querySelectorAll('span')]
                        .filter(s => re.test(s.textContent)).length;
                    return anchors.length > 0 && dated >= anchors.length;
                }""",
                timeout=8000,
            )
        except Exception:
            print(f"  [w{wid}] page {page_n}: hydrate-timeout", flush=True)
        return True
    except Exception as e:
        print(f"  [w{wid}] page {page_n}: nav-fail ({e.__class__.__name__})", flush=True)
        return False


# ---------------------------------------------------------------------------
# Shared state + orchestration
# ---------------------------------------------------------------------------

def _new_session_max():
    return (30 + random.betavariate(5, 2) * 90) * 60  # seconds


class State:
    def __init__(self):
        self.next_page: int = 0
        self.end_page: int = 0
        self.max_page: int = 0
        self.pages_scraped: int = 0
        self.total_new: int = 0
        self.dup_streak: int = 0
        self.nav_fail_streak: int = 0
        self.existing_rows: dict = {}
        self.runs: list = []
        self.current_run: dict = {}
        self.run_start: float = 0.0
        self.session_start: float = 0.0
        self.session_max: float = 0.0
        self.stop = asyncio.Event()
        self.pause_event = asyncio.Event()
        self.pause_event.set()           # not paused at startup
        self.page_lock = asyncio.Lock()  # guards next_page claim
        self.csv_lock = asyncio.Lock()   # guards existing_rows + write_all
        self.state_lock = asyncio.Lock() # guards counters + runs.csv


async def worker(wid: int, ctx, state: State, args):
    page = await ctx.new_page()
    retry_waits = (0, 30, 60, 300)
    try:
        while not state.stop.is_set():
            # Session-break gate (cleared by pacer during long breaks)
            await state.pause_event.wait()
            if state.stop.is_set():
                break

            # Atomically claim the next page number
            async with state.page_lock:
                page_n = state.next_page
                if page_n > state.end_page:
                    return
                state.next_page += 1

            url = f"{BASE_URL}/newsroom?language=en&page={page_n}"
            cycle_start = time.time()
            print(f"  [w{wid}] page {page_n}: nav...", flush=True)

            nav_ok = await navigate(page, url, wid, page_n)
            for wait in retry_waits:
                if state.stop.is_set():
                    return
                if nav_ok:
                    break
                if wait:
                    print(f"  [w{wid}] page {page_n}: wait {wait}s, retry...", flush=True)
                    await asyncio.sleep(wait)
                else:
                    print(f"  [w{wid}] page {page_n}: retry now...", flush=True)
                nav_ok = await navigate(page, url, wid, page_n)

            if not nav_ok:
                print(f"\n  [w{wid}] nav retries exhausted — signaling stop")
                state.stop.set()
                return

            await simulate_human(page)

            html = await page.content()
            if len(html) < 5000:
                print(f"  [w{wid}] page {page_n}: suspiciously small response ({len(html)}B) — possibly re-challenged", flush=True)
                async with state.state_lock:
                    state.nav_fail_streak += 1
                    blocked = state.nav_fail_streak >= 3
                if blocked:
                    print("\n  3 consecutive small/failed responses — exiting (likely blocked)")
                    state.stop.set()
                    return
                await asyncio.sleep(15 + random.uniform(0, 10))
                continue

            items = parse_page(html)

            new_count = 0
            updated_count = 0
            async with state.csv_lock:
                for it in items:
                    prev = state.existing_rows.get(it["url"])
                    if prev is None:
                        state.existing_rows[it["url"]] = it
                        new_count += 1
                    else:
                        changed = False
                        for k in CSV_FIELDS:
                            if not prev.get(k) and it.get(k):
                                prev[k] = it[k]
                                changed = True
                        if changed:
                            updated_count += 1
                if new_count or updated_count:
                    write_all(state.existing_rows)

            async with state.state_lock:
                state.total_new += new_count
                state.pages_scraped += 1
                state.nav_fail_streak = 0
                if page_n > state.max_page:
                    state.max_page = page_n
                state.current_run["to_page"]     = str(state.max_page)
                state.current_run["total_pages"] = str(state.pages_scraped)
                state.current_run["duration"]    = fmt_duration(time.time() - state.run_start)
                # dup_streak is approximate under parallelism — counts page
                # completions in finish order. Reset by any new>0 completion.
                if new_count == 0:
                    state.dup_streak += 1
                    if state.dup_streak >= args.dup_stop:
                        print(f"\n  [w{wid}] {state.dup_streak} consecutive all-duplicate page completions — done")
                        state.stop.set()
                else:
                    state.dup_streak = 0
                write_runs(state.runs)
                tn = state.total_new

            print(f"  [w{wid}] page {page_n}: new={new_count}  updated={updated_count}  total_new={tn}", flush=True)

            if args.until_date and items:
                page_dts = [it["datetime"] for it in items if it.get("datetime")]
                if page_dts and all(dt[:10] < args.until_date for dt in page_dts):
                    print(f"\n  [w{wid}] all items on page {page_n} older than {args.until_date} — done")
                    state.stop.set()
                    return

            if not items:
                print(f"  [w{wid}] 0 items parsed on page {page_n} — stopping")
                state.stop.set()
                return

            # Cycle-target sleep: target whole-iteration time ~3s (log-normal),
            # subtract elapsed nav+hydration+sim+parse+write so the inter-request
            # cadence the server sees is what's randomized, not the leftover pad.
            target = min(15.0, random.lognormvariate(math.log(3) - 0.18, 0.6))
            elapsed = time.time() - cycle_start
            if target > elapsed:
                await asyncio.sleep(target - elapsed)
    finally:
        try:
            await page.close()
        except Exception:
            pass


async def session_pacer(state: State):
    """Long break (5–10min) every session_max seconds. Pauses all workers via
    pause_event; shifts run_start forward so duration excludes the break."""
    while not state.stop.is_set():
        try:
            await asyncio.wait_for(state.stop.wait(), timeout=5.0)
            return
        except asyncio.TimeoutError:
            pass
        if time.time() - state.session_start >= state.session_max:
            break_secs = random.uniform(5, 10) * 60
            print(f"\n  [break] sleeping {break_secs/60:.1f}min — session was {(time.time()-state.session_start)/60:.1f}min\n")
            state.pause_event.clear()
            try:
                await asyncio.wait_for(state.stop.wait(), timeout=break_secs)
                return
            except asyncio.TimeoutError:
                pass
            state.run_start += break_secs
            state.session_start = time.time()
            state.session_max = _new_session_max()
            state.pause_event.set()
            print(f"  [resume] next session window: {state.session_max/60:.0f}min\n")


async def probe_once(args):
    """--probe: fetch page 1, print structure, exit (no CSV writes)."""
    try:
        ensure_chrome(args.debug_port, args.chrome_profile, args.chrome_exe)
    except Exception as e:
        print(f"Could not start/find Chrome on CDP port {args.debug_port}: {e}")
        return
    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp(f"http://127.0.0.1:{args.debug_port}")
        except Exception as e:
            print(f"Failed to connect to Chrome via CDP on port {args.debug_port}.")
            print(f"Error: {e}")
            return
        ctx = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = await ctx.new_page()
        try:
            url = f"{BASE_URL}/newsroom?language=en&page=1"
            print(f"PROBE: navigating to {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)
            html = await page.content()
            print(f"  html length: {len(html)}")
            items = parse_page(html)
            print(f"  parsed items: {len(items)}")
            if items:
                print("\n--- first 3 items ---")
                for it in items[:3]:
                    for k, v in it.items():
                        print(f"  {k}: {v}")
                    print()
            else:
                print("\n--- HTML head (first 3000 chars) ---")
                print(html[:3000])
        finally:
            try:
                await page.close()
            except Exception:
                pass


async def main_async():
    sys.stdout.reconfigure(line_buffering=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"bw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    log_file = open(log_path, "a", encoding="utf-8", buffering=1)
    sys.stdout = _Tee(sys.stdout, log_file)
    sys.stderr = _Tee(sys.stderr, log_file)
    print(f"[log] {log_path}", flush=True)
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    parser = argparse.ArgumentParser()
    parser.add_argument("--debug-port", type=int, default=9222)
    parser.add_argument("--chrome-exe", type=str, default=None,
                        help="path to chrome.exe (auto-detected if omitted)")
    parser.add_argument("--chrome-profile", type=str, default=r"C:\bw-chrome-profile",
                        help="Chrome user-data-dir for auto-launch")
    parser.add_argument("--from-page",  type=int, default=None,
                        help="start page (default: max to_page in runs + 1, else 1)")
    parser.add_argument("--to-page",    type=int, default=None,
                        help="end page (inclusive). If omitted, scrape until dup-stop or until-date triggers.")
    parser.add_argument("--until-date", type=str, default=None,
                        help="stop when ALL items on a page have datetime < this YYYY-MM-DD")
    parser.add_argument("--dup-stop",   type=int, default=5,
                        help="stop after N consecutive all-duplicate page completions (default 5)")
    parser.add_argument("--parallelism", type=int, default=2,
                        help="number of concurrent tabs/workers (default 2)")
    parser.add_argument("--probe",      action="store_true",
                        help="fetch page 1, print structure, exit (no CSV writes)")
    args = parser.parse_args()

    if args.probe:
        await probe_once(args)
        return

    os.makedirs(os.path.dirname(OUTPUT_CSV) or ".", exist_ok=True)

    state = State()
    state.existing_rows = load_existing_rows()
    state.runs = load_runs()
    prior_to_pages = [int(r["to_page"]) for r in state.runs if r.get("to_page", "").isdigit()]
    max_page = max(prior_to_pages) if prior_to_pages else 0
    start_page = args.from_page if args.from_page is not None else (max_page + 1 if max_page else 1)
    end_page = args.to_page if args.to_page is not None else start_page + 100000
    state.next_page = start_page
    state.end_page = end_page

    existing_dts = [r.get("datetime", "") for r in state.existing_rows.values() if r.get("datetime")]
    newest = max(existing_dts) if existing_dts else "(none)"
    oldest = min(existing_dts) if existing_dts else "(none)"
    print(f"Existing: {len(state.existing_rows)} URLs in {OUTPUT_CSV}")
    print(f"  newest: {newest}   oldest: {oldest}   max page seen: {max_page or '(none)'}")
    print(f"Pages {start_page}..{end_page}   parallelism={args.parallelism}", end="")
    if args.until_date:
        print(f"   until_date={args.until_date}", end="")
    print("\n")

    state.run_start = time.time()
    state.session_start = time.time()
    state.session_max = _new_session_max()
    print(f"  session window: {state.session_max/60:.0f}min before break\n")

    state.current_run = {
        "started_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "from_page":   str(start_page),
        "to_page":     "",
        "total_pages": "0",
        "duration":    "00:00:00",
    }
    state.runs.append(state.current_run)
    write_runs(state.runs)

    try:
        ensure_chrome(args.debug_port, args.chrome_profile, args.chrome_exe)
    except Exception as e:
        print(f"Could not start/find Chrome on CDP port {args.debug_port}: {e}")
        return

    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp(f"http://127.0.0.1:{args.debug_port}")
        except Exception as e:
            print(f"Failed to connect to Chrome via CDP on port {args.debug_port}.")
            print(f"Error: {e}")
            return

        ctx = browser.contexts[0] if browser.contexts else await browser.new_context()

        # Close any leftover scraper tabs from prior runs that didn't exit
        # cleanly. Match by URL so we don't touch the user's primer/other tabs.
        stale = [pg for pg in ctx.pages if "/newsroom?language=en&page=" in (pg.url or "")]
        for pg in stale:
            try:
                await pg.close()
            except Exception:
                pass
        if stale:
            print(f"Closed {len(stale)} leftover scraper tab(s) from prior run(s).", flush=True)
        print(f"Connected. Context has {len(ctx.pages)} existing tab(s); opening {args.parallelism} more.", flush=True)

        pacer_task = asyncio.create_task(session_pacer(state))
        worker_tasks = [
            asyncio.create_task(worker(i, ctx, state, args))
            for i in range(args.parallelism)
        ]
        try:
            await asyncio.gather(*worker_tasks)
        finally:
            state.stop.set()
            state.pause_event.set()  # unblock pacer if it was mid-break
            await pacer_task

    state.current_run["duration"] = fmt_duration(time.time() - state.run_start)
    write_runs(state.runs)
    print(f"\nDone. {state.total_new} new articles -> {OUTPUT_CSV}")
    print(f"Ran {state.pages_scraped} pages ({state.current_run['from_page']} -> {state.current_run['to_page'] or 'none'}) in {state.current_run['duration']}")


if __name__ == "__main__":
    asyncio.run(main_async())
