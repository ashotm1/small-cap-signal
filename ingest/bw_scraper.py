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
RANGES_CSV    = "data/bw_worker_ranges.csv"
LOG_DIR       = "logs"
BASE_URL      = "https://www.businesswire.com"

CSV_FIELDS    = ["datetime", "ticker", "exchange", "title", "url"]
RUNS_FIELDS   = ["started_at", "from_page", "to_page", "total_pages", "duration"]
RANGES_FIELDS = ["wid", "start_page", "end_page"]

CHUNK_SIZE          = 10000  # default gap between worker starts at first-run init
SHIFT_DUP_THRESHOLD = 3      # consecutive dups that mark the end of phase-1 shift discovery

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

def _atomic_replace(tmp: str, dst: str, attempts: int = 2, base_delay: float = 0.05) -> bool:
    """os.replace(tmp, dst) with one retry on Windows PermissionError.

    Windows fails MoveFileEx with WinError 5 when `dst` is held open by
    another process (VS Code / Excel viewing the CSV). One brief retry
    catches transient file-watcher locks; longer retries are pointless
    (persistent locks won't release) and would block the asyncio loop.
    On final failure, log + drop the .tmp and return False so the caller
    can continue. The next save attempt will pick up the latest state.
    """
    delay = base_delay
    for i in range(attempts):
        try:
            os.replace(tmp, dst)
            return True
        except PermissionError:
            if i == attempts - 1:
                print(f"  [warn] could not replace {dst} after {attempts} attempts "
                      f"(file held open by another process?) — skipping save", flush=True)
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                return False
            time.sleep(delay)
            delay *= 2


def load_existing_rows() -> dict:
    """url → row dict from existing CSV."""
    rows: dict = {}
    if not os.path.exists(OUTPUT_CSV):
        return rows
    with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows[row["url"]] = row
    return rows


# Scraped HTML occasionally carries "unusual" line terminators (LS/PS/NEL).
# They corrupt nothing but trip editors' "unusual line terminator" warnings.
# Map each to a space (1:1, so field lengths are preserved).
_LINE_SEP_FIX = {0x2028: " ", 0x2029: " ", 0x85: " "}


def _clean_row(row: dict) -> dict:
    return {k: (v.translate(_LINE_SEP_FIX) if isinstance(v, str) else v)
            for k, v in row.items()}


def write_all(rows: dict):
    """Atomic full rewrite via tmp + rename. Use only when existing rows were
    updated in place — otherwise prefer append_new()."""
    tmp = OUTPUT_CSV + ".tmp"
    with open(tmp, "w", newline="\n", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for row in rows.values():
            w.writerow(_clean_row({k: row.get(k, "") for k in CSV_FIELDS}))
    _atomic_replace(tmp, OUTPUT_CSV)


def append_new(rows: list):
    """Append rows to OUTPUT_CSV. Writes header if file is empty or missing.
    O(len(rows)) instead of O(total_rows) — used on the new-only path."""
    needs_header = not (os.path.exists(OUTPUT_CSV) and os.path.getsize(OUTPUT_CSV) > 0)
    with open(OUTPUT_CSV, "a", newline="\n", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if needs_header:
            w.writeheader()
        for row in rows:
            w.writerow(_clean_row({k: row.get(k, "") for k in CSV_FIELDS}))


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
    _atomic_replace(tmp, RUNS_CSV)


def load_worker_ranges() -> list:
    """Return [(start, end), ...] in wid order. [] if file missing."""
    if not os.path.exists(RANGES_CSV):
        return []
    rows = []
    with open(RANGES_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append((int(r["wid"]), int(r["start_page"]), int(r["end_page"])))
    rows.sort()
    return [(s, e) for _, s, e in rows]


def save_worker_ranges(ranges: list):
    """Atomic rewrite. Rows are sorted by start_page so wid in the persisted
    file always reflects page-order (lowest start = wid 0). The in-memory
    wid for a running worker is NOT reassigned — sorting is for persistence
    only, so the NEXT run loads workers in page-sorted order even after
    relocations scrambled them this run."""
    os.makedirs(os.path.dirname(RANGES_CSV) or ".", exist_ok=True)
    tmp = RANGES_CSV + ".tmp"
    sorted_ranges = sorted(ranges, key=lambda r: r[0])
    with open(tmp, "w", newline="\n", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RANGES_FIELDS)
        w.writeheader()
        for wid, (s, e) in enumerate(sorted_ranges):
            w.writerow({"wid": wid, "start_page": s, "end_page": e})
    _atomic_replace(tmp, RANGES_CSV)


def _save_ranges_locked(state):
    """Persist the pool, omitting ranges marked 'dropped' (a fully consumed
    front range whose worker has exited). Routing every in-run save through
    here keeps a dropped range from being re-added by another worker's save.
    Caller MUST hold state.range_lock."""
    save_worker_ranges([
        (s, e) for i, (s, e) in enumerate(zip(state.worker_start, state.worker_end))
        if state.range_status[i] != "dropped"
    ])


def _find_infiltrated(state, ridx: int, page_n: int):
    """Return index of a range (other than ridx) whose [start, end] contains
    page_n, else None. Used to detect when a worker has scraped forward into
    territory already covered by another range."""
    for idx in range(len(state.worker_start)):
        if idx == ridx or state.range_status[idx] == "dropped":
            continue
        if state.worker_start[idx] <= page_n <= state.worker_end[idx]:
            return idx
    return None


def _claim_free_range_locked(state) -> int | None:
    """Claim the lowest-start range whose status is 'free', mark it 'live', and
    return its index. None if no free range. Caller MUST hold state.range_lock."""
    free = [i for i in range(len(state.range_status)) if state.range_status[i] == "free"]
    if not free:
        return None
    idx = min(free, key=lambda i: state.worker_start[i])
    state.range_status[idx] = "live"
    return idx


def _new_chunk_locked(state) -> int:
    """Append a fresh 'live' chunk at (max known page + CHUNK_SIZE), persist,
    return its index. Caller MUST hold state.range_lock."""
    start = max(max(state.worker_start), max(state.worker_end)) + CHUNK_SIZE
    state.worker_start.append(start)
    state.worker_end.append(start)
    state.range_status.append("live")
    idx = len(state.worker_start) - 1
    _save_ranges_locked(state)
    return idx


def compute_worker_ranges(existing: list, parallelism: int, first_run_start: int) -> list:
    """Return per-worker (start, end) of length `parallelism`.

    First-run init (no existing):
        Wi = (first_run_start + i*CHUNK_SIZE, same)   # end==start marks "fresh"

    Extension (existing has fewer entries than parallelism):
        Each new wid gets start = max(existing_ends) + CHUNK_SIZE.

    If existing has MORE entries than parallelism, all entries are kept
    (don't drop stored ranges when the user runs with smaller parallelism).
    Only the first `parallelism` workers spawn; the rest still bound chunks.
    """
    if not existing:
        return [(first_run_start + i * CHUNK_SIZE,
                 first_run_start + i * CHUNK_SIZE) for i in range(parallelism)]
    ranges = list(existing)
    while len(ranges) < parallelism:
        new_start = max(e for _, e in ranges) + CHUNK_SIZE
        ranges.append((new_start, new_start))
    return ranges


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
                  wait_secs: float = 15.0) -> bool:
    """Connect to existing CDP if up; otherwise launch Chrome detached with the
    given debug port + profile dir and wait until the port is reachable.

    Returns True if Chrome was just launched by this call, False if a CDP
    listener was already on the port.
    """
    if _cdp_port_open(debug_port):
        return False
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
        # Stop Chrome throttling non-foreground tabs — without these, only the
        # focused tab loads at full speed and parallel workers stall (the
        # globe-in-spinner). Lets N tabs actually load concurrently.
        "--disable-renderer-backgrounding",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-features=CalculateNativeWinOcclusion",
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
            return True
    raise TimeoutError(f"Chrome didn't expose CDP on port {debug_port} within {wait_secs}s")


async def _cleanup_existing_pages(ctx, launched_chrome: bool):
    """Close stale tabs in the CDP context. If we just launched Chrome, close
    every tab (the default new-tab is the only thing there). If Chrome was
    already running, only close leftover scraper tabs (match by URL) so the
    user's other tabs are preserved.

    If closing would empty the context, opens a blank placeholder first to
    keep the Chrome process alive (closing the last window exits Chrome)."""
    if launched_chrome:
        victims = list(ctx.pages)
        label = "default"
    else:
        victims = [pg for pg in ctx.pages
                   if "/newsroom?language=en&page=" in (pg.url or "")]
        label = "leftover scraper"
    survivors = [pg for pg in ctx.pages if pg not in victims]
    if not survivors and victims:
        try:
            await ctx.new_page()  # placeholder; worker tabs will be opened by main
        except Exception:
            pass
    for pg in victims:
        try:
            await pg.close()
        except Exception:
            pass
    if victims:
        print(f"Closed {len(victims)} {label} tab(s).", flush=True)


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

    Returns (handler_total_sec, n_steps).
    """
    if duration_ms is None:
        duration_ms = random.randint(200, 500)
    sx, sy = _last_mouse_pos
    steps = min(5, max(2, duration_ms // 75))
    per_step = (duration_ms / steps) / 1000  # seconds
    handler_total = 0.0
    for i in range(1, steps + 1):
        t = i / steps
        await page.mouse.move(sx + (x - sx) * t, sy + (y - sy) * t)
        # Diagnostic: pull per-handler timings injected via init script.
        try:
            batch = await page.evaluate(
                "(() => { const t = window.__hTimes || []; window.__hTimes = []; return t; })()"
            )
            for _name, ms in batch:
                handler_total += ms / 1000.0
        except Exception:
            pass
        await asyncio.sleep(per_step)
    _last_mouse_pos[0], _last_mouse_pos[1] = x, y
    return handler_total, steps


async def simulate_human(page):
    """Variable mix of mouse moves, scrolls, and pauses. Returns a compact
    breakdown string of per-action wall-clock times (temp instrumentation)."""
    timings: list[tuple[str, float]] = []
    try:
        n_actions = random.choices([0, 1, 2, 3, 4], weights=[10, 30, 35, 20, 5])[0]
        for _ in range(n_actions):
            action = random.choices(
                ["move", "scroll_down", "scroll_up", "pause"],
                weights=[15, 30, 15, 40],
            )[0]
            t0 = time.time()
            if action == "move":
                h, n = await human_move(page, random.randint(150, 1300), random.randint(150, 750))
                tot = time.time() - t0
                timings.append((f"move(n:{n},h:{h:.2f},o:{max(0, tot-h):.2f})", tot))
            elif action == "scroll_down":
                await page.evaluate(f"window.scrollBy(0, {random.randint(100, 900)})")
                timings.append(("scrollD", time.time() - t0))
            elif action == "scroll_up":
                await page.evaluate(f"window.scrollBy(0, -{random.randint(50, 400)})")
                timings.append(("scrollU", time.time() - t0))
            # else: 'pause' is just the wait below
            t1 = time.time()
            await asyncio.sleep(random.randint(100, 700) / 1000)
            timings.append(("pause", time.time() - t1))

        # ~1/30 pages: a no-op click (real click event, doesn't navigate)
        if random.random() < 1 / 30:
            t0 = time.time()
            await page.evaluate("document.body.click()")
            timings.append(("click", time.time() - t0))
        # ~1/15 pages: a Tab keypress (fires focus + key events)
        if random.random() < 1 / 15:
            t0 = time.time()
            await page.keyboard.press("Tab")
            timings.append(("tab", time.time() - t0))
    except Exception:
        pass
    if not timings:
        return "sim=(noop)"
    return "sim=(" + " ".join(f"{n}:{t:.2f}" for n, t in timings) + ")"


def _is_target_closed(e: Exception) -> bool:
    return ("TargetClosed" in type(e).__name__ or
            "Target page, context or browser has been closed" in str(e))


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
        except Exception as e:
            if _is_target_closed(e):
                raise
            print(f"  [w{wid}] page {page_n}: hydrate-timeout", flush=True)
        return True
    except Exception as e:
        if _is_target_closed(e):
            raise
        print(f"  [w{wid}] page {page_n}: nav-fail ({e.__class__.__name__})", flush=True)
        return False


# ---------------------------------------------------------------------------
# Shared state + orchestration
# ---------------------------------------------------------------------------

def _new_session_max():
    return (30 + random.betavariate(5, 2) * 90) * 60  # seconds


class State:
    def __init__(self):
        # Range pool (indexed by range id, NOT worker id). Grows as new chunks
        # are appended. Workers claim/release indices from this pool.
        self.worker_start: list = []
        self.worker_end:   list = []
        self.range_status: list = []   # per-range: "free" | "live" | "burnt"
        self.pages_scraped: int = 0
        self.total_new: int = 0
        self.nav_fail_streak: int = 0
        self.existing_rows: dict = {}
        self.runs: list = []
        self.current_run: dict = {}
        self.nav_started: bool = False
        self.run_start: float = 0.0
        self.session_start: float = 0.0
        self.session_max: float = 0.0
        self.stop = asyncio.Event()
        self.pause_event = asyncio.Event()
        self.pause_event.set()           # not paused at startup
        self.csv_lock = asyncio.Lock()   # guards existing_rows + write_all/append_new
        self.state_lock = asyncio.Lock() # guards counters + runs.csv
        self.range_lock = asyncio.Lock() # guards worker_start/end + claimed/burnt + ranges save


_MOUSEMOVE_TIMER_SCRIPT = """
(() => {
  if (window.__hTimesInstalled) return;
  window.__hTimesInstalled = true;
  window.__hTimes = [];
  const origAdd = EventTarget.prototype.addEventListener;
  EventTarget.prototype.addEventListener = function(type, fn, ...rest) {
    if (type === 'mousemove' && typeof fn === 'function') {
      const wrapped = function(e) {
        const t0 = performance.now();
        try { return fn.call(this, e); }
        finally { window.__hTimes.push([fn.name || 'anon', performance.now() - t0]); }
      };
      return origAdd.call(this, type, wrapped, ...rest);
    }
    return origAdd.call(this, type, fn, ...rest);
  };
})();
"""


async def worker(wid: int, ctx, state: State, args):
    """One Chrome tab scraping its assigned page range.

    Range model: each worker owns a persistent (start, end) stored in
    data/bw_worker_ranges.csv. Always resumes at `start` on every run.

    Fresh worker (end == start, never scraped):
        Scrape forward from start. Stop on per-worker --dup-stop OR chunk
        boundary (next worker's start, +inf for the last worker).

    Resuming worker (end > start):
        Phase 1 (shift discovery): scrape forward from start, counting
            consecutive dup pages. When SHIFT_DUP_THRESHOLD seen,
            shift_count = pages_scraped_in_phase1 - threshold.
            If shift_count <= 0, no shift; worker is done.
        Phase 2 (catch-up): jump to (old_end + shift_count). Scrape forward
            like a fresh worker (dup-stop OR chunk boundary).
    """
    page = await ctx.new_page()
    retry_waits = (0, 30, 60, 300)

    ridx = wid
    async with state.range_lock:
        state.range_status[ridx] = "live"
    start = state.worker_start[ridx]
    end   = state.worker_end[ridx]
    is_fresh = (end <= start)

    page_n = start
    pages_in_phase1 = 0
    consecutive_dups = 0
    in_phase_2 = is_fresh   # fresh workers behave like phase 2 (no shift detection)
    max_scraped = start - 1

    if is_fresh:
        print(f"  [w{wid}] r{ridx} FRESH start={start}", flush=True)
    else:
        print(f"  [w{wid}] r{ridx} RESUME range=[{start}, {end}]", flush=True)

    try:
        while not state.stop.is_set():
            await state.pause_event.wait()
            if state.stop.is_set():
                break

            if args.to_page is not None and page_n > args.to_page:
                print(f"  [w{wid}] reached --to-page {args.to_page} — done", flush=True)
                break

            url = f"{BASE_URL}/newsroom?language=en&page={page_n}"
            cycle_start = time.time()
            phase = "p1" if not in_phase_2 else ("fresh" if is_fresh else "p2")
            print(f"  [w{wid}] page {page_n} ({phase}): nav...", flush=True)

            # Start run timer on first navigation (not at script startup)
            async with state.state_lock:
                if not state.nav_started:
                    state.nav_started = True
                    state.run_start = time.time()
                    state.current_run["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    write_runs(state.runs)

            # --- TIMING (temporary instrumentation) ---
            t_nav_start = time.time()
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
            t_nav = time.time() - t_nav_start

            if not nav_ok:
                print(f"\n  [w{wid}] nav retries exhausted — signaling stop")
                state.stop.set()
                return

            sim_str = await simulate_human(page)

            t_parse_start = time.time()
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
            t_parse = time.time() - t_parse_start

            new_count = 0
            updated_count = 0
            async with state.csv_lock:
                new_items = []
                for it in items:
                    prev = state.existing_rows.get(it["url"])
                    if prev is None:
                        state.existing_rows[it["url"]] = it
                        new_items.append(it)
                        new_count += 1
                    else:
                        changed = False
                        for k in CSV_FIELDS:
                            if not prev.get(k) and it.get(k):
                                prev[k] = it[k]
                                changed = True
                        if changed:
                            updated_count += 1
                # Fast path: only new rows → append. Full rewrite only when an
                # existing row was back-filled (touches rows in the middle).
                if updated_count:
                    write_all(state.existing_rows)
                elif new_items:
                    append_new(new_items)

            if page_n > max_scraped:
                max_scraped = page_n
            if new_count == 0:
                consecutive_dups += 1
            else:
                consecutive_dups = 0
            if not in_phase_2:
                pages_in_phase1 += 1

            async with state.range_lock:
                # Persist the range's end whenever it advances.
                if max_scraped > state.worker_end[ridx]:
                    state.worker_end[ridx] = max_scraped
                    _save_ranges_locked(state)
                running_max = max(state.worker_end)
            async with state.state_lock:
                state.total_new += new_count
                state.pages_scraped += 1
                state.nav_fail_streak = 0
                state.current_run["to_page"]     = str(running_max)
                state.current_run["total_pages"] = str(state.pages_scraped)
                state.current_run["duration"]    = fmt_duration(time.time() - state.run_start)
                write_runs(state.runs)
                tn = state.total_new

            t_total = time.time() - cycle_start
            print(f"  [w{wid}] page {page_n} ({phase}): new={new_count}  updated={updated_count}  "
                  f"total_new={tn}  cdup={consecutive_dups}  "
                  f"[nav={t_nav:.2f}s {sim_str} parse={t_parse:.2f}s total={t_total:.2f}s]",
                  flush=True)

            # until-date stays a GLOBAL stop — once we've reached articles older
            # than the cutoff, no worker should keep going regardless of range.
            if args.until_date and items:
                page_dts = [it["datetime"] for it in items if it.get("datetime")]
                if page_dts and all(dt[:10] < args.until_date for dt in page_dts):
                    print(f"\n  [w{wid}] all items on page {page_n} older than {args.until_date} — global stop")
                    state.stop.set()
                    return

            if not items:
                # Past end of valid pages for this worker's region. Stop just
                # this worker (other workers' ranges may still be valid).
                print(f"  [w{wid}] 0 items parsed on page {page_n} — stopping this worker", flush=True)
                break

            # Stop / phase-transition decisions
            if not in_phase_2:
                # Phase 1: shift discovery. Once we see SHIFT_DUP_THRESHOLD
                # consecutive dups, jump to (old_end + shift_count) and continue
                # like a fresh worker — even when shift_count == 0 (no shift at
                # top of range, but new content may still exist past old_end).
                # Also advance the stored start to the jump position: pages
                # between (old_start + pages_in_phase1) and (old_end + shift_count)
                # are known dups (shifted-down content already in CSV), so next
                # run can skip directly to the new frontier for shift detection.
                if consecutive_dups >= SHIFT_DUP_THRESHOLD:
                    shift_count = pages_in_phase1 - SHIFT_DUP_THRESHOLD
                    new_page_n = end + shift_count
                    # Do NOT advance worker_start. The 50 new URLs we just
                    # picked up in phase 1 are at original positions below
                    # the original start; if we advance start, next run's
                    # phase 1 lands in already-scraped territory and reads
                    # shift_count=0, missing real shifts. Keep start static.
                    print(f"  [w{wid}] phase 1 done: shift_count={shift_count}, jumping to page {new_page_n}; start stays at {start}", flush=True)
                    page_n = new_page_n
                    in_phase_2 = True
                    consecutive_dups = 0
                    # skip page_n += 1 — we just set it explicitly
                    target = min(15.0, random.lognormvariate(math.log(args.cycle_mean) - 0.18, 0.6))
                    elapsed = time.time() - cycle_start
                    if target > elapsed:
                        await asyncio.sleep(target - elapsed)
                    continue
            else:
                # Phase 2 / fresh. Two ways the current range ends:
                #   (a) we crossed into another range (infiltration), or
                #   (b) we hit dup-stop in our own territory.
                # Both burn the current range and switch to a new one; only the
                # next-range choice differs.
                next_ridx = None
                if consecutive_dups >= SHIFT_DUP_THRESHOLD:
                    async with state.range_lock:
                        inf = _find_infiltrated(state, ridx, page_n)
                        if inf is not None:
                            # A range only ever FINISHES by infiltrating another.
                            # Drop the finished range so the next run won't RESUME it.
                            #   middle range: hand its front boundary to the infiltrated
                            #     range (move inf.start down to ridx.start) so inf's
                            #     phase-1 resumes there and can still detect shift counts.
                            #   lowest range: just drop it — the next-smallest range keeps
                            #     its start with thousands of dup pages before it, so its
                            #     phase-1 runs but won't detect shifts (accepted drawback).
                            live = [i for i in range(len(state.worker_start))
                                    if state.range_status[i] != "dropped"]
                            is_lowest = ridx == min(live, key=lambda i: state.worker_start[i])
                            if not is_lowest:
                                state.worker_start[inf] = state.worker_start[ridx]
                            state.range_status[ridx] = "dropped"           # remove from CSV
                            next_ridx = _claim_free_range_locked(state)    # free range, else fresh chunk
                            if next_ridx is None:
                                next_ridx = _new_chunk_locked(state)
                            _save_ranges_locked(state)
                            kind = ("lowest — dropped" if is_lowest
                                    else f"middle — dropped, r{inf}.start→{state.worker_start[inf]}")
                            print(f"  [w{wid}] r{ridx} infiltrated r{inf} at page {page_n} "
                                  f"(cdup={consecutive_dups}; {kind}) — switching to r{next_ridx}",
                                  flush=True)
                if next_ridx is None and consecutive_dups >= args.dup_stop:
                    # Dup-stop (no infiltration) is NOT a finish — keep the range
                    # (burnt, still persisted) so the next run resumes it. Only
                    # infiltration/completion drops a range from the pool.
                    async with state.range_lock:
                        state.range_status[ridx] = "burnt"
                        next_ridx = _claim_free_range_locked(state)        # free range, else exit
                        _save_ranges_locked(state)
                    if next_ridx is None:
                        print(f"  [w{wid}] r{ridx} dup-stop ({args.dup_stop} dups), no free range — exiting", flush=True)
                        break
                    print(f"  [w{wid}] r{ridx} dup-stop ({args.dup_stop} dups) — switching to r{next_ridx}", flush=True)
                if next_ridx is not None:
                    ridx = next_ridx
                    start = state.worker_start[ridx]
                    end   = state.worker_end[ridx]
                    is_fresh = (end <= start)
                    in_phase_2 = is_fresh
                    page_n = start
                    pages_in_phase1 = 0
                    consecutive_dups = 0
                    max_scraped = start - 1
                    target = min(15.0, random.lognormvariate(math.log(args.cycle_mean) - 0.18, 0.6))
                    elapsed = time.time() - cycle_start
                    if target > elapsed:
                        await asyncio.sleep(target - elapsed)
                    continue

            page_n += 1

            # Cycle-target sleep: target whole-iteration time ~--cycle-mean s
            # (log-normal, σ=0.6), subtract elapsed nav+hydration+sim+parse+write
            # so the inter-request cadence the server sees is what's randomized.
            target = min(15.0, random.lognormvariate(math.log(args.cycle_mean) - 0.18, 0.6))
            elapsed = time.time() - cycle_start
            if target > elapsed:
                await asyncio.sleep(target - elapsed)
    finally:
        async with state.range_lock:
            if state.range_status[ridx] == "live":
                state.range_status[ridx] = "burnt"
            print(f"  [w{wid}] DONE  last range r{ridx}=[{state.worker_start[ridx]}, {state.worker_end[ridx]}]", flush=True)
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
        launched_chrome = ensure_chrome(args.debug_port, args.chrome_profile, args.chrome_exe)
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
        await _cleanup_existing_pages(ctx, launched_chrome)
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
                        help="W0 start for first-run init only. Ignored if "
                             f"{RANGES_CSV} already exists (delete to re-init).")
    parser.add_argument("--to-page",    type=int, default=None,
                        help="global per-worker upper bound. Worker stops when page_n > --to-page.")
    parser.add_argument("--until-date", type=str, default=None,
                        help="global stop when ALL items on a page have datetime < this YYYY-MM-DD")
    parser.add_argument("--dup-stop",   type=int, default=8,
                        help="per-worker: stop after N consecutive all-duplicate pages "
                             "(applies in fresh mode and phase 2; phase 1 always uses "
                             f"SHIFT_DUP_THRESHOLD={SHIFT_DUP_THRESHOLD}). default 8")
    parser.add_argument("--parallelism", type=int, default=2,
                        help="number of concurrent tabs/workers (default 2)")
    parser.add_argument("--cycle-mean", type=float, default=3.0,
                        help="per-worker cycle-target sleep mean in seconds, log-normal σ=0.6 (default 3.0)")
    parser.add_argument("--probe",      action="store_true",
                        help="fetch page 1, print structure, exit (no CSV writes)")
    args = parser.parse_args()

    # dup-stop must exceed SHIFT_DUP_THRESHOLD or phase-2 stops before the
    # relocation check can ever fire (relocation requires cdup>=SHIFT_DUP_THRESHOLD).
    if args.dup_stop <= SHIFT_DUP_THRESHOLD:
        bumped = SHIFT_DUP_THRESHOLD + 1
        print(f"  --dup-stop {args.dup_stop} <= SHIFT_DUP_THRESHOLD ({SHIFT_DUP_THRESHOLD}); bumped to {bumped}", flush=True)
        args.dup_stop = bumped

    if args.probe:
        await probe_once(args)
        return

    os.makedirs(os.path.dirname(OUTPUT_CSV) or ".", exist_ok=True)

    state = State()
    state.existing_rows = load_existing_rows()
    state.runs = load_runs()
    prior_to_pages = [int(r["to_page"]) for r in state.runs if r.get("to_page", "").isdigit()]
    max_page = max(prior_to_pages) if prior_to_pages else 0

    existing_ranges = load_worker_ranges()
    if existing_ranges:
        if args.from_page is not None:
            print(f"  --from-page {args.from_page} IGNORED: {RANGES_CSV} exists; delete to re-init")
        first_run_start = 0  # unused
    else:
        first_run_start = (args.from_page if args.from_page is not None
                           else (max_page + 1 if max_page else 1))
    ranges = compute_worker_ranges(existing_ranges, args.parallelism, first_run_start)
    save_worker_ranges(ranges)   # persist any extension (new workers added)
    state.worker_start = [s for s, _ in ranges]
    state.worker_end   = [e for _, e in ranges]
    state.range_status = ["free"] * len(ranges)   # workers flip their own to live/burnt

    existing_dts = [r.get("datetime", "") for r in state.existing_rows.values() if r.get("datetime")]
    newest = max(existing_dts) if existing_dts else "(none)"
    oldest = min(existing_dts) if existing_dts else "(none)"
    print(f"Existing: {len(state.existing_rows)} URLs in {OUTPUT_CSV}")
    print(f"  newest: {newest}   oldest: {oldest}   max page seen: {max_page or '(none)'}")
    print(f"Worker ranges (parallelism={args.parallelism}):")
    for wid, (s, e) in enumerate(ranges):
        kind = "FRESH" if e <= s else "RESUME"
        print(f"  w{wid}: [{s}, {e}] {kind}")
    if args.until_date:
        print(f"  until_date={args.until_date}")
    print()

    state.session_start = time.time()
    state.session_max = _new_session_max()
    print(f"  session window: {state.session_max/60:.0f}min before break\n")

    # from_page/to_page in runs.csv: span across all workers' ranges
    # (legacy schema — kept stable so prior-run lookup still works).
    state.current_run = {
        "started_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "from_page":   str(min(state.worker_start)),
        "to_page":     "",
        "total_pages": "0",
        "duration":    "00:00:00",
    }
    state.runs.append(state.current_run)
    write_runs(state.runs)

    try:
        launched_chrome = ensure_chrome(args.debug_port, args.chrome_profile, args.chrome_exe)
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
        await _cleanup_existing_pages(ctx, launched_chrome)
        await ctx.add_init_script(script=_MOUSEMOVE_TIMER_SCRIPT)
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
    print(f"Ran {state.pages_scraped} pages in {state.current_run['duration']}")
    print("Worker ranges (saved):")
    for wid, (s, e) in enumerate(zip(state.worker_start, state.worker_end)):
        print(f"  w{wid}: [{s}, {e}]")


if __name__ == "__main__":
    asyncio.run(main_async())
