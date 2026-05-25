"""
sec/pr_detect.py — Press release detection and title extraction for SEC EX-99 exhibits.

Heuristic + LLM logic to determine whether an EX-99 filing is a press release
and to extract its title from the raw HTML.
"""
import re

import anthropic
from bs4 import BeautifulSoup

from regex.catalysts import _EARNINGS

_anthropic_client = anthropic.AsyncAnthropic()

# --- PR detection heuristics (H1-H6) ---

# H1: investor relations / media contact block (checked in last 200 words)
_CONTACT_BLOCK = re.compile(
    r"\b(?:investor\s+|media\s+)?(?:contacts?|relations):\s",
    re.IGNORECASE,
)

# H2: wire service name anywhere in first 200 words
_WIRE_SERVICE = re.compile(
    r"Business\s*Wire|PR\s*Newswire|Globe\s*Newswire|Access\s*Wire"
    r"|Market\s*wired|Canada\s*Newswire|CNW\s*Group|EQS\s*News|Benzinga|Newsfile"
    r"|Access\s*Newswire",
    re.IGNORECASE,
)

# H3: explicit PR header phrases
_PR_HEADERS = re.compile(
    r"for immediate release|news release|press release",
    re.IGNORECASE,
)

# H4: standalone date anywhere in first 200 words e.g. "March 27, 2026"
_STANDALONE_DATE = re.compile(
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{1,2},?\s+\d{4}\b"
)

# H5: common press release action verbs
_PR_VERBS = re.compile(
    r"issued a press release|provides an update"
    r"|today announced|announced today|"
    r"today reported|reported today|today released|released today",
    re.IGNORECASE,
)

# H6: whitelisted exchange ticker e.g. "(NYSE: CCL)"
_TICKER = re.compile(
    r"\((?:NYSE|NASDAQ|LSE|OTCQB|OTCQX|NYSE American|NYSE Arca|NASDAQ GSM|NASDAQ CM)"
    r"(?:/(?:NYSE|NASDAQ|LSE))?[:\s]+[A-Z]{1,6}[;,\s]*"
    r"(?:(?:NYSE|NASDAQ|LSE|OTCQB|OTCQX)[:\s]+[A-Z]{1,6}[;,\s]*)?"
    r"\)",
    re.IGNORECASE,
)

_SKIP_TOKENS = re.compile(r"^(EX-\d+\.\d+|Exhibit|\S+\.html?|\d+\.\d+|\d+)$", re.IGNORECASE)

# Leading junk: EDGAR file basenames ("bod_janx2026xfinal-nr"), "-more-" markers
_JUNK_LEAD = re.compile(
    r"^(?:-\w+-|[a-z0-9]+(?:[_-][a-z0-9]+){1,}|[a-z]+\d+[a-z][a-z0-9]*)$",
    re.IGNORECASE,
)

_TITLE_GARBAGE = re.compile(
    r"^for more information|^for immediate release|^contacts?:|^media contact|^investor contact"
    r"|^source:|^about ",
    re.IGNORECASE,
)

_ANNOUNCES_TAIL = re.compile(r"\bAnnounces?\s*$", re.IGNORECASE)

_PLAIN_TITLE_VERB = re.compile(
    r"\b(announces?|appoints?|completes?|launches?|introduces?|acquires?|divests?|"
    r"names?\s+new|elects?\s+|strengthens?|establishes?|enters?\s+into|expands?\b)",
    re.IGNORECASE,
)


def _parse_soup(html_text):
    """Parse HTML, return (soup, words) — single parse reused by callers."""
    soup = BeautifulSoup(html_text, "html.parser")
    raw = " ".join(soup.stripped_strings)
    raw = re.sub(r"[​‌‍﻿]", "", raw)
    return soup, [w for w in raw.split() if w]


def _is_bold(el):
    """Return True if element or any descendant carries bold styling."""
    style = el.get("style", "")
    if "font-weight:700" in style.replace(" ", "") or "font-weight:bold" in style.replace(" ", ""):
        return True
    if re.search(r"font\s*:[^;]*\bbold\b", style, re.IGNORECASE):
        return True
    if el.find(["b", "strong"]):
        return True
    for child in el.find_all(True):
        s = child.get("style", "")
        if "font-weight:700" in s.replace(" ", "") or "font-weight:bold" in s.replace(" ", ""):
            return True
        if re.search(r"font\s*:[^;]*\bbold\b", s, re.IGNORECASE):
            return True
    return False


def _is_valid_title(text: str) -> bool:
    """Return False if text looks like a dateline, contact block, or boilerplate."""
    if not text:
        return False
    if text.rstrip().endswith(":"):
        return False
    if len(text.split()) > 35:
        return False
    if _STANDALONE_DATE.search(text) and len(text.split()) < 8:
        return False
    if _TITLE_GARBAGE.search(text):
        return False
    return True


def _bold_title(soup):
    """Return text of first valid bold <p> or bold <font> (in <div>) with 4+ words, or None."""
    for p in soup.find_all("p", limit=20):
        if not _is_bold(p):
            continue
        text = " ".join(p.get_text(" ", strip=True).split())
        if _ANNOUNCES_TAIL.search(text):
            next_p = p.find_next_sibling("p")
            if next_p:
                next_text = " ".join(next_p.get_text(" ", strip=True).split())
                if next_text:
                    text = text + " " + next_text
        if len(text.split()) >= 4 and _is_valid_title(text):
            return text
    for font in soup.find_all("font", limit=20):
        if font.find_parent("p"):
            continue
        is_bold = _is_bold(font) or bool(font.find_parent(["b", "strong"]))
        if not is_bold:
            continue
        text = " ".join(font.get_text(" ", strip=True).split())
        if len(text.split()) >= 4 and _is_valid_title(text):
            return text
    return None


def _plain_title(words):
    """Fallback: scan raw words for a title-like sentence containing a PR action verb."""
    while words and (_SKIP_TOKENS.match(words[0]) or _JUNK_LEAD.match(words[0])):
        words = words[1:]

    if not words:
        return None

    text = " ".join(words[:80])
    m = _PLAIN_TITLE_VERB.search(text)
    if not m:
        return None

    suffix = text[m.end():].split()[:25]
    candidate = (text[:m.end()] + (" " + " ".join(suffix) if suffix else "")).strip()
    candidate = re.sub(r"^(?:News|Source|Alert|Notice|Update)\s+", "", candidate, flags=re.IGNORECASE)
    candidate = " ".join(candidate.split()[:35])

    dl = _STANDALONE_DATE.search(candidate)
    if dl:
        before_date = candidate[:dl.start()]
        before_date = re.sub(r"\s+[A-Z]{2,}(?:\s+[A-Z][a-zA-Z]+)*\s*$", "", before_date)
        candidate = before_date.strip().rstrip(",–—- ")

    return candidate if _is_valid_title(candidate) else None


def _strip_slug(title: str) -> str:
    """Strip leading EDGAR filename slug(s) and skip-tokens from a title string."""
    words = title.split()
    while words and (_JUNK_LEAD.match(words[0]) or _SKIP_TOKENS.match(words[0])):
        words = words[1:]
    return " ".join(words).strip()


def extract_title(html_text):
    """Extract press release title from HTML."""
    soup, words = _parse_soup(html_text)
    title = _bold_title(soup)
    if title:
        return _strip_slug(title) or None
    result = _plain_title(list(words))
    return _strip_slug(result) if result else None


def is_earnings(html_text):
    """Return True if the bold title contains earnings keywords."""
    soup, _ = _parse_soup(html_text)
    title = _bold_title(soup)
    return bool(title and _EARNINGS.search(title))


def analyze_heuristics(html_text):
    """
    Runs all 6 heuristics independently with no hierarchy or early exit.
    Returns a dict with 1 (fired) or 0 (did not fire) for each heuristic.
    """
    _, words = _parse_soup(html_text)
    first = words[:200]
    text_first = " ".join(first)
    text_last = " ".join(words[-200:])

    return {
        "H1": int(bool(_CONTACT_BLOCK.search(text_last))),
        "H2": int(bool(_WIRE_SERVICE.search(text_first))),
        "H3": int(bool(_PR_HEADERS.search(" ".join(first[:40])))),
        "H4": int(bool(_STANDALONE_DATE.search(text_first))),
        "H5": int(bool(_PR_VERBS.search(text_first))),
        "H6": int(bool(_TICKER.search(text_first))),
    }


def classify_heuristic(signals):
    """
    Classify from a pre-computed heuristics dict (output of analyze_heuristics).
    Returns label string or None.

    Strong signals (trusted directly):
      H1        - investor/media contact block
      H2        - wire service name
      H3        - explicit PR header phrase
      H4+H6     - dateline + ticker
      H5+H6     - PR verb + ticker

    Weak signals (caller should verify with LLM):
      combined  - dateline (H4) + PR verb (H5), no ticker
      H6        - ticker alone
    """
    if signals["H1"]: return "H1"
    if signals["H2"]: return "H2"
    if signals["H3"]: return "H3"
    if signals["H6"] and signals["H4"]: return "H4+H6"
    if signals["H6"] and signals["H5"]: return "H5+H6"
    if signals["H6"]: return "H6"
    if signals["H4"] and signals["H5"]: return "combined"
    return None


async def extract_title_llm(html_text):
    """
    Extract title via LLM when heuristic extraction fails.
    Sends first 100 stripped words to Claude Haiku.
    Returns title string or None.
    """
    soup = BeautifulSoup(html_text, "html.parser")
    raw = " ".join(soup.stripped_strings)
    raw = re.sub(r"[​‌‍﻿]", "", raw)
    words = [w for w in raw.split() if w]
    while words and _SKIP_TOKENS.match(words[0]):
        words.pop(0)
    if not words:
        return None
    excerpt = " ".join(words[:100])
    message = await _anthropic_client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=50,
        system="Extract the press release title from this excerpt. Return only the title text, nothing else. If no clear title is present, return 'unknown'.",
        messages=[{"role": "user", "content": excerpt}],
    )
    result = message.content[0].text.strip()
    return None if result.lower() == "unknown" else result


async def classify_llm(html_text):
    """
    Classify using Claude Haiku. Returns "llm" if yes, None if no.
    Sends first 300 words stripped of zero-width spaces and XBRL metadata.
    """
    soup = BeautifulSoup(html_text, "html.parser")
    raw = " ".join(soup.stripped_strings)
    raw = re.sub(r"[​‌‍﻿]", "", raw)
    words = [w for w in raw.split() if w]
    while words and _SKIP_TOKENS.match(words[0]):
        words.pop(0)
    if not words:
        return None
    excerpt = " ".join(words[:300])
    message = await _anthropic_client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=10,
        system="Is the following document excerpt a press release? Answer with only 'yes' or 'no'.",
        messages=[{"role": "user", "content": excerpt}],
    )
    answer = message.content[0].text.strip().lower()
    return "llm" if answer.startswith("yes") else None
