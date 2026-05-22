"""
bw_signal_filter.py — build data/bw_signal_filtered.csv from data/bw_news.csv.

Keep a row only if BOTH:
  1. its scraped ticker is in the universe (data/ticker_universe.csv), AND
  2. its title is NOT a law-firm litigation solicitation.

Why (2): "deadline alert / class action / investor alert / <firm> investigates"
releases are tagged with the *target* company's ticker, so a ticker-only filter
sweeps them in even though they're litigation marketing, not issuer news.

Usage:  python scripts/bw_signal_filter.py
"""
import csv
import re
import sys

csv.field_size_limit(10**7)

NEWS = "data/bw_news.csv"
UNI  = "data/ticker_universe.csv"
OUT  = "data/bw_signal_filtered.csv"

# Law-firm solicitation markers — high-precision phrases + the major
# securities-litigation firms (a title carrying any of these is ~never issuer news).
# Litigation / law-firm solicitation markers. Class-action content is treated as
# noise (not a signal event) per the pipeline, so bare "class action" IS a
# trigger here — even an issuer's own settlement announcement is dropped.
_SPAM_RE = re.compile(
    r"\b(deadline alert|final deadline|investor alert|shareholder alert|"
    r"stock alert|class[ -]?action|securities class|"
    r"lead plaintiff|securities fraud|contact the firm|law offices of|"
    r"reminds (investors|shareholders)|encourages (investors|shareholders)|"
    r"alerts (investors|shareholders)|notifies (investors|shareholders)|"
    r"(investors|shareholders) who lost)\b"
    r"|investigat(es|ion)\b.{0,40}\b(claims|investors|shareholders|fraud|on behalf)\b"
    r"|\b(faruqi|bronstein|pomerantz|rosen law|levi & korsinsky|robbins geller|"
    r"kahn swick|glancy prongay|hagens berman|bragar eagel|kessler topaz|"
    r"scott\+scott|labaton|block & leviton|johnson fistel|kirby mcinerney|"
    r"gainey mckenna|the schall|kaskela|holzer|bernstein li(eb|t)|"
    r"wolf haldenstein|federman|monteverde|halper sadeh|grabar|gross law|"
    r"keller rohrback|howard g\. smith|saxena white|cohen milstein)\b",
    re.IGNORECASE,
)


def is_spam(title: str) -> bool:
    return bool(_SPAM_RE.search(title or ""))


def main():
    uni = set()
    for r in csv.DictReader(open(UNI, encoding="utf-8")):
        t = (r.get("ticker") or "").strip().upper()
        if t:
            uni.add(t)

    with open(NEWS, encoding="utf-8", newline="") as f:
        rd = csv.DictReader(f)
        cols = rd.fieldnames
        rows = list(rd)

    kept, no_ticker, spam = [], 0, []
    for r in rows:
        if (r.get("ticker") or "").strip().upper() not in uni:
            no_ticker += 1
            continue
        if is_spam(r.get("title", "")):
            spam.append(r)
            continue
        kept.append(r)

    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(kept)

    print(f"total bw_news rows : {len(rows):,}")
    print(f"  dropped no/!uni ticker : {no_ticker:,}")
    print(f"  dropped law-firm spam  : {len(spam):,}")
    print(f"  KEPT (-> {OUT}) : {len(kept):,}")
    print(f"\nspam as % of ticker-matched: {len(spam)/max(len(spam)+len(kept),1):.1%}")
    print("\n--- 20 sample EXCLUDED (spam) titles ---")
    for r in spam[:20]:
        print(f"  {r.get('ticker',''):6} {r.get('title','')[:90]}")


if __name__ == "__main__":
    main()
