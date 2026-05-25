"""
regex/catalysts.py — Catalyst classification from press release titles.

Source-agnostic: operates on a title string only. Used by all newswire
sources, SEC, and the feature extraction runner.
"""
import re

# Used by classify_catalyst (earnings conjunction) and sec.pr_detect (is_earnings).
_EARNINGS = re.compile(
    r"\bQ[1-4]\b|first quarter|second quarter|third quarter|fourth quarter"
    r"|full[- ]year|fiscal year|financial results|\bearnings\b",
    re.IGNORECASE,
)
_REPORTS = re.compile(r"\breports\b", re.IGNORECASE)

# Insertion order = the order tags are returned in (SIGNAL group, then EXCLUSION).
_CATALYST_PARTS = {
    # ── SIGNAL tags ──────────────────────────────────────────────────────────
    "biotech": [
        r"to presents?.{0,40}data",
        r"data.{0,40}to present",
        r"phase [123i]+[abi]?\s+(?:study|trial|data|results|clinical|readout|dose)",
        r"phase [123]/[123]",
        r"\bPDUFA\b",
        r"fda (?:approval|clearance|designation|grants|breakthrough)",
        r"clinical (?:trial|data|results|studies|development|pathway)",
        r"\btrial results\b",
        r"\bregistrational trial\b",
        r"510\(?k\)?",
        r"breakthrough device",
        r"complete response letter",
        r"\bCRL\b",
        r"(?:first|initial).{0,20}(?:commercial|patient).{0,20}(?:case|treatment|use)",
        r"\btopline\b",
        r"\bpivotal.{0,20}(?:study|trial|data)\b",
        r"\bIND\b.{0,20}(?:clearance|submission|filing)",
        r"\bNDA\b",
        r"\bBLA\b",
        r"\bsNDA\b",
        r"\bsBLA\b",
        r"orphan drug",
        r"rare disease designation",
        r"\benrolls?\b.{0,20}(?:first|initial).{0,20}patient",
    ],
    "private_placement": [r"private placement"],
    "collaboration": [
        r"strategic collaboration",
        r"collaboration agreement",
        r"\blicensing\s+agreement\b",
        r"\blicense deal\b",
        r"strategic partnership",
        r"strategic alliance",
        r"co-development agreement",
    ],
    "m&a": [
        r"\bmerger\b",
        r"\bacquires\b",
        r"to acquire",
        r"to merge",
        r"\btender offer\b",
        r"\bacquisition\b",
        r"\bcommitted to.{0,20}transaction\b",
        r"\bclosing expected\b",
        r"to be acquired",
        r"definitive agreement.{0,30}(?:acqui|merg|sale)",
        r"sale to.{0,20}(?:private equity|PE firm)",
        r"take.{0,10}private",
    ],
    "new_product": [r"unveils", r"\bintroduces\b"],
    "contract": [
        r"\bawarded?\b.{0,30}(?:contract|order|grant|funding)",
        r"\bwins?\b.{0,30}contract",
        r"\bsecures?\b.{0,30}(?:contract|order)",
        r"\breceives?\b.{0,30}order\b",
    ],
    "crypto_treasury": [
        r"bitcoin.{0,30}(?:treasury|reserve|strateg|purchase|acquisition|holding)",
        r"(?:digital asset|ethereum|crypto).{0,30}(?:treasury|reserve|strateg)",
        r"\bETH holdings\b",
        r"\bBTC holdings\b",
    ],
    # ── EXCLUSION tags ───────────────────────────────────────────────────────
    "asset_transaction": [
        r"asset sale",
        r"sale of.{0,30}(?:operations|division|unit|subsidiary|\bassets?\b)",
        r"\bdivests?\b",
        r"disposition of",
        r"agreement to sell.{0,40}(?:propert|subsidiar|stake|\bassets?\b)",
    ],
    "agreement": [r"\bagreement\b"],
    "offering": [r"registered direct offering", r"announces pricing"],
    # PIPE = Private Investment in Public Equity. Case-sensitive (?-i:PIPE) so it
    # never matches "pipeline"/"pipe" (energy/biotech).
    "pipe": [
        r"\b(?-i:PIPE)\b.{0,30}(?:contracts?|financing|investment|shares)",
        r"million.{0,20}\b(?-i:PIPE)\b|\b(?-i:PIPE)\b.{0,20}million",
    ],
    "debt_offering": [
        r"senior notes",
        r"senior unsecured",
        r"debt restructuring",
        r"restructuring (of )?debt",
        r"notes offering",
        r"credit facility",
        r"\bnotes due\b",
        r"credit facilities",
        r"exchangeable.*debentures",
        r"secured.*credit",
        r"\b\d+\.?\d*%\s+(?:senior|notes|debentures)",
    ],
    "personnel": [
        r"\bappointments?\b",
        r"\bappoints?\b",
        r"\bexecutive\b.{0,40}(?:update|names|departure|transitions?|changes?)",
        r"\bretires?\b",
        r"\bdeparture\b",
        r"\bchief\s+(?:executive|financial|operating|marketing|technology|medical)\s+officer\b",
        r"\bC[FOM]O\b",
        r"\bCEO\b",
        r"\bCOO\b",
        r"\bCTO\b",
        r"\bCMO\b",
        r"\bleadership\s+(?:changes?|transitions?|updates?)\b",
        r"\bjoins?\b.{0,30}(?:board of directors|advisory board|board as)",
        r"\bsucceeds?\b.{0,30}(?:as|CEO|CFO|COO|president|chairman)",
        r"\belects?\b.{0,30}(?:director|chairman|president)",
        r"\bnamed\b.{0,30}(?:CEO|CFO|COO|CTO|president|chairman|director)",
    ],
    "buyback": [r"share repurchase", r"stock repurchase", r"\bbuyback\b", r"repurchase program"],
    "split": [r"stock split"],
    "dividend": [r"dividends?"],
    "legal": [
        r"settlement agreement",
        r"resolv.{0,20}(?:litigation|lawsuit|patent dispute)",
        r"patent settlement",
        r"\blitigation settlement\b",
    ],
    "rights_plan": [r"rights agreement", r"shareholder rights plan", r"rights plan", r"\bpoison pill\b"],
    "nasdaq_alert": [r"minimum bid price", r"nasdaq notification"],
    "spac": [r"business combination", r"over-allotment", r"separate trading.{0,20}(?:shares|warrants)", r"de-spac"],
    "rebranding": [r"name change", r"\brebrands?\b", r"announces new name", r"formerly known as"],
    "investor_event": [
        r"investor day",
        r"analyst day",
        r"to speak at",
        r"conference call scheduled",
        r"to ring the bell",
        r"schedules.*(?:earnings call|earnings release)",
        r"to participate in.{0,40}(?:conference|summit|forum|symposium)",
        r"to present at.{0,40}(?:conference|summit|forum|symposium)",
        r"to host.{0,40}(?:conference|investor|analyst)",
    ],
    "regulatory": [
        r"commission (?:approves?|authorizes?)",
        r"authorizes? new rates",
        r"regulatory approval(?! of drug| of therapy| of treatment)",
        r"restores? compliance",
        r"nasdaq.*(?:compliance|rule)",
    ],
    "operational_update": [
        r"assets under management",
        r"\bAUM\b",
        r"monthly production",
        r"operational update",
        r"business update",
        r"annual report",
        r"shareholder letter",
        r"corporate update",
        r"termination of.{0,20}lease",
        r"\blease.{0,20}termination\b",
    ],
    "financial_update": [
        r"record.{0,30}(?:commitments?|investments?)",
        r"distribution rate",
        r"net asset value",
        r"\bNAV\b",
    ],
}

_CATALYST_RE = {
    cat: re.compile("|".join(parts), re.IGNORECASE)
    for cat, parts in _CATALYST_PARTS.items()
}
_CATALYST_PART_RE = {
    cat: [(src, re.compile(src, re.IGNORECASE)) for src in parts]
    for cat, parts in _CATALYST_PARTS.items()
}


def classify_catalyst(title):
    """
    Classify catalyst types from PR title using keyword patterns.
    Returns list of matched catalyst tags, or ['other'].

    Tags split into two groups:
      SIGNAL     — goes to LLM feature extraction + model training
      EXCLUSION  — skip LLM extraction (low/no price signal)
    """
    if not title:
        return ["other"]

    tags = [cat for cat, rx in _CATALYST_RE.items() if rx.search(title)]

    if _REPORTS.search(title) and _EARNINGS.search(title):
        tags.append("earnings")

    return tags if tags else ["other"]


def catalyst_hits(title):
    """
    Per-part fire list for false-positive analysis:
        [(catalyst, part_regex_src), ...]

    Walks every sub-pattern individually (no short-circuit), so it reports ALL
    alternatives that fired, including overlaps — unlike classify_catalyst,
    which uses the joined pattern and stops at the first hit per catalyst.
    Note: 'earnings' (the _REPORTS ∧ _EARNINGS conjunction) is not represented.
    """
    if not title:
        return []
    return [(cat, src)
            for cat, parts in _CATALYST_PART_RE.items()
            for src, rx in parts if rx.search(title)]


# Catalyst tags that carry price signal -> kept for LLM feature extraction +
# model training. Every other tag is an EXCLUSION tag (low/no price signal).
# "other" is intentionally a SIGNAL tag: an unclassified title may still be a
# real event — especially when the source title is truncated (e.g. ANW URL
# slugs cut a catalyst keyword off) — so we keep it rather than risk dropping
# signal. classify_catalyst is a recall gate: a false drop is permanent, a
# false keep is cheap.
POTENTIAL_SIGNALS = frozenset({
    "other",
    "biotech",
    "private_placement",
    "m&a",
    "crypto_treasury",
    "contract",
    "new_product",
    "collaboration",
})


def is_signal(tags) -> bool:
    """True if any catalyst tag is a price-signal catalyst (see POTENTIAL_SIGNALS)."""
    return any(tag in POTENTIAL_SIGNALS for tag in tags)
