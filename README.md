# AI Market Signal

Event-driven trading signal pipeline for small-cap stocks. Scrapes catalyst press releases from the major newswires, extracts structured features from each release via LLM, pairs them with intraday price reactions, and feeds an ML model that predicts continuation/reversal at decision time.

Scope is non-earnings catalysts — M&A, biotech, crypto treasury, collaborations, contracts, product launches, private placements.

Status: Newswire scraping + body-extraction running across GlobeNewswire, PRNewswire, ACCESS Newswire, and Business Wire; LLM feature extraction working. ML training stage designed, not yet built.

---

## Repository layout

The repo is a Python package run from the root with `python -m <package.module>` (imports are absolute, e.g. `from regex.catalysts import classify_catalyst`).

| Package | Role |
|---|---|
| `ingest/` | Production scrapers + HTTP price fetch (`ingest/recon/` = dev reverse-engineering tools) |
| `sources/` | Per-source processing — `gnw/`, `prnw/`, `bw/`, `anw/` (classify → filter → extract) |
| `sec/` | SEC/EDGAR 8-K/EX-99 stream (secondary source) |
| `regex/` | Shared regex — `catalysts.py` (the catalyst recall gate) |
| `features/` | Pre-ML feature extraction — `runner.py` + `schemas/` registry |
| `market/` | Polygon price/market-data fetch |
| `ml/` | Training stage (designed, empty) |
| `analysis/` | One-off eval / inspection / cleanup scripts |
| `ui_legacy/` | The old sentiment demo, self-contained |
| `sec/pipeline.py` | Orchestrates the SEC ingest steps |

---

## Newswire sources

The primary data sources are the newswire websites. Each website has its own dedicated scraper, because the data extraction implementation is website specific. 

**scrape headlines/URLs → classify/filter to the tradeable universe → fetch the article page for the full body + structured fields** — every stage append-safe (skips already-done URLs). The extracted news article bodies are the input to feature extraction (below).

| Source | Scrape → list | Filter to signal | Body extractor → output |
|---|---|---|---|
| GlobeNewswire | `ingest/gnw_scraper.py` → `gnw_news.csv` | `sources/gnw/gnw_classifier.py` → `gnw_signal_filter.py` | `sources/gnw/gnw_extract_fields.py` → `gnw_signal_articles.csv` |
| PRNewswire | `ingest/prn_scraper.py` → `data/prn_data/` | `sources/prnw/prn_classifier.py` (ticker ∈ universe) | `sources/prnw/prn_extract_fields.py` → `data/prn_articles/` |
| ACCESS Newswire | `ingest/anw_scraper.py` → `data/anw/` | post-hoc (full-run, then filter) | `sources/anw/anw_extract_fields.py` → `data/anw_articles/` |
| Business Wire | `ingest/bw_scraper.py` → `bw_news.csv` | `sources/bw/bw_signal_filter.py` | `sources/bw/bw_extract_fields.py` → `data/bw_articles/` |

(The `prnw` directory is PR Newswire — spelled `prnw` because `prn` is a reserved device name on Windows.)

**Body extraction** (`sources/*/*_extract_fields.py`) fetches each PR page and pulls JSON-LD / `og:` metadata plus the full article body into namespaced `<src>_*` columns. Most sources use plain `httpx`; 

**Business Wire is behind Akamai Bot Manager**, so its scraper and extractor drive a real warmed Chrome over CDP instead (a parallel tab pool with a block-detector that aborts before a session block escalates to an IP ban). Where the ticker is known before fetch (BW, PRN) the set is filtered first and only the universe subset is fetched; ACCESS Newswire has no pre-fetch ticker (truncated slugs) so it fetches the full archive and filters on the extracted ticker.

**Filtering** keeps rows whose ticker is in [data/ticker_universe.csv](data/ticker_universe.csv) and drops law-firm / class-action litigation releases (deadline & investor alerts, "*\<firm\> investigates*") — they carry the target company's ticker but aren't signal events. The catalyst signals are also filtered. If the title has clear catalyst (not "other") returned by regex classifier, and its one of the signal catalysts that are key to these project we only keep those rows.

---

## Feature extraction

[extract_features.py](scripts/extract_features.py) is the ML input stage: it turns a press-release body into a ~15-field LLM feature schema (Sonnet, batch or real-time) → `data/*_features.csv`. The schema covers dollar amount, commitment, specificity, hype, dilution, named partners, milestone guidance, restatement, and free-text green/red flags. Its input is the extracted article bodies from the newswire sources above.

Catalyst tagging itself is a shared, source-agnostic regex gate (`classify_catalyst` in [regex/catalysts.py](regex/catalysts.py)) applied on the title before the expensive body fetch — tuned for recall (a missed catalyst is a permanent drop; a false positive is cheap and caught downstream).

---

## SEC EDGAR (8-K / EX-99) — secondary source

An additional catalyst stream from SEC filings. [sec/pipeline.py](sec/pipeline.py) chains the ingest stages, each append-safe; `--days` / `--date-from` / `--date-to` scope only the index download, later steps process the full accumulated set.

```bash
python -m sec.pipeline --days 30 --llm --market
```

Flow: [sec/download_idx.py](sec/download_idx.py) + [sec/parse_idx.py](sec/parse_idx.py) → `data/8k.csv` → [sec/batch_filter.py](sec/batch_filter.py) (fetch filing exhibits) → `data/8k_ex99.csv` → [sec/classify_exhibits.py](sec/classify_exhibits.py) (heuristic + regex catalyst) → `data/ex_99_classified.csv` → [sec/classify_catalyst_llm.py](sec/classify_catalyst_llm.py) *(`--llm`, Haiku reclass of `catalyst=other`)* → [market/fetch_market_data.py](market/fetch_market_data.py) *(`--market`, Polygon prices + `<$500M` cap filter)*. This stream is a cross-check / coverage backstop — an 8-K confirms a release was material enough to file — rather than the primary feature feed, which comes from the newswire bodies above.

---

## ML target (design)

- Scope: <$500M point-in-time market cap, signal catalyst, price up >=10% within first 5m of news
- Decision time `t` = the moment the stock crosses +10%, not news time
- Target: quantile regression (P10/P50/P90) on log returns at multiple horizons, market-residualized at 1d
- Features: pre-news technical state + lookback news history + current-PR extracted features (~40 cols)
- Validation: walk-forward only

Layer 1 hard filters (no model): drop dilutive offerings, restatements, excluded catalyst types. Layer 2 XGBoost runs only on what passes.

---

## Sentiment UI *(demo front end)*

Original headline-sentiment demo from before the project pivoted. FinBERT / GPT-4o Mini / Claude Haiku scored on titles. Lives entirely under [ui_legacy/](ui_legacy/) (`ai_sentiment/`, `api/`, `static/`, `template/`, `finbert_service/`) — run `python -m ui_legacy.api.main`, separate from the main pipeline.

---

## Requirements

- `ANTHROPIC_API_KEY` — LLM classification + feature extraction
- `MASSIVE_API_KEY` or `POLYGON_API_KEY` — Polygon.io price data (Starter+ / unlimited tier; the market-data fetcher runs concurrent requests and assumes no rate limit)
- `SEC_USER_AGENT` — SEC EDGAR fair-access policy (e.g. `"Name email@example.com"`)
- `OPENAI_API_KEY` — only if using GPT model in the legacy UI
- Business Wire extraction needs a real Chrome (CDP) — see [ingest/bw_scraper.py](ingest/bw_scraper.py) for the warmed-profile setup
