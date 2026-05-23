# AI Market Signal

Event-driven trading signal pipeline for small-cap stocks. Scrapes catalyst press releases from the major newswires, extracts structured features from each release via LLM, pairs them with intraday price reactions, and feeds an ML model that predicts continuation/reversal at decision time.

Scope is non-earnings catalysts — M&A, biotech, crypto treasury, collaborations, contracts, product launches, private placements.

Status: Newswire scraping + body-extraction running across GlobeNewswire, PRNewswire, ACCESS Newswire, and Business Wire; LLM feature extraction working. ML training stage designed, not yet built.

---

## Newswire sources

The primary data sources are the newswire websites. Each website has its own dedicated scraper, because the data extraction implementation is website specific. 

**scrape headlines/URLs → classify/filter to the tradeable universe → fetch the article page for the full body + structured fields** — every stage append-safe (skips already-done URLs). The extracted news article bodies are the input to feature extraction (below).

| Source | Scrape → list | Filter to signal | Body extractor → output |
|---|---|---|---|
| GlobeNewswire | `gnw_scraper.py` → `gnw_news.csv` | `gnw_classifier.py` → `gnw_signal_filter.py` | `gnw_extract_fields.py` → `gnw_signal_articles.csv` |
| PRNewswire | `prn_scraper.py` → `data/prn_data/` | `prn_classifier.py` (ticker ∈ universe) | `prn_extract_fields.py` → `data/prn_articles/` |
| ACCESS Newswire | `anw_scraper.py` → `data/anw/` | post-hoc (full-run, then filter) | `anw_extract_fields.py` → `data/anw_articles/` |
| Business Wire | `bw_scraper.py` → `bw_news.csv` | `bw_signal_filter.py` | `bw_extract_fields.py` → `data/bw_articles/` |

**Body extraction** ([scripts/](scripts/)`*_extract_fields.py`) fetches each PR page and pulls JSON-LD / `og:` metadata plus the full article body into namespaced `<src>_*` columns. Most sources use plain `httpx`; 

**Business Wire is behind Akamai Bot Manager**, so its scraper and extractor drive a real warmed Chrome over CDP instead (a parallel tab pool with a block-detector that aborts before a session block escalates to an IP ban). Where the ticker is known before fetch (BW, PRN) the set is filtered first and only the universe subset is fetched; ACCESS Newswire has no pre-fetch ticker (truncated slugs) so it fetches the full archive and filters on the extracted ticker.

**Filtering** keeps rows whose ticker is in [data/ticker_universe.csv](data/ticker_universe.csv) and drops law-firm / class-action litigation releases (deadline & investor alerts, "*\<firm\> investigates*") — they carry the target company's ticker but aren't signal events. The catalyst signals are also filtered. If the title has clear catalyst (not "other") returned by regex classifier, and its one of the signal catalysts that are key to these project we only keep those rows.

---

## Feature extraction

[extract_features.py](scripts/extract_features.py) is the ML input stage: it turns a press-release body into a ~15-field LLM feature schema (Sonnet, batch or real-time) → `data/*_features.csv`. The schema covers dollar amount, commitment, specificity, hype, dilution, named partners, milestone guidance, restatement, and free-text green/red flags. Its input is the extracted article bodies from the newswire sources above.

Catalyst tagging itself is a shared, source-agnostic regex gate (`classify_catalyst` in [pr_detection.py](scripts/pr_detection.py)) applied on the title before the expensive body fetch — tuned for recall (a missed catalyst is a permanent drop; a false positive is cheap and caught downstream).

---

## SEC EDGAR (8-K / EX-99) — secondary source

An additional catalyst stream from SEC filings. [pipeline.py](pipeline.py) chains the ingest stages, each append-safe; `--days` / `--date-from` / `--date-to` scope only the index download, later steps process the full accumulated set.

```bash
python pipeline.py --days 30 --llm --market
```

Flow: [download_idx.py](scripts/download_idx.py) + [parse_idx.py](scripts/parse_idx.py) → `data/8k.csv` → [batch_filter.py](scripts/batch_filter.py) (fetch filing exhibits) → `data/8k_ex99.csv` → [classify_exhibits.py](scripts/classify_exhibits.py) (heuristic + regex catalyst) → `data/ex_99_classified.csv` → [classify_catalyst_llm.py](scripts/classify_catalyst_llm.py) *(`--llm`, Haiku reclass of `catalyst=other`)* → [fetch_market_data.py](scripts/fetch_market_data.py) *(`--market`, Polygon prices + `<$500M` cap filter)*. The classified exhibits feed the same feature-extraction stage above.

---

## ML target (designed)

- Scope: <$500M point-in-time market cap, signal catalyst, price up >=10% within first 5m of news
- Decision time `t` = the moment the stock crosses +10%, not news time
- Target: quantile regression (P10/P50/P90) on log returns at multiple horizons, market-residualized at 1d
- Features: pre-news technical state + lookback news history + current-PR extracted features (~40 cols)
- Validation: walk-forward only

Layer 1 hard filters (no model): drop dilutive offerings, restatements, excluded catalyst types. Layer 2 XGBoost runs only on what passes.

---

## Sentiment UI *(demo front end)*

Original headline-sentiment demo from before the project pivoted. FinBERT / GPT-4o Mini / Claude Haiku scored on titles. Lives in [ai_sentiment/](ai_sentiment/), [api/](api/), [static/](static/) — runs locally, separate from the main pipeline.

---

## Requirements

- `ANTHROPIC_API_KEY` — LLM classification + feature extraction
- `MASSIVE_API_KEY` or `POLYGON_API_KEY` — Polygon.io price data (Starter+ / unlimited tier; the market-data fetcher runs concurrent requests and assumes no rate limit)
- `SEC_USER_AGENT` — SEC EDGAR fair-access policy (e.g. `"Name email@example.com"`)
- `OPENAI_API_KEY` — only if using GPT model in the legacy UI
- Business Wire extraction needs a real Chrome (CDP) — see [scraper/bw_scraper.py](scraper/bw_scraper.py) for the warmed-profile setup
