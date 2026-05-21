# AI Market Signal

Event-driven trading signal pipeline for small-cap stocks. Detects catalyst press releases, extracts structured features via LLM, pairs them with intraday price reactions, and feeds an ML model that predicts continuation/reversal at decision time.

Scope is non-earnings catalysts — M&A, biotech, crypto treasury, collaborations, contracts, product launches, private placements.

Status: Data pipeline and feature extraction working. ML training stage designed, not yet built.

---

## Pipeline (EDGAR — primary source)

[pipeline.py](pipeline.py) chains the EDGAR ingest stages. Each step is append-safe and skips already-processed rows. The `--days` / `--date-from` / `--date-to` flags apply only to the index download; subsequent steps process the full accumulated dataset.

```bash
python pipeline.py --days 30 --llm --market
python pipeline.py --date-from 2022-01-01 --date-to 2025-12-31 --llm --market
```

1. [download_idx.py](scripts/download_idx.py) — daily SEC EDGAR indexes
2. [parse_idx.py](scripts/parse_idx.py) → `data/8k.csv`
3. [batch_filter.py](scripts/batch_filter.py) — fetch filing index pages → `data/8k_ex99.csv`
4. [classify_exhibits.py](scripts/classify_exhibits.py) — heuristics + regex catalyst tagging → `data/ex_99_classified.csv`
5. [classify_catalyst_llm.py](scripts/classify_catalyst_llm.py) *(--llm)* — Haiku batch fallback for `catalyst=other`
6. [fetch_market_data.py](scripts/fetch_market_data.py) *(--market)* — Polygon: ticker details, 1-min bars, daily bars; `<$500M` market-cap filter, point-in-time

[extract_features.py](scripts/extract_features.py) runs separately (Sonnet, batch or real-time). Extracts a 15-field schema covering dollar amount, commitment, specificity, hype, dilution, named partners, milestone guidance, restatement, and free-text green/red flags. Output: `data/pr_features.csv`.

---

## Other news sources 
These collect headlines + URLs from different press release sites.

- [scraper/stocktitan_scraper.py](scraper/stocktitan_scraper.py) — daily page scrape, ticker + tags + ST-internal scores
- [scraper/gnw_scraper.py](scraper/gnw_scraper.py) — GlobeNewsWire paginated search, Nasdaq/NYSE only, ticker via preview text or `ticker_universe` name lookup
- [scraper/prn_scraper.py](scraper/prn_scraper.py) — PRNewswire monthly gz sitemaps, all article urls since 2010 → `data/prn_data/prn_YYYY-MM.csv`

---

## ML target (designed)

- Scope: <$500M point-in-time market cap, signal catalyst, price up >=10% within first 5m of news
- Decision time `t` = the moment the stock crosses +10%, not news time
- Target: quantile regression (P10/P50/P90) on log returns at multiple horizons, market-residualized at 1d
- Features: pre-news technical state + lookback news history + current-PR extracted features (~40 cols)
- Validation: walk-forward only

Layer 1 hard filters (no model): drop dilutive offerings, restatements, excluded catalyst types. Layer 2 XGBoost runs only on what passes.

---

## Sentiment UI *(legacy demo)*

Original headline-sentiment demo from before the project pivoted. FinBERT / GPT-4o Mini / Claude Haiku scored on titles. Lives in [ai_sentiment/](ai_sentiment/), [api/](api/), [static/](static/) — runs locally, separate from the main pipeline.

---

## Requirements

- `ANTHROPIC_API_KEY` — LLM classification + feature extraction
- `MASSIVE_API_KEY` or `POLYGON_API_KEY` — Polygon.io price data (Starter+ / unlimited tier; the market-data fetcher runs concurrent requests and assumes no rate limit)
- `SEC_USER_AGENT` — SEC EDGAR fair-access policy (e.g. `"Name email@example.com"`)
- `OPENAI_API_KEY` — only if using GPT model in the legacy UI
