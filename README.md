# NXMAN Daily Market Brief

A professional Streamlit-based market dashboard and daily newsletter PDF generator.

## Features

- **Unified indicator cards** — each card is a single Plotly figure containing metric values *and* a 30-day sparkline visually integrated inside the coloured border. Green/red borders reflect daily direction.
- **Two card strips** — Market Indicators (S&P 500, Nasdaq, Stoxx 600, MSCI World, SMI) and Asset Class Performance (global equities, bonds, gold, bitcoin, oil, US 10Y, EUR/CHF).
- **Core cross-asset YTD chart** — weekly-resampled YTD performance with event markers (Iran conflict start, Iran ceasefire).
- **Morning snapshot mode** — freezes the first run at/after 08:00 Zurich; re-runs reproduce the same newsletter all day.
- **Live mode** — always fetches fresh data.
- **Gemini writing layer** — headline, subheadline, What Matters bullets, news summary. Falls back gracefully if unavailable.
- **Marketaux news** — ranked, deduplicated, topic-classified (Macro/Rates, Geopolitics, Equities, Commodities, Crypto).
- **One-page landscape PDF** — download button generates a polished newsletter-ready PDF.
- **Streamlit Cloud ready** — `get_secret()` reads from `st.secrets` (cloud) then `.env` (local).

## Secrets / API keys

| Key | Required | Notes |
|---|---|---|
| `GEMINI_API_KEY` | Optional | Enables AI-written commentary |
| `GEMINI_MODEL` | Optional | Default: `gemini-2.5-flash` |
| `GEMINI_FALLBACK_MODELS` | Optional | Comma-separated fallback list |
| `MARKETAUX_API_TOKEN` | Optional | Live news; falls back to placeholder headlines |
| `FRED_API_KEY` | Optional | FRED-sourced US 10Y; falls back to `^TNX` via yfinance |
| `MANUAL_BUND_10Y` | Optional | Manual German 10Y yield override e.g. `2.45` |
| `MANUAL_CH_10Y` | Optional | Manual Swiss 10Y yield override |
| `IRAN_WAR_START_DATE` | Optional | Default: `2026-02-28` |
| `IRAN_CEASEFIRE_DATE` | Optional | Default: `2026-04-07` |

**Local:** create `.env` with `KEY=value` pairs.
**Streamlit Cloud:** add keys under *App settings → Secrets* in TOML format.

## Running locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Deploying to Streamlit Cloud

1. Push `app.py`, `requirements.txt`, `README.md` to a GitHub repo.
2. [share.streamlit.io](https://share.streamlit.io) → New app → select repo.
3. *Advanced settings → Secrets*:
   ```toml
   GEMINI_API_KEY = "your-key"
   MARKETAUX_API_TOKEN = "your-token"
   FRED_API_KEY = "your-key"
   ```
4. Deploy. Open from any browser or iPhone. Add to Home Screen for app-like access.

## Architecture notes

- `render_combined_card()` — single Plotly figure per card. Text metrics are annotations in the top margin; sparkline is a line trace below. Coloured border via a `shapes` rect. This is the only reliable way to integrate a chart visually *inside* a card in Streamlit.
- `build_pdf()` — ReportLab one-page landscape; chart rendered via kaleido PNG export.
- `build_base_state()` / `add_render_outputs()` — data fetching separated from rendering so snapshots can be frozen and replayed.
