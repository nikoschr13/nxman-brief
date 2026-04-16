import os
import json
import time
from io import BytesIO
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
import yfinance as yf
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh
from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle

load_dotenv()


def get_secret(key: str, default: str = "") -> str:
    """Read from Streamlit secrets (cloud) then .env / environment."""
    try:
        val = st.secrets.get(key)
        if val is not None:
            return str(val).strip()
    except Exception:
        pass
    return os.getenv(key, default).strip()


PRIMARY = "#103B73"
SECONDARY = "#1E88E5"
LIGHT = "#F3F8FE"
TEXT = "#14304D"

ZURICH_TZ = ZoneInfo("Europe/Zurich")
SNAPSHOT_HOUR = 8
SNAPSHOT_DIR = Path("snapshots")
SNAPSHOT_DIR.mkdir(exist_ok=True)

IRAN_WAR_START_DATE = get_secret("IRAN_WAR_START_DATE", "2026-02-28")
IRAN_CEASEFIRE_DATE = get_secret("IRAN_CEASEFIRE_DATE", "2026-04-07")

MARKETAUX_API_TOKEN = get_secret("MARKETAUX_API_TOKEN")
FRED_API_KEY = get_secret("FRED_API_KEY")
GEMINI_API_KEY = get_secret("GEMINI_API_KEY")
GEMINI_MODEL = get_secret("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_FALLBACK_MODELS = [
    m.strip()
    for m in get_secret("GEMINI_FALLBACK_MODELS", "gemini-2.5-flash,gemini-1.5-flash").split(",")
    if m.strip()
]
MANUAL_BUND_10Y = get_secret("MANUAL_BUND_10Y")
MANUAL_CH_10Y   = get_secret("MANUAL_CH_10Y")

# GitHub Gist persistence for morning snapshots
GITHUB_TOKEN   = get_secret("GITHUB_TOKEN")
GITHUB_GIST_ID = get_secret("GITHUB_GIST_ID")
GIST_FILENAME  = "nxman_snapshots.json"

ASSETS = [
    ("equities", "sp500",      "S&P 500",               "S&P 500 large-cap US equity index",                  "^GSPC",           True),
    ("equities", "nasdaq100",  "Nasdaq 100",             "Top 100 non-financial Nasdaq companies",             "^NDX",            True),
    ("equities", "stoxx600",   "Stoxx Europe 600",       "Broad European equity benchmark",                    "^STOXX",          True),
    ("equities", "msci_world", "MSCI World",             "Global developed-market equity index",               "^990100-USD-STRD",True),  # fallback: ^MXWO, URTH
    ("equities", "msci_em",    "MSCI Emerging Markets",  "Emerging-market equity index",                       "^891800-USD-STRD",False),
    ("equities", "nikkei225",  "Nikkei 225",             "Japanese large-cap equity index",                    "^N225",           False),
    ("equities", "smi",        "SMI (Switzerland)",      "Swiss large-cap equity index",                       "^SSMI",           True),
    ("fx",       "eurusd",     "EUR/USD",                "Euros per US dollar — key global FX pair",           "EURUSD=X",        False),
    ("fx",       "usdchf",     "USD/CHF",                "US dollars per Swiss franc",                         "USDCHF=X",        False),
    ("fx",       "eurchf",     "EUR/CHF",                "Euros per Swiss franc — key for Swiss investors",    "EURCHF=X",        True),
    ("fx",       "dxy",        "DXY (USD Index)",        "US Dollar basket index vs major currencies",         "DX-Y.NYB",        False),
    ("commodities","gold",     "Gold",                   "Gold spot / front-month futures",                    "GC=F",            True),
    ("commodities","silver",   "Silver",                 "Silver spot / front-month futures",                  "SI=F",            False),
    ("commodities","wti",      "WTI Crude",              "WTI crude oil front-month futures",                  "CL=F",            True),
    ("commodities","brent",    "Brent Crude",            "Brent crude oil front-month futures",                "BZ=F",            False),
    ("commodities","copper",   "Copper",                 "Copper front-month futures",                         "HG=F",            False),
    ("alternatives","bitcoin", "Bitcoin",                "Largest cryptocurrency by market cap",               "BTC-USD",         True),
    ("alternatives","ethereum","Ethereum",               "Second-largest cryptocurrency by market cap",        "ETH-USD",         False),
    ("sentiment",  "vix",      "VIX",                    "CBOE Volatility Index — S&P 500 implied vol, fear gauge", "^VIX",       False),
]

# Multiple tickers to try for MSCI World (in order of preference)
MSCI_WORLD_TICKERS = ["^990100-USD-STRD", "^MXWO", "URTH"]

RATES = [
    ("rates", "us10y", "US 10Y Treasury", "Yield on 10-year US government bonds; key global benchmark", "DGS10", True),
    ("rates", "bund10y", "German 10Y Bund", "Yield on 10-year German government bonds; core euro area benchmark", None, False),
    ("rates", "ch10y", "Swiss 10Y Government Bond", "Yield on 10-year Swiss government bonds; Swiss franc benchmark", None, False),
]

INDICATOR_STRIP = [
    {"type": "asset", "key": "sp500",     "label": "S&P 500"},
    {"type": "asset", "key": "nasdaq100", "label": "Nasdaq 100"},
    {"type": "asset", "key": "stoxx600",  "label": "Stoxx Europe 600"},
    {"type": "asset", "key": "msci_world","label": "MSCI World"},
    {"type": "asset", "key": "smi",       "label": "SMI"},
    {"type": "asset", "key": "nikkei225", "label": "Nikkei 225"},
    {"type": "fear",  "key": "vix",       "label": "VIX (Fear Gauge)"},
    {"type": "asset", "key": "dxy",       "label": "DXY (USD Index)"},
]

ASSET_CLASS_STRIP = [
    {"type": "asset", "key": "msci_world",   "label": "Global Equities"},
    {"type": "asset", "key": "global_bonds", "label": "Global Bonds"},
    {"type": "asset", "key": "usd_bonds",    "label": "USD Bonds"},
    {"type": "asset", "key": "eur_bonds",    "label": "EUR Bonds"},
    {"type": "asset", "key": "gold",         "label": "Gold"},
    {"type": "asset", "key": "bitcoin",      "label": "Bitcoin"},
    {"type": "asset", "key": "wti",          "label": "WTI Oil"},
    {"type": "yield", "key": "us10y",        "label": "US 10Y Yield"},
]

FX_STRIP = [
    {"type": "asset", "key": "eurusd",  "label": "EUR/USD"},
    {"type": "asset", "key": "usdchf",  "label": "USD/CHF"},
    {"type": "asset", "key": "eurchf",  "label": "EUR/CHF"},
    {"type": "asset", "key": "dxy",     "label": "DXY (USD Index)"},
    {"type": "asset", "key": "gold",    "label": "Gold"},
    {"type": "asset", "key": "wti",     "label": "WTI Oil"},
]

# ── Macro events calendar (update as needed) ─────────────────────────────────
MACRO_EVENTS = [
    {"date": "2026-04-17", "event": "ECB Rate Decision",       "category": "Central Banks"},
    {"date": "2026-05-02", "event": "US Jobs Report (NFP)",    "category": "US Data"},
    {"date": "2026-05-07", "event": "FOMC Rate Decision",      "category": "Central Banks"},
    {"date": "2026-05-13", "event": "US CPI Release",          "category": "US Data"},
    {"date": "2026-06-05", "event": "ECB Rate Decision",       "category": "Central Banks"},
    {"date": "2026-06-05", "event": "US Jobs Report (NFP)",    "category": "US Data"},
    {"date": "2026-06-11", "event": "US CPI Release",          "category": "US Data"},
    {"date": "2026-06-18", "event": "FOMC Rate Decision",      "category": "Central Banks"},
    {"date": "2026-07-03", "event": "US Jobs Report (NFP)",    "category": "US Data"},
    {"date": "2026-07-15", "event": "US CPI Release",          "category": "US Data"},
    {"date": "2026-07-24", "event": "ECB Rate Decision",       "category": "Central Banks"},
    {"date": "2026-07-30", "event": "FOMC Rate Decision",      "category": "Central Banks"},
    {"date": "2026-08-05", "event": "US Jobs Report (NFP)",    "category": "US Data"},
    {"date": "2026-09-11", "event": "ECB Rate Decision",       "category": "Central Banks"},
    {"date": "2026-09-17", "event": "FOMC Rate Decision",      "category": "Central Banks"},
]

# ── News category style map ───────────────────────────────────────────────────
CATEGORY_STYLE = {
    "Macro / Rates":  {"bg": "#DBEAFE", "text": "#1E3A5F", "border": "#93C5FD"},
    "Geopolitics":    {"bg": "#FEE2E2", "text": "#7F1D1D", "border": "#FCA5A5"},
    "Equities":       {"bg": "#DCFCE7", "text": "#14532D", "border": "#86EFAC"},
    "Commodities":    {"bg": "#FEF3C7", "text": "#78350F", "border": "#FCD34D"},
    "Crypto":         {"bg": "#EDE9FE", "text": "#4C1D95", "border": "#C4B5FD"},
    "Other":          {"bg": "#F3F4F6", "text": "#374151", "border": "#D1D5DB"},
}


st.set_page_config(page_title="Daily Market Brief", layout="wide")

st.markdown(
    """
<style>
.stApp { background: #F3F8FE; }
.block-container { padding-top: 0.6rem !important; padding-left: 1rem !important;
                   padding-right: 1rem !important; max-width: 100% !important; }
.hero { background: linear-gradient(90deg, #103B73, #1E88E5); color: white;
        padding: 14px 18px; border-radius: 14px; margin-bottom: 10px;
        box-shadow: 0 4px 14px rgba(16,59,115,.14); }
.hero h1 { margin: 0; font-size: clamp(16px, 4vw, 22px); }
.hero-sub { opacity: .85; margin-top: 3px; font-size: clamp(11px, 2.5vw, 13px); }
.section-card { background: white; border-radius: 14px; padding: 12px 14px;
                box-shadow: 0 3px 12px rgba(16,59,115,.07); margin-bottom: 8px; }
@media (max-width: 768px) {
    .block-container { padding-left: 0.4rem !important; padding-right: 0.4rem !important; }
    section[data-testid="stSidebar"] { min-width: 240px !important; }
}
div[data-testid="stMetric"] { background: transparent !important;
                               padding: 0 !important; border: 0 !important; }
details summary { font-size: 14px !important; padding: 6px 0 !important; }
</style>
""",
    unsafe_allow_html=True,
)

st.markdown(
    "<div class='hero'>"
    "<h1>Daily Market Brief</h1>"
    "<div class='hero-sub'>Cross-asset · News · Morning snapshot mode</div>"
    "</div>",
    unsafe_allow_html=True,
)


def now_zurich():
    return datetime.now(ZURICH_TZ)


def snapshot_path_for_date(d):
    return SNAPSHOT_DIR / f"{d}.json"


def pct_change(current, previous):
    if previous in (None, 0) or pd.isna(previous) or pd.isna(current):
        return None
    return ((current / previous) - 1.0) * 100.0


def bps_change(current, previous):
    if previous is None or pd.isna(previous) or pd.isna(current):
        return None
    return (float(current) - float(previous)) * 100.0


def value_on_or_before(series, target_date):
    eligible = series[series.index <= target_date]
    return None if eligible.empty else float(eligible.iloc[-1])


def nice(df):
    out = df.copy()
    for col in ["level", "d1", "wtd", "mtd", "ytd"]:
        if col in out.columns:
            out[col] = out[col].apply(lambda x: None if pd.isna(x) else round(float(x), 2))
    return out


def compact_table(df):
    cols = [c for c in ["label", "level", "d1", "wtd", "mtd", "ytd"] if c in df.columns]
    out = nice(df[cols].copy())
    return out.fillna("N/A")


def definitions_table(df):
    cols = [c for c in ["label", "description"] if c in df.columns]
    return df[cols].drop_duplicates().copy()


def short_url(u, max_len=40):
    if not u:
        return ""
    return u if len(u) <= max_len else u[: max_len - 3] + "..."


def fmt_pct(v):
    if v is None or pd.isna(v):
        return "N/A"
    return f"{float(v):+.2f}%"


def fmt_num(v):
    if v is None or pd.isna(v):
        return "N/A"
    return f"{float(v):,.2f}"


@st.cache_data(ttl=900)
def fetch_yf_series(ticker):
    df = yf.download(ticker, period="1y", interval="1d", progress=False, auto_adjust=False, threads=False)
    if df is None or df.empty:
        raise ValueError(ticker)
    series = df["Adj Close"] if "Adj Close" in df.columns else df["Close"]
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
    series = series.dropna()
    if series.empty:
        raise ValueError(ticker)
    return series


@st.cache_data(ttl=900)
def fetch_yf_series_with_fallback(tickers: list, label: str):
    """Try each ticker in order; return (series, ticker_used) or raise."""
    for t in tickers:
        try:
            s = fetch_yf_series(t)
            if len(s) >= 20:
                return s, t
        except Exception:
            continue
    raise ValueError(f"All tickers failed for {label}: {tickers}")


@st.cache_data(ttl=900)
def fetch_fred_series(series_id):
    if not FRED_API_KEY:
        raise ValueError("No FRED key")
    r = requests.get(
        "https://api.stlouisfed.org/fred/series/observations",
        params={"series_id": series_id, "api_key": FRED_API_KEY, "file_type": "json", "sort_order": "asc"},
        timeout=30,
    )
    r.raise_for_status()
    obs = [o for o in r.json().get("observations", []) if o.get("value") not in {".", None, ""}]
    if len(obs) < 2:
        raise ValueError(series_id)
    return pd.Series([float(o["value"]) for o in obs], index=pd.to_datetime([o["date"] for o in obs]))


def build_manual_rate_history(level_text):
    try:
        level = float(level_text)
    except Exception:
        return None
    today = pd.Timestamp.today().normalize()
    dates = pd.bdate_range(start=pd.Timestamp(today.year, 1, 1), end=today)
    return pd.Series([level] * len(dates), index=dates)


@st.cache_data(ttl=900)
def load_news_marketaux(count):
    if not MARKETAUX_API_TOKEN:
        return pd.DataFrame()

    try:
        published_after = (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%S")
        r = requests.get(
            "https://api.marketaux.com/v1/news/all",
            params={
                "api_token": MARKETAUX_API_TOKEN,
                "language": "en",
                "limit": max(count * 6, 30),
                "published_after": published_after,
            },
            timeout=30,
        )
        r.raise_for_status()

        rows = []
        for item in r.json().get("data", []):
            rows.append(
                {
                    "headline": item.get("title") or "",
                    "source": item.get("source") or "",
                    "published_at": item.get("published_at") or "",
                    "url": item.get("url") or "",
                    "why_it_matters": item.get("description") or "",
                    "provider": "Marketaux",
                }
            )
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=900)
def load_news(count):
    placeholder_items = [
        {"headline": "Oil remains central as geopolitical tension stays elevated",       "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Higher oil prices support inflation concerns and affect rates, equities and currencies.", "provider": "Placeholder"},
        {"headline": "Markets remain sensitive to higher-for-longer rate expectations",   "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "If rates stay elevated, bonds and equities may both face valuation pressure.",            "provider": "Placeholder"},
        {"headline": "Risk sentiment mixed across regions",                               "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Regional leadership remains uneven, which supports diversification.",                     "provider": "Placeholder"},
        {"headline": "Dollar strength weighs on emerging-market assets",                  "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "A strong USD tightens financial conditions in EM economies.",                             "provider": "Placeholder"},
        {"headline": "Gold holds near highs amid central bank demand",                    "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Central bank buying underpins gold as a reserve diversification tool.",                   "provider": "Placeholder"},
        {"headline": "China stimulus expectations support commodity demand",              "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Chinese policy stimulus could lift industrial metals and energy prices.",                  "provider": "Placeholder"},
        {"headline": "European equities outperform on valuation re-rating",              "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Cheaper valuations attract flows when US growth expectations moderate.",                   "provider": "Placeholder"},
        {"headline": "Credit spreads stable; no systemic stress signals",                "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Tight spreads suggest credit markets are not pricing in near-term recession risk.",        "provider": "Placeholder"},
        {"headline": "Crypto volatility elevated; Bitcoin tests key resistance",         "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Bitcoin remains a high-beta risk asset, often amplifying broader sentiment moves.",       "provider": "Placeholder"},
        {"headline": "Swiss franc holds safe-haven bid; EUR/CHF under pressure",         "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "CHF strength can compress Swiss equity earnings and affects EUR-denominated portfolios.", "provider": "Placeholder"},
    ]
    placeholder_df = pd.DataFrame(placeholder_items[:max(count, 10)])
    placeholder_df["category"] = "Other"

    if not MARKETAUX_API_TOKEN:
        return placeholder_df, {
            "live_news": False,
            "article_count": 0,
            "url_count": 0,
            "reason": "No MARKETAUX_API_TOKEN",
        }

    df = load_news_marketaux(count)
    if df.empty:
        return placeholder_df, {
            "live_news": False,
            "article_count": 0,
            "url_count": 0,
            "reason": "Marketaux returned no usable articles",
        }

    df["headline_key"] = df["headline"].fillna("").str.lower().str.strip()
    df = df.drop_duplicates(subset=["headline_key", "source", "url"]).copy()

    def classify(headline: str):
        h = (headline or "").lower()
        if any(k in h for k in ["fed", "ecb", "inflation", "treasury", "yield", "rates", "cpi", "ppi"]):
            return "Macro / Rates"
        if any(k in h for k in ["iran", "war", "ceasefire", "russia", "ukraine", "china", "tariff", "trade"]):
            return "Geopolitics"
        if any(k in h for k in ["oil", "gold", "copper", "crude", "brent", "wti", "commodity"]):
            return "Commodities"
        if any(k in h for k in ["bitcoin", "crypto", "ethereum"]):
            return "Crypto"
        if any(k in h for k in ["earnings", "stock", "shares", "equity", "nasdaq", "s&p", "dow"]):
            return "Equities"
        return "Other"

    def score_row(row):
        headline = (row.get("headline") or "").lower()
        score = 0
        for kw in [
            "fed", "ecb", "inflation", "yield", "treasury", "oil", "iran", "war",
            "ceasefire", "china", "tariff", "earnings", "economy", "rates",
            "dollar", "euro", "franc", "bitcoin", "gold"
        ]:
            if kw in headline:
                score += 2
        if row.get("url"):
            score += 1
        if row.get("source"):
            score += 1
        return score

    df["category"] = df["headline"].apply(classify)
    df["score"] = df.apply(score_row, axis=1)
    df = df.sort_values(by=["score", "published_at"], ascending=[False, False]).head(max(count, 10)).copy()
    df = df.drop(columns=["headline_key", "score"], errors="ignore")

    final_rows = []
    seen = set()
    # First pass: up to 3 per category to ensure variety
    for category in ["Macro / Rates", "Geopolitics", "Equities", "Commodities", "Crypto", "Other"]:
        sub = df[df["category"] == category].head(3)
        for _, row in sub.iterrows():
            hk = row["headline"]
            if hk not in seen:
                seen.add(hk)
                final_rows.append(row)
    # If still under count, fill from remaining scored articles
    remaining = df[~df["headline"].isin(seen)].head(count)
    for _, row in remaining.iterrows():
        if len(final_rows) >= count:
            break
        final_rows.append(row)

    final_df = pd.DataFrame(final_rows).head(count) if final_rows else df.head(count)

    return final_df, {
        "live_news": True,
        "article_count": len(final_df),
        "url_count": int(final_df["url"].fillna("").astype(str).str.len().gt(0).sum()),
        "reason": "Live Marketaux news ranked by relevance and topic coverage",
    }


def build_local_news_summary(news_df):
    if news_df is None or news_df.empty:
        return "No news headlines were available, so no summary could be generated."

    groups = {}
    for _, row in news_df.iterrows():
        cat = row.get("category", "Other")
        groups.setdefault(cat, []).append(row.get("headline", ""))

    ordered = []
    for cat in ["Macro / Rates", "Geopolitics", "Equities", "Commodities", "Crypto", "Other"]:
        if cat in groups:
            ordered.append(f"{cat}: " + "; ".join(groups[cat][:2]))
    return " | ".join(ordered[:4])


def try_gemini_model(model_name, payload):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
    return requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json={
            "contents": [{"parts": [{"text": payload}]}],
            "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json"},
        },
        timeout=45,
    )


def gemini_generate_json(payload):
    if not GEMINI_API_KEY:
        return None, "No GEMINI_API_KEY"

    models_to_try = [GEMINI_MODEL] + [m for m in GEMINI_FALLBACK_MODELS if m != GEMINI_MODEL]
    errors = []

    for model_name in models_to_try:
        for attempt in range(3):
            try:
                r = try_gemini_model(model_name, payload)

                if r.ok:
                    data = r.json()
                    candidates = data.get("candidates", [])
                    if not candidates:
                        errors.append(f"{model_name}: no candidates returned")
                        break

                    parts = candidates[0].get("content", {}).get("parts", [])
                    raw = "".join([p.get("text", "") for p in parts]).strip()
                    if not raw:
                        errors.append(f"{model_name}: empty response")
                        break

                    try:
                        parsed = json.loads(raw)
                        return parsed, f"Gemini response OK ({model_name})"
                    except Exception:
                        errors.append(f"{model_name}: invalid JSON | {raw[:120]}")
                        break

                else:
                    msg = f"{model_name}: HTTP {r.status_code} | {r.text[:160]}"
                    if r.status_code == 503 and attempt < 2:
                        time.sleep(2 + attempt)
                        continue
                    errors.append(msg)
                    break

            except Exception as e:
                msg = f"{model_name}: {type(e).__name__}: {str(e)[:120]}"
                if attempt < 2:
                    time.sleep(2 + attempt)
                    continue
                errors.append(msg)
                break

    return None, "Gemini failed: " + " || ".join(errors[:4])


def build_writing(news_df, snapshot, use_gemini):
    local_summary = build_local_news_summary(news_df)

    fallback = {
        "headline": "Mixed market tone as rates remain in focus",
        "subheadline": "Markets remain driven by a mix of rates, currencies, commodities and uneven regional equity performance.",
        "news_summary": local_summary,
        "what_matters": [
            "Rates remain central because higher yields usually pressure existing bond prices.",
            "Energy and geopolitics still matter because oil can affect inflation expectations and risk sentiment.",
            "Currencies remain important for CHF-based and EUR-linked investors.",
            "Cross-asset performance is mixed, so leadership should be monitored rather than assumed.",
        ],
        "news_bullets": [],
    }

    if not use_gemini:
        return fallback, {"gemini_used": False, "reason": "Checkbox off"}

    payload = json.dumps(
        {
            "instruction": (
                "Return strict JSON with keys: headline, subheadline, news_summary, what_matters, news_bullets. "
                "what_matters: exactly 4 concise bullet strings about key investment themes. "
                "news_bullets: 5 to 8 plain-English bullets summarising what happened since yesterday "
                "and what it means for markets. "
                "Each bullet must link the event to the market impact — like: "
                "'US-Iran nuclear talks progressed — equity markets rallied while Treasury yields fell as risk appetite improved' "
                "or 'Fed minutes signalled fewer cuts — bond prices fell as traders repriced rate expectations upward'. "
                "Be specific, factual, cause-and-effect. No jargon, no preamble."
            ),
            "headlines": news_df[["headline", "source", "category"]].fillna("").to_dict(orient="records") if news_df is not None and not news_df.empty else [],
            "market_snapshot": snapshot[["label", "group", "d1", "wtd", "ytd"]].fillna("").to_dict(orient="records")[:20],
        }
    )

    out, reason = gemini_generate_json(payload)
    if isinstance(out, dict) and isinstance(out.get("what_matters"), list) and len(out["what_matters"]) >= 4:
        return (
            {
                "headline":      out.get("headline")     or fallback["headline"],
                "subheadline":   out.get("subheadline")  or fallback["subheadline"],
                "news_summary":  out.get("news_summary") or fallback["news_summary"],
                "what_matters":  out["what_matters"][:4],
                "news_bullets":  out.get("news_bullets") or [],
                "article_angles": out.get("article_angles") or [],
            },
            {"gemini_used": True, "reason": reason},
        )

    return {**fallback, "news_bullets": [], "article_angles": []}, {"gemini_used": False, "reason": reason}


def build_bundle():
    history_frames = []
    chart_allowed_keys = []
    metas = []

    for group, key, label, desc, ticker, chart_include in ASSETS:
        metas.append((group, key, label, desc))
        if chart_include:
            chart_allowed_keys.append(key)
        try:
            # MSCI World: try multiple tickers in order
            if key == "msci_world":
                s, _ = fetch_yf_series_with_fallback(MSCI_WORLD_TICKERS, "MSCI World")
            else:
                s = fetch_yf_series(ticker)
            history_frames.append(pd.DataFrame({
                "date":        pd.to_datetime(s.index),
                "key":         key,
                "label":       label,
                "group":       group,
                "value":       s.values,
                "source_type": "live",
            }))
        except Exception:
            pass

    for group, key, label, desc, fred_series, chart_include in RATES:
        metas.append((group, key, label, desc))
        if chart_include:
            chart_allowed_keys.append(key)

        s = None
        if key == "us10y":
            try:
                s = fetch_fred_series("DGS10")
            except Exception:
                try:
                    s = fetch_yf_series("^TNX") / 10.0
                except Exception:
                    s = None
        elif key == "bund10y":
            s = build_manual_rate_history(MANUAL_BUND_10Y)
        elif key == "ch10y":
            s = build_manual_rate_history(MANUAL_CH_10Y)

        if s is not None:
            history_frames.append(
                pd.DataFrame(
                    {
                        "date": pd.to_datetime(s.index),
                        "key": key,
                        "label": label,
                        "group": group,
                        "value": s.values,
                        "source_type": "live" if key == "us10y" else "manual",
                    }
                )
            )

    bond_proxies = [
        ("global_bonds", "Global Bonds", "BNDW", "Global aggregate bond ETF proxy"),
        ("usd_bonds", "USD Bonds", "BND", "US aggregate bond ETF proxy"),
        ("eur_bonds", "EUR Bonds", "IEAG", "EUR investment-grade bond ETF proxy"),
    ]

    for key, label, ticker, desc in bond_proxies:
        try:
            s = fetch_yf_series(ticker)
            history_frames.append(
                pd.DataFrame(
                    {
                        "date": pd.to_datetime(s.index),
                        "key": key,
                        "label": label,
                        "group": "bonds",
                        "value": s.values,
                        "source_type": "live",
                    }
                )
            )
            metas.append(("bonds", key, label, desc))
        except Exception:
            metas.append(("bonds", key, label, desc))

    history = pd.concat(history_frames, ignore_index=True) if history_frames else pd.DataFrame(columns=["date", "key", "label", "group", "value", "source_type"])

    snapshot_rows = []
    today = pd.Timestamp.today().normalize()
    year_start = pd.Timestamp(today.year, 1, 1)
    month_start = pd.Timestamp(today.year, today.month, 1)
    week_start = today - pd.Timedelta(days=today.weekday())

    for group, key, label, desc in metas:
        g = history[history["key"] == key].sort_values("date")
        if g.empty:
            snapshot_rows.append(
                {
                    "group": group,
                    "key": key,
                    "label": label,
                    "description": desc,
                    "level": None,
                    "d1": None,
                    "wtd": None,
                    "mtd": None,
                    "ytd": None,
                }
            )
            continue

        series = pd.Series(g["value"].values, index=pd.to_datetime(g["date"]))
        latest = float(series.iloc[-1])
        prev = float(series.iloc[-2]) if len(series) >= 2 else None
        snapshot_rows.append(
            {
                "group": group,
                "key": key,
                "label": label,
                "description": desc,
                "level": latest,
                "d1": pct_change(latest, prev),
                "wtd": pct_change(latest, value_on_or_before(series, week_start)),
                "mtd": pct_change(latest, value_on_or_before(series, month_start)),
                "ytd": pct_change(latest, value_on_or_before(series, year_start)),
            }
        )

    return pd.DataFrame(snapshot_rows), history, chart_allowed_keys


def build_weekly_chart_df(history, allowed, include_crypto_flag, start_date=None):
    """Weekly-resampled YTD (or custom window) returns, always including today's latest close."""
    if history.empty:
        return pd.DataFrame(columns=["date", "key", "label", "group", "return_pct"])

    df = history.copy()
    df["date"] = pd.to_datetime(df["date"])
    max_date = df["date"].max()

    if start_date is None:
        window_start = pd.Timestamp(max_date.year, 1, 1)
    else:
        window_start = pd.Timestamp(start_date)

    df = df[(df["date"] >= window_start) & (df["key"].isin(allowed))]

    if not include_crypto_flag:
        df = df[df["group"] != "alternatives"]

    parts = []
    for key, g in df.groupby("key"):
        g = g.sort_values("date").set_index("date")
        weekly = g["value"].resample("W-FRI").last().dropna()
        if weekly.empty:
            continue

        # Always append today's latest close if it's newer than the last weekly point
        latest_daily = g["value"].dropna()
        if not latest_daily.empty:
            latest_ts = latest_daily.index[-1]
            if latest_ts > weekly.index[-1]:
                weekly.loc[latest_ts] = float(latest_daily.iloc[-1])
                weekly = weekly.sort_index()

        base = float(weekly.iloc[0])
        if base == 0:
            continue
        returns = ((weekly / base) - 1.0) * 100.0
        parts.append(pd.DataFrame({
            "date":       returns.index,
            "key":        key,
            "label":      g["label"].iloc[0],
            "group":      g["group"].iloc[0],
            "return_pct": returns.values,
        }))

    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(
        columns=["date", "key", "label", "group", "return_pct"]
    )


def pick_chart_of_day(history, news_df):
    """Ask Gemini to nominate the most interesting chart given:
    - The asset with the biggest z-score move vs its 30-day range
    - The most-mentioned keyword across today's headlines
    Returns dict: {key, label, reason, timeframe_days} or None.
    """
    if not GEMINI_API_KEY or history.empty:
        return None

    # Find biggest movers (z-score of latest value vs 30-day rolling)
    today = pd.Timestamp.today().normalize()
    movers = []
    for key, g in history.groupby("key"):
        g = g.sort_values("date").set_index("date")
        recent = g["value"].last("30D").dropna()
        if len(recent) < 5:
            continue
        mean, std = recent.mean(), recent.std()
        if std == 0:
            continue
        latest = float(recent.iloc[-1])
        z = abs((latest - mean) / std)
        lbl = g["label"].iloc[0] if "label" in g.columns else key
        movers.append({"key": key, "label": lbl, "zscore": round(z, 2), "latest": round(latest, 4)})

    movers = sorted(movers, key=lambda x: x["zscore"], reverse=True)[:6]

    # Count most-mentioned keywords in news
    kw_counts: dict = {}
    if news_df is not None and not news_df.empty:
        all_headlines = " ".join(news_df["headline"].fillna("").str.lower().tolist())
        for kw in ["iran", "fed", "ecb", "china", "tariff", "oil", "gold", "dollar",
                   "inflation", "rate", "war", "ceasefire", "bitcoin", "recession"]:
            cnt = all_headlines.count(kw)
            if cnt > 0:
                kw_counts[kw] = cnt
    top_kws = sorted(kw_counts.items(), key=lambda x: x[1], reverse=True)[:4]

    payload = json.dumps({
        "instruction": (
            "You are a financial analyst. Pick ONE chart of the day from the candidates below. "
            "Choose the most interesting for an investor to see right now — biggest unusual move, "
            "or most relevant to the top news themes. "
            "Return strict JSON only: {\"key\": \"<asset_key>\", \"label\": \"<asset_label>\", "
            "\"reason\": \"<one concise sentence explaining why this chart matters today>\", "
            "\"timeframe_days\": <30|60|90|180>}"
        ),
        "top_movers_by_zscore": movers,
        "top_news_keywords": [{"keyword": k, "mentions": v} for k, v in top_kws],
    })

    try:
        out, _ = gemini_generate_json(payload)
        if isinstance(out, dict) and out.get("key") and out.get("reason"):
            return out
    except Exception:
        pass

    # Fallback: just use the top mover
    if movers:
        m = movers[0]
        return {"key": m["key"], "label": m["label"],
                "reason": f"{m['label']} showing an unusual move today (z-score {m['zscore']:.1f}).",
                "timeframe_days": 60}
    return None


def render_chart_of_day(cotd, history):
    """Render the Chart of the Day: focused line chart of nominated asset."""
    if cotd is None:
        return
    key      = cotd.get("key", "")
    label    = cotd.get("label", key)
    reason   = cotd.get("reason", "")
    tf_days  = int(cotd.get("timeframe_days", 60))

    g = history[history["key"] == key].sort_values("date")
    if g.empty:
        st.caption(f"No data for chart of the day ({key})")
        return

    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=tf_days)
    g = g[g["date"] >= cutoff]
    if g.empty:
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=g["date"], y=g["value"],
        mode="lines",
        line=dict(width=2.5, color=PRIMARY),
        fill="tozeroy",
        fillcolor="rgba(16,59,115,0.07)",
        hovertemplate="%{x|%d %b %Y}<br><b>%{y:.2f}</b><extra></extra>",
        name=label,
    ))
    fig.add_hline(y=float(g["value"].iloc[0]), line_dash="dot",
                  line_color="#94A3B8", line_width=1,
                  annotation_text=f"Start ({tf_days}d ago)",
                  annotation_position="bottom left",
                  annotation_font_size=9)
    fig.update_layout(
        height=280,
        margin=dict(l=10, r=10, t=10, b=30),
        plot_bgcolor="white", paper_bgcolor="white",
        showlegend=False,
        xaxis=dict(showgrid=False, tickformat="%d %b", tickfont=dict(size=9)),
        yaxis=dict(showgrid=True, gridcolor="#F0F4F8", tickfont=dict(size=9)),
    )
    add_event_marker(fig, IRAN_WAR_START_DATE,   "Iran conflict", "#C62828", 0.10, 9)
    add_event_marker(fig, IRAN_CEASEFIRE_DATE,   "Ceasefire",     "#12B76A", 0.08, 9)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False},
                    key="chart_of_day_fig")
    st.caption(f"🔍 {reason}")


def add_event_marker(fig, event_date, label, line_color, fill_opacity=0.10, font_size=11):
    if not event_date:
        return
    dt = pd.Timestamp(event_date)
    fig.add_vrect(
        x0=dt - pd.Timedelta(days=1),
        x1=dt + pd.Timedelta(days=1),
        fillcolor=line_color,
        opacity=fill_opacity,
        line_width=0,
    )
    fig.add_vline(x=dt, line_dash="dash", line_color=line_color, line_width=2)
    fig.add_annotation(
        x=dt,
        y=1.02,
        yref="paper",
        text=label,
        showarrow=False,
        bgcolor="white",
        bordercolor=line_color,
        borderwidth=1,
        font=dict(size=font_size, color=line_color),
    )


def pdf_chart_subset(weekly_df):
    allowed = {"sp500", "stoxx600", "msci_world", "us10y", "gold", "wti", "global_bonds"}
    return weekly_df[weekly_df["key"].isin(allowed)].copy() if not weekly_df.empty else weekly_df


def render_combined_card(item, snapshot_row, history, chart_key):
    """Single Plotly figure per card: coloured border + metric annotations + sparkline.

    Layout: height=265, margin_t=118, margin_b=24
      plot-area top  = (265-118)/265 = 0.555  → annotations sit at y > 0.56
      plot-area bot  = 24/265        = 0.091
    3 cards per row gives ~33 % screen width — enough breathing room.
    """
    H, MT, MB, ML, MR = 265, 118, 24, 10, 8

    if snapshot_row.empty:
        fig = go.Figure()
        fig.update_layout(
            height=H, margin=dict(l=ML, r=MR, t=MT, b=MB),
            plot_bgcolor="#F8FAFC", paper_bgcolor="#F8FAFC",
            showlegend=False, xaxis=dict(visible=False), yaxis=dict(visible=False),
            annotations=[dict(x=0.5, y=0.75, xref="paper", yref="paper",
                              text=f"<b>{item['label']}</b><br><span style='color:#9AA8B7'>No data</span>",
                              font=dict(size=11, color="#475467"), showarrow=False)],
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=chart_key)
        return

    r = snapshot_row.iloc[0]
    card_type = item.get("type", "asset")

    # ── Compute display values ────────────────────────────────────────────────
    if card_type == "yield":
        level = r["level"]
        hd1, hytd = r["d1"], r["ytd"]
        prev_l = level / (1 + hd1  / 100) if level is not None and hd1  not in (None, 0) and not pd.isna(hd1)  else None
        ytd_l  = level / (1 + hytd / 100) if level is not None and hytd not in (None, 0) and not pd.isna(hytd) else None
        d1_bps  = bps_change(level, prev_l) if prev_l is not None else None
        ytd_bps = bps_change(level, ytd_l)  if ytd_l  is not None else None
        value_str = "N/A" if level is None or pd.isna(level) else f"{float(level):.2f}%"
        d1_str    = "N/A" if d1_bps  is None else f"{d1_bps:+.1f} bps"
        ytd_str   = "N/A" if ytd_bps is None else f"{ytd_bps:+.1f} bps"
        move = d1_bps
        pos_col, neg_col = ("#F04438", "#FFF5F5"), ("#12B76A", "#F0FDF4")   # yield: up = bad

    elif card_type == "fear":   # VIX — up = bad (inverted)
        d1 = r["d1"]
        value_str = fmt_num(r["level"])
        d1_str    = fmt_pct(d1)
        ytd_str   = fmt_pct(r["ytd"])
        move = d1
        pos_col, neg_col = ("#F04438", "#FFF5F5"), ("#12B76A", "#F0FDF4")

    else:                       # regular asset
        d1 = r["d1"]
        value_str = fmt_num(r["level"])
        d1_str    = fmt_pct(d1)
        ytd_str   = fmt_pct(r["ytd"])
        move = d1
        pos_col, neg_col = ("#12B76A", "#F0FDF4"), ("#F04438", "#FFF5F5")

    if move is not None and not pd.isna(move) and move > 0:
        accent, bg = pos_col
    elif move is not None and not pd.isna(move) and move < 0:
        accent, bg = neg_col
    else:
        accent, bg = "#94A3B8", "#F8FAFC"

    line_color = accent if accent != "#94A3B8" else PRIMARY

    # Optional VIX note
    extra = ""
    if card_type == "fear" and r["level"] is not None and not pd.isna(r["level"]):
        v = float(r["level"])
        extra = "  ⚠ High" if v >= 30 else ("  Elevated" if v >= 20 else "  Calm")

    # Sparkline
    g = history[history["key"] == item["key"]].sort_values("date").tail(30)

    fig = go.Figure()
    if not g.empty:
        fig.add_trace(go.Scatter(
            x=g["date"], y=g["value"],
            mode="lines",
            line=dict(width=2, color=line_color),
            hovertemplate="%{x|%d %b}<br>%{y:.2f}<extra></extra>",
        ))

    lbl = item["label"][:24] + ("…" if len(item["label"]) > 24 else "")

    fig.update_layout(
        height=H,
        margin=dict(l=ML, r=MR, t=MT, b=MB),
        plot_bgcolor=bg,
        paper_bgcolor=bg,
        showlegend=False,
        xaxis=dict(
            showgrid=False, showline=False,
            tickformat="%d %b", tickfont=dict(size=8, color="#9AA8B7"),
            nticks=4, showticklabels=True, automargin=True,
        ),
        yaxis=dict(
            showgrid=False, showline=False,
            showticklabels=False,   # hide y-axis numbers — sparkline is for shape only
            automargin=False,
        ),
        # All annotation y values > 0.56 (above plot area top at 0.555)
        annotations=[
            dict(x=0.04, y=0.98, xref="paper", yref="paper",
                 xanchor="left", yanchor="top",
                 text=f"<b>{lbl}</b>",
                 font=dict(size=10, color="#475467"), showarrow=False),
            dict(x=0.04, y=0.87, xref="paper", yref="paper",
                 xanchor="left", yanchor="top",
                 text=f"<b>{value_str}</b>{extra}",
                 font=dict(size=21, color="#0F2D52"), showarrow=False),
            dict(x=0.04, y=0.71, xref="paper", yref="paper",
                 xanchor="left", yanchor="top",
                 text=f"<b>1D</b> {d1_str}",
                 font=dict(size=11, color="#344054"), showarrow=False),
            dict(x=0.04, y=0.61, xref="paper", yref="paper",
                 xanchor="left", yanchor="top",
                 text=f"YTD  {ytd_str}",
                 font=dict(size=10, color="#667085"), showarrow=False),
        ],
        shapes=[dict(
            type="rect", xref="paper", yref="paper",
            x0=0, y0=0, x1=1, y1=1,
            line=dict(color=accent, width=2),
            fillcolor="rgba(0,0,0,0)", layer="above",
        )],
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=chart_key)



def render_ticker_strip(snapshot):
    """One-line compact ticker: name + value + coloured daily change for 10 key instruments."""
    KEYS = [
        ("sp500",     "S&P 500",   "asset"),
        ("nasdaq100", "Nasdaq",    "asset"),
        ("stoxx600",  "Stoxx 600", "asset"),
        ("smi",       "SMI",       "asset"),
        ("gold",      "Gold",      "asset"),
        ("wti",       "Oil",       "asset"),
        ("us10y",     "US 10Y",    "yield"),
        ("vix",       "VIX",       "fear"),
        ("bitcoin",   "Bitcoin",   "asset"),
        ("eurchf",    "EUR/CHF",   "asset"),
    ]
    cells = []
    for key, label, typ in KEYS:
        row = snapshot[snapshot["key"] == key]
        if row.empty:
            cells.append(
                f"<td style='padding:4px 10px;text-align:center;border-right:1px solid #E4EDF6;'>"
                f"<div style='font-size:9px;color:#9AA8B7;font-weight:600;'>{label}</div>"
                f"<div style='font-size:13px;color:#9AA8B7;'>N/A</div></td>"
            )
            continue
        r = row.iloc[0]
        level, d1 = r["level"], r["d1"]

        if typ == "yield":
            prev = level / (1 + d1 / 100) if level is not None and d1 not in (None, 0) and not pd.isna(d1) else None
            d1v  = bps_change(level, prev) if prev is not None else None
            val_str   = f"{float(level):.2f}%" if level is not None else "N/A"
            delta_str = f"{d1v:+.0f} bps" if d1v is not None else "N/A"
            up_bad = True; move = d1v
        elif typ == "fear":
            val_str   = fmt_num(level)
            delta_str = fmt_pct(d1)
            up_bad = True; move = d1
        else:
            val_str   = fmt_num(level)
            delta_str = fmt_pct(d1)
            up_bad = False; move = d1

        if move is not None and not pd.isna(move):
            pos = (move > 0 and not up_bad) or (move < 0 and up_bad)
            neg = (move < 0 and not up_bad) or (move > 0 and up_bad)
            col = "#16A34A" if pos else "#DC2626" if neg else "#64748B"
        else:
            col = "#64748B"

        cells.append(
            f"<td style='padding:5px 10px;text-align:center;border-right:1px solid #E4EDF6;'>"
            f"<div style='font-size:9px;color:#64748B;font-weight:600;white-space:nowrap;'>{label}</div>"
            f"<div style='font-size:14px;font-weight:800;color:#0F2D52;line-height:1.2;'>{val_str}</div>"
            f"<div style='font-size:11px;font-weight:700;color:{col};'>{delta_str}</div>"
            f"</td>"
        )

    st.markdown(
        "<div style='background:white;border:1px solid #D6E4F2;border-radius:12px;"
        "overflow-x:auto;-webkit-overflow-scrolling:touch;margin-bottom:10px;'>"
        "<table style='width:100%;min-width:600px;border-collapse:collapse;'><tr>"
        + "".join(cells) +
        "</tr></table></div>",
        unsafe_allow_html=True,
    )


def render_card_strip(snapshot, history, strip, title, caption, strip_name):
    """Render a row of unified Plotly cards - each card contains metrics + sparkline."""
    st.subheader(title)
    st.caption(caption)

    rows = [strip[i:i + 3] for i in range(0, len(strip), 3)]
    for row_idx, block in enumerate(rows):
        cols = st.columns(len(block), gap="small")
        for col_idx, (col, item) in enumerate(zip(cols, block)):
            key = item["key"]
            row = snapshot[snapshot["key"] == key]
            with col:
                render_combined_card(
                    item, row, history,
                    chart_key=f"card_{strip_name}_{row_idx}_{col_idx}_{key}",
                )


def render_macro_calendar():
    """Show the next upcoming macro events from the hardcoded MACRO_EVENTS list."""
    today = pd.Timestamp.today().normalize()
    upcoming = [e for e in MACRO_EVENTS if pd.Timestamp(e["date"]) >= today][:7]

    if not upcoming:
        st.caption("No upcoming events in the calendar.")
        return

    CAT_COLOR = {
        "Central Banks": ("#1E3A5F", "#DBEAFE"),
        "US Data":       ("#14532D", "#DCFCE7"),
        "EU Data":       ("#78350F", "#FEF3C7"),
    }

    cols = st.columns(len(upcoming))
    for col, ev in zip(cols, upcoming):
        dt = pd.Timestamp(ev["date"])
        days_away = (dt - today).days
        if days_away == 0:
            day_label = "TODAY"
            day_color = "#EF4444"
        elif days_away == 1:
            day_label = "Tomorrow"
            day_color = "#F97316"
        else:
            day_label = f"In {days_away}d"
            day_color = "#475467"

        cat = ev.get("category", "Other")
        text_c, bg_c = CAT_COLOR.get(cat, ("#374151", "#F3F4F6"))

        with col:
            st.markdown(
                f"""
                <div style='background:{bg_c};border:1px solid {text_c}33;border-radius:10px;
                            padding:8px 10px;text-align:center;'>
                    <div style='font-size:10px;font-weight:700;color:{text_c};margin-bottom:2px;'>{cat}</div>
                    <div style='font-size:11px;font-weight:800;color:#0F2D52;line-height:1.25;margin-bottom:4px;'>{ev["event"]}</div>
                    <div style='font-size:10px;color:#475467;'>{dt.strftime("%d %b %Y")}</div>
                    <div style='font-size:11px;font-weight:700;color:{day_color};margin-top:2px;'>{day_label}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_news_bullets(writing, news_df):
    """Simple plain-English bullet list: what happened + why markets moved.
    Uses Gemini news_bullets when available, falls back to ranked headlines."""

    bullets = writing.get("news_bullets") or []

    if bullets:
        for b in bullets:
            st.markdown(f"- {b}")
    else:
        # Fallback: ranked headlines with category prefix
        if news_df is None or news_df.empty:
            st.caption("No news available.")
            return
        for _, r in news_df.head(10).iterrows():
            cat = r.get("category", "")
            headline = r.get("headline", "")
            prefix = f"**{cat}** — " if cat and cat != "Other" else ""
            st.markdown(f"- {prefix}{headline}")


def build_pdf(title, chart_png, equities_df, rates_df, commodities_df, metrics, writing, news_df, status):
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        rightMargin=0.45 * cm,
        leftMargin=0.45 * cm,
        topMargin=0.45 * cm,
        bottomMargin=0.4 * cm,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("title", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=17, textColor=colors.white, leading=18)
    strap = ParagraphStyle("strap", parent=styles["BodyText"], fontName="Helvetica", fontSize=7.2, leading=8.4, textColor=colors.white)
    h = ParagraphStyle("h", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=8.4, textColor=colors.HexColor(PRIMARY), spaceAfter=2, leading=9.5)
    body = ParagraphStyle("body", parent=styles["BodyText"], fontName="Helvetica", fontSize=6.5, leading=7.8, textColor=colors.HexColor(TEXT))
    body_small = ParagraphStyle("body_small", parent=body, fontSize=6.1, leading=7.1)

    def clean_df_for_pdf(df):
        out = df.copy()
        for c in out.columns:
            if c in ["level", "d1", "wtd", "mtd", "ytd"]:
                if c == "level":
                    out[c] = out[c].apply(fmt_num)
                else:
                    out[c] = out[c].apply(lambda x: "N/A" if x is None or pd.isna(x) else f"{float(x):.2f}%")
            else:
                out[c] = out[c].astype(str)
        return out

    def shorten_text(s, max_len=44):
        s = "" if s is None else str(s)
        return s if len(s) <= max_len else s[: max_len - 3] + "..."

    def pct_cell(v):
        return "N/A" if v is None or pd.isna(v) else f"{float(v):.2f}%"

    story = []

    header = Table(
        [
            [Paragraph(title, title_style), Paragraph(datetime.now().strftime("%A, %d %B %Y"), strap)],
            [Paragraph("Daily Market Brief", strap), Paragraph("", strap)],
        ],
        colWidths=[22.8 * cm, 5.0 * cm],
        rowHeights=[0.62 * cm, 0.28 * cm],
    )
    header.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(PRIMARY)),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("ALIGN", (1, 0), (1, 0), "RIGHT"),
            ]
        )
    )
    story += [header, Spacer(1, 0.05 * cm)]

    status_line = f"Gemini: {'ON' if status['gemini_used'] else 'OFF'} | Live news: {'ON' if status['live_news'] else 'OFF'} | Articles: {status['article_count']} | URLs: {status['url_count']}"
    status_tbl = Table([[Paragraph(status_line, body_small)]], colWidths=[27.8 * cm])
    status_tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(LIGHT)),
                ("BOX", (0, 0), (-1, -1), 0.25, colors.HexColor("#C9DCEE")),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    story += [status_tbl, Spacer(1, 0.05 * cm)]

    summary_text = (
        f"<b>Today in one line</b><br/><br/>"
        f"<font size='10'><b>{writing['headline']}</b></font><br/><br/>"
        f"{writing['subheadline']}"
    )
    summary = Table([[Paragraph(summary_text, body)]], colWidths=[10.4 * cm])
    summary.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.white),
                ("BOX", (0, 0), (-1, -1), 0.30, colors.HexColor("#D6E4F2")),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )

    metric_cells = [
        Paragraph(f"<b>Global eq. YTD</b><br/>{pct_cell(metrics.get('global_equities_ytd'))}", body_small),
        Paragraph(f"<b>Global bonds YTD</b><br/>{pct_cell(metrics.get('global_bonds_ytd'))}", body_small),
        Paragraph(f"<b>USD bonds YTD</b><br/>{pct_cell(metrics.get('usd_bonds_ytd'))}", body_small),
        Paragraph(f"<b>EUR bonds YTD</b><br/>{pct_cell(metrics.get('eur_bonds_ytd'))}", body_small),
    ]
    metrics_tbl = Table([metric_cells], colWidths=[2.55 * cm] * 4)
    metrics_tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(LIGHT)),
                ("BOX", (0, 0), (-1, -1), 0.30, colors.HexColor("#C9DCEE")),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#C9DCEE")),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    left_col = Table([[summary], [Spacer(1, 0.04 * cm)], [metrics_tbl]], colWidths=[10.4 * cm])

    # What Matters bullets
    wm_rows = [[Paragraph(f"• {x}", body_small)] for x in writing["what_matters"][:4]]
    what_tbl = Table(wm_rows, colWidths=[7.0 * cm])
    what_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0),(-1,-1), colors.white),
        ("BOX",        (0,0),(-1,-1), 0.30, colors.HexColor("#D6E4F2")),
        ("LEFTPADDING",(0,0),(-1,-1), 5), ("RIGHTPADDING",(0,0),(-1,-1), 5),
        ("TOPPADDING", (0,0),(-1,-1), 3), ("BOTTOMPADDING",(0,0),(-1,-1), 3),
    ]))

    # News bullets (Gemini or fallback)
    bullets = writing.get("news_bullets") or []
    if not bullets:
        bullets = [r.get("headline","") for _, r in news_df.head(5).iterrows()] if not news_df.empty else ["No headlines available"]
    bullet_rows = [[Paragraph(f"→ {b}", body_small)] for b in bullets[:6]]
    news_bul_tbl = Table(bullet_rows, colWidths=[7.0 * cm])
    news_bul_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0),(-1,-1), colors.white),
        ("BOX",        (0,0),(-1,-1), 0.30, colors.HexColor("#D6E4F2")),
        ("LEFTPADDING",(0,0),(-1,-1), 5), ("RIGHTPADDING",(0,0),(-1,-1), 5),
        ("TOPPADDING", (0,0),(-1,-1), 2), ("BOTTOMPADDING",(0,0),(-1,-1), 2),
    ]))

    mid_col = Table(
        [[Paragraph("What Matters", h)], [what_tbl],
         [Spacer(1, 0.03 * cm)],
         [Paragraph("What's Moving Markets", h)], [news_bul_tbl]],
        colWidths=[7.2 * cm],
    )

    chart_box = Table(
        [[Image(BytesIO(chart_png), width=9.0 * cm, height=4.9 * cm)]] if chart_png else [[Paragraph("No chart available", body)]],
        colWidths=[9.2 * cm],
    )
    chart_box.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.white),
                ("BOX", (0, 0), (-1, -1), 0.30, colors.HexColor("#D6E4F2")),
                ("LEFTPADDING", (0, 0), (-1, -1), 3),
                ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )

    story += [Table([[left_col, mid_col, chart_box]], colWidths=[10.5 * cm, 7.3 * cm, 9.3 * cm]), Spacer(1, 0.06 * cm)]

    def styled_table(df, widths, font_size=6.0, header_size=6.5):
        df2 = clean_df_for_pdf(df)
        if "label" in df2.columns:
            df2["label"] = df2["label"].apply(lambda x: shorten_text(x, 34))
        data = [list(df2.columns)] + df2.astype(str).values.tolist()
        tbl = Table(data, colWidths=widths, repeatRows=1)
        tbl.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(PRIMARY)),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), header_size),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#D6E4F2")),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor(LIGHT)]),
                    ("TEXTCOLOR", (0, 1), (-1, -1), colors.HexColor(TEXT)),
                    ("FONTSIZE", (0, 1), (-1, -1), font_size),
                    ("LEFTPADDING", (0, 0), (-1, -1), 2),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 2),
                    ("TOPPADDING", (0, 0), (-1, -1), 2),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )
        return tbl

    eq_tbl = Table([[Paragraph("Equities", h)], [styled_table(equities_df, [3.65 * cm, 1.05 * cm, 0.82 * cm, 0.82 * cm, 0.82 * cm, 0.82 * cm])]], colWidths=[7.98 * cm])
    rt_tbl = Table([[Paragraph("Rates", h)], [styled_table(rates_df, [3.55 * cm, 1.0 * cm, 0.8 * cm, 0.8 * cm, 0.8 * cm, 0.8 * cm])]], colWidths=[7.75 * cm])
    cm_tbl = Table([[Paragraph("Commodities / Alternatives / Bonds", h)], [styled_table(commodities_df, [3.55 * cm, 1.0 * cm, 0.8 * cm, 0.8 * cm, 0.8 * cm, 0.8 * cm])]], colWidths=[7.75 * cm])

    news_show = news_df.copy().fillna("")
    if not news_show.empty:
        news_show = news_show[["headline", "source", "url"]].head(3).copy()
        news_show["headline"] = news_show["headline"].apply(lambda x: shorten_text(x, 52))
        news_show["source"] = news_show["source"].apply(lambda x: shorten_text(x, 16))
        news_show["url"] = news_show["url"].apply(lambda x: short_url(x, 24))
    else:
        news_show = pd.DataFrame({"headline": ["No live article links"], "source": [""], "url": [""]})

    news_tbl = Table([[Paragraph("Specific News / Links", h)], [styled_table(news_show, [4.5 * cm, 1.35 * cm, 1.8 * cm], font_size=5.8, header_size=6.3)]], colWidths=[7.95 * cm])

    story += [Table([[eq_tbl, rt_tbl, cm_tbl, news_tbl]], colWidths=[8.0 * cm, 7.8 * cm, 7.8 * cm, 8.0 * cm])]
    doc.build(story)
    return buffer.getvalue()


def serialize_state(state):
    """Serialise state to JSON-safe dict. History is excluded — always re-fetched."""
    out = {}
    skip = {"history"}   # too large; re-fetched fresh on load
    for k, v in state.items():
        if k in skip:
            continue
        if isinstance(v, pd.DataFrame):
            out[k] = {"__type__": "dataframe", "value": v.to_json(orient="records", date_format="iso")}
        elif isinstance(v, list):
            try:
                json.dumps(v)
                out[k] = v
            except Exception:
                pass
        elif isinstance(v, (str, int, float, bool, type(None))):
            out[k] = v
        elif isinstance(v, dict):
            try:
                json.dumps(v)
                out[k] = v
            except Exception:
                pass
    return out


def deserialize_state(data):
    out = {}
    for k, v in data.items():
        if isinstance(v, dict) and v.get("__type__") == "dataframe":
            try:
                out[k] = pd.read_json(BytesIO(v["value"].encode()), orient="records")
            except Exception:
                pass
        else:
            out[k] = v
    return out


# ── GitHub Gist persistence ───────────────────────────────────────────────────
def _gist_headers():
    return {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}


def _load_gist_all() -> dict:
    """Return the full snapshots dict from the Gist file, or {} on any error."""
    if not GITHUB_TOKEN or not GITHUB_GIST_ID:
        return {}
    try:
        r = requests.get(f"https://api.github.com/gists/{GITHUB_GIST_ID}",
                         headers=_gist_headers(), timeout=15)
        if not r.ok:
            return {}
        files = r.json().get("files", {})
        raw = files.get(GIST_FILENAME, {}).get("content", "{}")
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


def _save_gist_all(all_snaps: dict) -> bool:
    """Write the full snapshots dict back to the Gist file."""
    if not GITHUB_TOKEN or not GITHUB_GIST_ID:
        return False
    try:
        payload = {"files": {GIST_FILENAME: {"content": json.dumps(all_snaps)}}}
        r = requests.patch(f"https://api.github.com/gists/{GITHUB_GIST_ID}",
                           headers=_gist_headers(), json=payload, timeout=20)
        return r.ok
    except Exception:
        return False


def save_snapshot(base_state, snapshot_date):
    payload = serialize_state(base_state)
    payload["snapshot_date"]     = snapshot_date
    payload["snapshot_saved_at"] = now_zurich().isoformat()

    if GITHUB_TOKEN and GITHUB_GIST_ID:
        all_snaps = _load_gist_all()
        all_snaps[snapshot_date] = payload
        # Keep only last 10 snapshots to stay well under Gist size limits
        if len(all_snaps) > 10:
            for old_key in sorted(all_snaps.keys())[:-10]:
                del all_snaps[old_key]
        _save_gist_all(all_snaps)
    else:
        # Fallback: local file (works locally, not on Streamlit Cloud)
        SNAPSHOT_DIR.mkdir(exist_ok=True)
        snapshot_path_for_date(snapshot_date).write_text(
            json.dumps(payload), encoding="utf-8"
        )


def load_snapshot(snapshot_date):
    if GITHUB_TOKEN and GITHUB_GIST_ID:
        all_snaps = _load_gist_all()
        payload   = all_snaps.get(snapshot_date)
        if payload is None:
            return None
        return deserialize_state(payload)
    else:
        path = snapshot_path_for_date(snapshot_date)
        if not path.exists():
            return None
        return deserialize_state(json.loads(path.read_text(encoding="utf-8")))


def latest_available_snapshot():
    if GITHUB_TOKEN and GITHUB_GIST_ID:
        all_snaps = _load_gist_all()
        if not all_snaps:
            return None, None
        latest_date = sorted(all_snaps.keys())[-1]
        return latest_date, deserialize_state(all_snaps[latest_date])
    else:
        files = sorted(SNAPSHOT_DIR.glob("*.json"))
        if not files:
            return None, None
        latest = files[-1]
        return latest.stem, load_snapshot(latest.stem)


def build_base_state(include_crypto_flag, news_count_value, use_gemini_flag):
    snapshot, history, chart_allowed_keys = build_bundle()

    if not include_crypto_flag:
        snapshot = snapshot[snapshot["group"] != "alternatives"].reset_index(drop=True)

    equities_df = snapshot[snapshot["group"] == "equities"][["label", "description", "level", "d1", "wtd", "mtd", "ytd"]]
    rates_df = snapshot[snapshot["group"] == "rates"][["label", "description", "level", "d1", "wtd", "mtd", "ytd"]]
    commodities_df = snapshot[snapshot["group"].isin(["commodities", "alternatives", "bonds"])] [["label", "description", "level", "d1", "wtd", "mtd", "ytd"]]

    news_df, news_status = load_news(news_count_value)
    writing, gemini_status = build_writing(news_df, snapshot, use_gemini_flag)

    # Merge Gemini per-article angles into news_df (keyed by headline)
    angles = writing.pop("article_angles", [])
    if angles and not news_df.empty:
        angle_map = {a.get("headline", ""): a.get("angle", "") for a in angles if isinstance(a, dict)}
        news_df = news_df.copy()
        news_df["gemini_angle"] = news_df["headline"].map(angle_map).fillna("")

    status = {
        "gemini_used": gemini_status["gemini_used"],
        "gemini_reason": gemini_status["reason"],
        "gemini_requested": use_gemini_flag,
        "live_news": news_status["live_news"],
        "article_count": news_status["article_count"],
        "url_count": news_status["url_count"],
        "news_reason": news_status["reason"],
    }

    def get_metric(key, field):
        s = snapshot.loc[snapshot["key"] == key, field]
        return None if s.empty else s.iloc[0]

    metrics = {
        "global_equities_ytd": get_metric("msci_world", "ytd"),
        "global_bonds_ytd": get_metric("global_bonds", "ytd"),
        "usd_bonds_ytd": get_metric("usd_bonds", "ytd"),
        "eur_bonds_ytd": get_metric("eur_bonds", "ytd"),
        "gold_ytd": get_metric("gold", "ytd"),
        "bitcoin_ytd": get_metric("bitcoin", "ytd"),
    }

    return {
        "snapshot": snapshot,
        "history": history,
        "equities_df": equities_df,
        "rates_df": rates_df,
        "commodities_df": commodities_df,
        "news_df": news_df,
        "writing": writing,
        "status": status,
        "metrics": metrics,
        "chart_allowed_keys": chart_allowed_keys,
        "include_crypto_flag": include_crypto_flag,
    }


def add_render_outputs(base_state, chart_window="YTD"):
    history = base_state["history"]
    chart_allowed_keys = base_state["chart_allowed_keys"]
    include_crypto_flag = base_state.get("include_crypto_flag", True)

    # Determine start date from chart_window
    today = pd.Timestamp.today().normalize()
    window_map = {
        "YTD":       pd.Timestamp(today.year, 1, 1),
        "3 months":  today - pd.DateOffset(months=3),
        "6 months":  today - pd.DateOffset(months=6),
        "1 year":    today - pd.DateOffset(years=1),
    }
    start_date = window_map.get(chart_window, pd.Timestamp(today.year, 1, 1))

    weekly_df = build_weekly_chart_df(history, chart_allowed_keys, include_crypto_flag, start_date=start_date)
    fig = None
    pdf_chart_png = None

    if not weekly_df.empty:
        core_keys = ["msci_world", "sp500", "stoxx600", "gold", "wti", "us10y", "global_bonds"]
        expanded_keys = ["msci_world", "sp500", "stoxx600", "gold", "wti", "us10y", "global_bonds", "bitcoin", "smi"]
        selected_keys = core_keys if chart_mode == "Core" else expanded_keys
        chart_df = weekly_df[weekly_df["key"].isin(selected_keys)].copy()
        short_labels = {
            "msci_world": "World",
            "sp500": "S&P 500",
            "stoxx600": "Europe 600",
            "gold": "Gold",
            "wti": "WTI",
            "us10y": "US 10Y",
            "global_bonds": "Global Bonds",
            "bitcoin": "Bitcoin",
            "smi": "SMI",
        }
        chart_df["short_label"] = chart_df["key"].map(short_labels).fillna(chart_df["label"])

        fig = px.line(
            chart_df,
            x="date",
            y="return_pct",
            color="short_label",
            title=f"{chart_mode} Cross-Asset Performance — {chart_window} (base = 0%)",
            color_discrete_sequence=["#103B73", "#1E88E5", "#38A3FF", "#26A69A", "#EF6C00", "#7E57C2", "#6D4C41", "#00897B", "#C62828"],
        )
        fig.update_traces(hovertemplate="<b>%{fullData.name}</b><br>Date: %{x|%d %b %Y}<br>YTD: %{y:.2f}%<extra></extra>")
        fig.update_layout(
            xaxis_title="Week",
            yaxis_title="YTD move (%)",
            height=560,
            legend_title="",
            hovermode="closest",
            plot_bgcolor="white",
            paper_bgcolor="white",
            margin=dict(l=25, r=20, t=55, b=40),
        )
        fig.update_xaxes(showgrid=True, gridcolor="#E6EEF7")
        fig.update_yaxes(showgrid=True, gridcolor="#E6EEF7")
        add_event_marker(fig, IRAN_WAR_START_DATE, "Iran conflict start<br>28 Feb 2026", "#C62828", 0.12, 11)
        add_event_marker(fig, IRAN_CEASEFIRE_DATE, "Iran ceasefire agreed", "#12B76A", 0.10, 11)
        fig.add_hline(y=0, line_dash="dot", line_color="#78909C")

        pdf_df = pdf_chart_subset(weekly_df)
        if not pdf_df.empty:
            pdf_df = pdf_df.copy()
            pdf_short_labels = {
                "msci_world": "World",
                "sp500": "S&P 500",
                "stoxx600": "Europe 600",
                "gold": "Gold",
                "wti": "WTI",
                "us10y": "US 10Y",
                "global_bonds": "Global Bonds",
            }
            pdf_df["short_label"] = pdf_df["key"].map(pdf_short_labels).fillna(pdf_df["label"])
            pdf_fig = px.line(
                pdf_df,
                x="date",
                y="return_pct",
                color="short_label",
                title="Core Cross-Asset YTD Performance (Start of Year = 0%)",
                color_discrete_sequence=["#103B73", "#1E88E5", "#38A3FF", "#26A69A", "#EF6C00", "#7E57C2", "#6D4C41"],
            )
            pdf_fig.update_traces(hovertemplate="<b>%{fullData.name}</b><br>Date: %{x|%d %b %Y}<br>YTD: %{y:.2f}%<extra></extra>")
            pdf_fig.update_layout(
                xaxis_title="",
                yaxis_title="",
                height=360,
                legend_title="",
                hovermode="closest",
                plot_bgcolor="white",
                paper_bgcolor="white",
                margin=dict(l=20, r=15, t=30, b=20),
            )
            pdf_fig.update_xaxes(showgrid=True, gridcolor="#E6EEF7")
            pdf_fig.update_yaxes(showgrid=True, gridcolor="#E6EEF7")
            add_event_marker(pdf_fig, IRAN_WAR_START_DATE, "Iran conflict start<br>28 Feb 2026", "#C62828", 0.12, 10)
            add_event_marker(pdf_fig, IRAN_CEASEFIRE_DATE, "Iran ceasefire agreed", "#12B76A", 0.10, 10)
            pdf_fig.add_hline(y=0, line_dash="dot", line_color="#78909C")
            try:
                pdf_chart_png = pdf_fig.to_image(format="png", scale=2)
            except Exception:
                pdf_chart_png = None

    pdf_bytes = build_pdf(
        "Daily Market Brief",
        pdf_chart_png,
        base_state["equities_df"],
        base_state["rates_df"],
        base_state["commodities_df"],
        base_state["metrics"],
        base_state["writing"],
        base_state["news_df"],
        base_state["status"],
    )

    # Chart of the Day — Gemini picks the most interesting move + news theme
    cotd = pick_chart_of_day(history, base_state.get("news_df"))

    state = dict(base_state)
    state["fig"]           = fig
    state["pdf_bytes"]     = pdf_bytes
    state["chart_of_day"]  = cotd
    return state


generate = False

with st.sidebar:
    st.markdown("**Data**")
    include_crypto = st.checkbox("Include crypto", value=True)
    mode = st.radio("Data mode", ["Live", "Morning snapshot"], index=1)
    st.caption("Morning snapshot freezes at 08:00 Zurich — use for newsletters.")

    st.markdown("---")
    st.markdown("**Chart**")
    chart_mode   = st.radio("Series", ["Core", "Expanded"], index=0)
    chart_window = st.radio("Window", ["YTD", "3 months", "6 months", "1 year"], index=0)

    st.markdown("---")
    st.markdown("**News**")
    news_count = st.selectbox("Articles to fetch", [5, 8, 10, 15], index=1)
    all_categories = ["Macro / Rates", "Geopolitics", "Equities", "Commodities", "Crypto", "Other"]
    news_category_filter = st.multiselect(
        "Filter by topic",
        all_categories,
        default=[],
        placeholder="All topics",
    )

    st.markdown("---")
    st.markdown("**AI & refresh**")
    use_gemini_writing = st.checkbox("Gemini commentary + article angles", value=True)
    show_definitions   = st.checkbox("Show definitions tables", value=False)
    auto_refresh       = st.checkbox("Auto-refresh (live mode)", value=False)
    refresh_seconds    = st.selectbox("Refresh every (s)", [30, 60, 120, 300], index=1)

    if auto_refresh and mode == "Live":
        st_autorefresh(interval=refresh_seconds * 1000, key="live_refresh")

    if st.button("🔄 Refresh now", use_container_width=True):
        st.rerun()

    st.markdown("---")
    generate = st.button("▶  Generate Daily Brief", type="primary", use_container_width=True)
    st.caption("Default ceasefire marker: 07 Apr 2026. Override via IRAN_CEASEFIRE_DATE in secrets.")

if generate:
    znow = now_zurich()
    today_str = znow.date().isoformat()

    if mode == "Live":
        base_state = build_base_state(include_crypto, news_count, use_gemini_writing)
        state = add_render_outputs(base_state, chart_window)
        st.session_state.update(state)
        st.session_state["snapshot_mode_note"] = f"Live mode | generated at {znow.strftime('%H:%M')} Zurich"
        st.session_state["ui_use_gemini"] = use_gemini_writing

    else:
        saved_base = load_snapshot(today_str)
        if saved_base is not None:
            saved_base["include_crypto_flag"] = include_crypto
            state = add_render_outputs(saved_base, chart_window)
            st.session_state.update(state)
            requested_note = "ON" if use_gemini_writing else "OFF"
            snap_note = "ON" if state["status"].get("gemini_used") else "OFF"
            st.session_state["snapshot_mode_note"] = (
                f"Morning snapshot mode | frozen snapshot for {today_str} | "
                f"Gemini requested: {requested_note} | in snapshot: {snap_note}"
            )
            st.session_state["ui_use_gemini"] = use_gemini_writing

        else:
            if znow.hour >= SNAPSHOT_HOUR:
                base_state = build_base_state(include_crypto, news_count, use_gemini_writing)
                save_snapshot(base_state, today_str)
                state = add_render_outputs(base_state, chart_window)
                st.session_state.update(state)
                st.session_state["snapshot_mode_note"] = (
                    f"Morning snapshot mode | first snapshot for {today_str} created at "
                    f"{znow.strftime('%H:%M')} Zurich — frozen for today's newsletter."
                )
                st.session_state["ui_use_gemini"] = use_gemini_writing
            else:
                prev_date, prev_base = latest_available_snapshot()
                if prev_base is not None:
                    prev_base["include_crypto_flag"] = include_crypto
                    state = add_render_outputs(prev_base, chart_window)
                    st.session_state.update(state)
                    st.session_state["snapshot_mode_note"] = (
                        f"No {today_str} morning snapshot yet. Using latest: {prev_date}."
                    )
                    st.session_state["ui_use_gemini"] = use_gemini_writing
                else:
                    base_state = build_base_state(include_crypto, news_count, use_gemini_writing)
                    state = add_render_outputs(base_state, chart_window)
                    st.session_state.update(state)
                    st.session_state["snapshot_mode_note"] = (
                        f"No saved snapshot exists yet. Provisional live build at "
                        f"{znow.strftime('%H:%M')} Zurich."
                    )
                    st.session_state["ui_use_gemini"] = use_gemini_writing

if "snapshot" not in st.session_state:
    st.info("▶  Press **Generate Daily Brief** in the sidebar to load market data.")
else:
    snap    = st.session_state["snapshot"]
    hist    = st.session_state["history"]
    writing = st.session_state["writing"]
    status  = st.session_state["status"]

    # ── 1. Compact status line ────────────────────────────────────────────────
    mode_note = st.session_state.get("snapshot_mode_note", "")
    g_col  = "🟢" if status["gemini_used"]  else "🟡"
    n_col  = "🟢" if status["live_news"]    else "🟡"
    st.caption(
        f"{mode_note}   |   {g_col} Gemini {'ON' if status['gemini_used'] else 'OFF'}  "
        f"·  {n_col} News {'live' if status['live_news'] else 'placeholder'}  "
        f"·  {status['article_count']} articles"
    )

    # ── 2. Compact ticker strip ───────────────────────────────────────────────
    render_ticker_strip(snap)

    # ── 3. Narrative: news bullets + what matters + next events ──────────────
    col_news, col_right = st.columns([3, 2], gap="medium")

    with col_news:
        gemini_tag = "" if status["gemini_used"] else " *(enable Gemini for AI commentary)*"
        st.markdown(f"**📰 What's Moving Markets**{gemini_tag}")
        render_news_bullets(writing, st.session_state["news_df"])

    with col_right:
        st.markdown("**🎯 What Matters**")
        for b in writing.get("what_matters", []):
            st.markdown(f"- {b}")

        st.markdown("**📅 Upcoming Events**")
        today = pd.Timestamp.today().normalize()
        upcoming = [e for e in MACRO_EVENTS if pd.Timestamp(e["date"]) >= today][:5]
        for ev in upcoming:
            dt = pd.Timestamp(ev["date"])
            days = (dt - today).days
            day_label = "TODAY" if days == 0 else f"in {days}d"
            st.markdown(
                f"<div style='display:flex;justify-content:space-between;padding:3px 0;"
                f"border-bottom:1px solid #F0F4F8;font-size:12px;'>"
                f"<span style='color:#0F2D52;'>{ev['event']}</span>"
                f"<span style='color:#64748B;white-space:nowrap;margin-left:8px;'>"
                f"{dt.strftime('%d %b')} · <b style='color:{'#EF4444' if days==0 else '#475467'};'>{day_label}</b></span>"
                f"</div>",
                unsafe_allow_html=True,
            )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 4. Main cross-asset chart + Chart of the Day ─────────────────────────
    chart_col, cotd_col = st.columns([3, 2], gap="medium")

    with chart_col:
        if st.session_state["fig"] is not None:
            st.markdown(f"**{writing['headline']}**")
            st.caption(writing["subheadline"])
            st.plotly_chart(st.session_state["fig"], use_container_width=True, key="main_big_chart")
        else:
            st.info("No chart data — market data fetch may have failed.")

    with cotd_col:
        cotd = st.session_state.get("chart_of_day")
        cotd_label = cotd["label"] if cotd else "Chart of the Day"
        st.markdown(f"**📈 Chart of the Day — {cotd_label}**")
        render_chart_of_day(cotd, hist)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 5. FX section ─────────────────────────────────────────────────────────
    with st.expander("💱 FX & Commodities", expanded=True):
        render_card_strip(snap, hist, FX_STRIP,
                          "FX & Key Commodities",
                          "EUR/USD · USD/CHF · EUR/CHF · DXY · Gold · Oil",
                          "fx_section")

    # ── 6. Detailed cards (collapsed) ─────────────────────────────────────────
    with st.expander("📊 Equities & Asset Class Detail", expanded=False):
        render_card_strip(snap, hist, INDICATOR_STRIP,
                          "Market Indicators",
                          "Equity indices + VIX + DXY. VIX red = fear rising.",
                          "market_indicators")
        render_card_strip(snap, hist, ASSET_CLASS_STRIP,
                          "Asset Class Performance",
                          "Cross-asset. Yields in bps, all others in %.",
                          "asset_classes")

    # ── 6. Data tables ────────────────────────────────────────────────────────
    with st.expander("📋 Full Data Tables", expanded=False):
        tabs = st.tabs(["Equities", "Rates", "Commodities / Bonds", "Definitions", "History"])
        with tabs[0]:
            st.dataframe(compact_table(st.session_state["equities_df"]),   use_container_width=True, height=380)
        with tabs[1]:
            st.dataframe(compact_table(st.session_state["rates_df"]),      use_container_width=True, height=220)
        with tabs[2]:
            st.dataframe(compact_table(st.session_state["commodities_df"]),use_container_width=True, height=380)
        with tabs[3]:
            if show_definitions:
                st.dataframe(definitions_table(st.session_state["equities_df"]),    use_container_width=True, height=220)
                st.dataframe(definitions_table(st.session_state["rates_df"]),       use_container_width=True, height=130)
                st.dataframe(definitions_table(st.session_state["commodities_df"]), use_container_width=True, height=220)
            else:
                st.info("Enable 'Show definitions' in the sidebar.")
        with tabs[4]:
            st.dataframe(st.session_state["history"], use_container_width=True, height=480)

    # ── 7. PDF download ───────────────────────────────────────────────────────
    st.download_button(
        "⬇  Download one-page PDF newsletter",
        st.session_state["pdf_bytes"],
        file_name=f"nxman_daily_brief_{pd.Timestamp.today().date()}.pdf",
        mime="application/pdf",
        use_container_width=True,
    )
