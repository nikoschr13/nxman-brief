import os
import re
import json
import time
from io import BytesIO
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import feedparser
import streamlit as st
import yfinance as yf
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh
from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle

# Optional: Drive-backed research library feed. If brief_drive_reader isn't
# importable (e.g. dep not installed locally), Brief falls back to manual upload.
try:
    from brief_drive_reader import load_research_pdfs_dict_cached as _drive_pdf_loader
except Exception:
    _drive_pdf_loader = None

# Drive upload is intentionally disabled: Google service accounts cannot
# write to personal My Drive folders (Service Accounts do not have storage
# quota). PDFs are added to Drive manually by the user; the Brief just reads
# from Research_Inbox.
_drive_pdf_uploader = None  # kept for any legacy reference sites

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
# Only include models that are CURRENTLY in Google's Gemini API. The 1.5
# / 1.0 models were deprecated and return HTTP 404, which was previously
# burning through the fallback chain before reaching Groq. Keep this list
# current; if you override via secrets, only add models you've verified exist.
GEMINI_FALLBACK_MODELS = [
    m.strip()
    for m in get_secret(
        "GEMINI_FALLBACK_MODELS",
        "gemini-2.5-flash,gemini-2.0-flash,gemini-2.0-flash-lite"
    ).split(",")
    if m.strip()
]
GROQ_API_KEY   = get_secret("GROQ_API_KEY")
GROQ_MODEL     = get_secret("GROQ_MODEL", "llama-3.3-70b-versatile")
MANUAL_BUND_10Y = get_secret("MANUAL_BUND_10Y")
MANUAL_CH_10Y   = get_secret("MANUAL_CH_10Y")

# GitHub Gist persistence for morning snapshots
GITHUB_TOKEN   = get_secret("GITHUB_TOKEN")
GITHUB_GIST_ID = get_secret("GITHUB_GIST_ID")
GIST_FILENAME  = "nxman_snapshots.json"

# Gmail — for forwarded FT/Bloomberg/work emails as extra news source
GMAIL_EMAIL        = get_secret("GMAIL_EMAIL")
GMAIL_APP_PASSWORD = get_secret("GMAIL_APP_PASSWORD")

ASSETS = [
    ("equities", "sp500",      "S&P 500",
     "Index of 500 large US companies. When it goes UP, US equity prices are rising — good for stock holders. When it goes DOWN, US stocks are losing value.",
     "^GSPC", True),
    ("equities", "nasdaq100",  "Nasdaq 100",
     "Index of the 100 largest non-financial Nasdaq companies, heavily weighted to tech. When UP, tech and growth stocks are gaining. A bigger move than S&P 500 usually signals risk-on sentiment.",
     "^NDX", True),
    ("equities", "stoxx600",   "Stoxx Europe 600",
     "Benchmark covering 600 European companies across 17 countries. When UP, European equity prices are rising. A divergence from US indices signals region-specific drivers.",
     "^STOXX", True),
    ("equities", "msci_world", "MSCI World",
     "Index of large and mid-cap stocks across 23 developed markets. When UP, global equity portfolios are gaining value in aggregate.",
     "^990100-USD-STRD", True),
    ("equities", "msci_em",    "MSCI Emerging Markets",
     "Index of stocks across 24 emerging-market countries. When UP, EM equity prices are rising. More volatile than developed markets and sensitive to USD strength.",
     "^891800-USD-STRD", False),
    ("equities", "nikkei225",  "Nikkei 225",
     "Index of 225 large Japanese companies. When UP, Japanese equities are gaining. Often moves with USD/JPY — a weaker yen can boost Japanese exporters.",
     "^N225", False),
    ("equities", "smi",        "SMI (Switzerland)",
     "Index of the 20 largest Swiss companies (Nestlé, Novartis, Roche dominate). When UP, Swiss large-caps are gaining. Defensive nature means it often falls less than peers in downturns.",
     "^SSMI", True),
    ("fx",       "eurusd",     "EUR/USD",
     "How many USD one euro buys. When UP, the euro is strengthening vs the dollar — good for EUR-based investors holding USD assets (they lose value) and vice versa. Affects European exporters' competitiveness.",
     "EURUSD=X", False),
    ("fx",       "usdchf",     "USD/CHF",
     "How many CHF one USD buys. When UP, the dollar is strengthening vs the franc — CHF-based investors holding USD assets gain. A falling CHF can pressure Swiss exporters' margins.",
     "USDCHF=X", False),
    ("fx",       "eurchf",     "EUR/CHF",
     "How many CHF one euro buys. When DOWN, the franc is strengthening vs the euro — typical in risk-off markets as CHF is a safe-haven. Relevant for Swiss investors with EUR exposure.",
     "EURCHF=X", True),
    ("fx",       "dxy",        "DXY (USD Index)",
     "The dollar's strength against a basket of major currencies. When UP, the USD is strengthening globally — generally negative for commodities (priced in USD) and emerging markets.",
     "DX-Y.NYB", False),
    ("commodities","gold",     "Gold",
     "Gold spot price. When UP, gold is gaining — typically signals risk-off sentiment, inflation concerns, or USD weakness. Gold rises when investors seek safety.",
     "GC=F", True),
    ("commodities","silver",   "Silver",
     "Silver spot price. When UP, silver is gaining. Has both safe-haven and industrial demand, so it can move with gold OR with economic activity expectations.",
     "SI=F", False),
    ("commodities","wti",      "WTI Crude",
     "West Texas Intermediate crude oil price. When UP, oil costs more — raises energy and transport costs, feeds inflation, and benefits oil-exporting nations. Negative for airlines and energy-intensive industries.",
     "CL=F", True),
    ("commodities","brent",    "Brent Crude",
     "Brent crude oil price (global benchmark). When UP, global energy costs are rising. Brent typically trades at a slight premium to WTI and is more relevant for European and Asian pricing.",
     "BZ=F", False),
    ("commodities","copper",   "Copper",
     "Copper futures price. When UP, copper is gaining — often a signal of stronger global growth expectations, as copper is used in construction and manufacturing. Known as 'Dr Copper' for its predictive power.",
     "HG=F", False),
    ("alternatives","bitcoin", "Bitcoin",
     "Price of Bitcoin in USD. When UP, crypto is gaining. Bitcoin behaves as a high-beta risk asset — it tends to amplify market moves. A rally often reflects broader risk-on sentiment.",
     "BTC-USD", True),
    ("alternatives","ethereum","Ethereum",
     "Price of Ethereum in USD. When UP, ETH is gaining. More sensitive to developments in decentralised finance than Bitcoin. Usually moves directionally with Bitcoin but with higher volatility.",
     "ETH-USD", False),
    ("sentiment",  "vix",      "VIX (Fear Index)",
     "The CBOE Volatility Index — measures expected S&P 500 volatility over the next 30 days. When UP, fear and uncertainty are rising — investors expect larger price swings. Above 20 = elevated anxiety; above 30 = high fear. VIX rising is generally BAD for equities.",
     "^VIX", False),
]

# Multiple tickers to try for MSCI World (in order of preference)
MSCI_WORLD_TICKERS = ["^990100-USD-STRD", "^MXWO", "URTH"]

# Multiple tickers to try for MSCI Emerging Markets. The bare-index ticker
# (^891800-USD-STRD) is unreliable on yfinance, leaving the row blank — which
# the reviewer flagged as a credibility issue. Fall through to liquid US-listed
# EM ETFs (EEM/VWO/IEMG) so the row populates daily.
MSCI_EM_TICKERS = ["^891800-USD-STRD", "EEM", "VWO", "IEMG"]

RATES = [
    ("rates", "us10y", "US 10Y Treasury",
     "Yield on 10-year US government bonds. When the YIELD goes UP, existing bond prices fall (inverse relationship) — bad for bond holders. A rising yield also increases borrowing costs for companies and mortgages, and can pressure equity valuations. When the yield falls, existing bond prices rise.",
     "DGS10", True),
    ("rates", "bund10y", "German 10Y Bund",
     "Yield on 10-year German government bonds — the euro area's benchmark safe rate. When the YIELD goes UP, existing Bund prices fall. Rising Bund yields signal ECB tightening expectations or euro-area growth optimism. Falling yields signal flight to safety or rate-cut expectations.",
     None, False),
    ("rates", "ch10y", "Swiss 10Y Government Bond",
     "Yield on 10-year Swiss government bonds — one of the world's lowest-yielding safe assets. When UP, existing Swiss bond prices fall. A very low or negative yield reflects the franc's safe-haven status and SNB policy.",
     None, False),
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


st.set_page_config(page_title="Daily Market Briefing", layout="wide")

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
/* ── Sidebar: compact, no excess whitespace ── */
section[data-testid="stSidebar"] { padding-top: 0.5rem !important; }
section[data-testid="stSidebar"] .block-container { padding-top: 0.5rem !important; padding-bottom: 0.5rem !important; }
section[data-testid="stSidebar"] .stRadio { margin-bottom: 0 !important; }
section[data-testid="stSidebar"] .stRadio > label { margin-bottom: 0.1rem !important; font-size: 13px !important; }
section[data-testid="stSidebar"] .stRadio > div { gap: 0.15rem !important; }
section[data-testid="stSidebar"] .stCheckbox { margin-bottom: 0.1rem !important; }
section[data-testid="stSidebar"] .stCheckbox > label { font-size: 13px !important; }
section[data-testid="stSidebar"] .stSelectbox { margin-bottom: 0.2rem !important; }
section[data-testid="stSidebar"] .stButton { margin-top: 0.3rem !important; margin-bottom: 0.1rem !important; }
section[data-testid="stSidebar"] p { margin-bottom: 0.1rem !important; font-size: 13px !important; }
section[data-testid="stSidebar"] hr { margin: 0.3rem 0 !important; }
div[data-testid="stMetric"] { background: transparent !important;
                               padding: 0 !important; border: 0 !important; }
details summary { font-size: 14px !important; padding: 6px 0 !important; }

/* ── Mobile / iPhone responsive ── */
@media (max-width: 768px) {
  .block-container { padding-left: 0.4rem !important; padding-right: 0.4rem !important; }
  .hero h1 { font-size: 16px !important; }
  .hero-sub { font-size: 11px !important; }
  .section-card { padding: 8px 10px !important; border-radius: 10px !important; }

  /* Stack Streamlit columns vertically on mobile */
  div[data-testid="column"] { min-width: 100% !important; flex: 0 0 100% !important; }

  /* Ticker strip: scrollable horizontally */
  div[data-testid="stMarkdownContainer"] table { min-width: 560px !important; }

  /* Make Plotly charts not overflow */
  div[data-testid="stPlotlyChart"] { overflow-x: auto !important; }

  /* Sidebar: full width on mobile when open */
  section[data-testid="stSidebar"] { width: 80vw !important; }

  /* Reduce font sizes for readability on small screens */
  p, li, .stMarkdown { font-size: 13px !important; }
  h2, h3 { font-size: 15px !important; }
  details summary { font-size: 13px !important; }

  /* Expander headings */
  div[data-testid="stExpander"] summary { font-size: 13px !important; padding: 5px 0 !important; }

  /* Dataframes: allow horizontal scroll */
  div[data-testid="stDataFrame"] { overflow-x: auto !important; -webkit-overflow-scrolling: touch !important; }
  div[data-testid="stDataFrame"] iframe { min-width: 340px !important; }

  /* Metrics: smaller padding */
  div[data-testid="stMetric"] label { font-size: 11px !important; }
  div[data-testid="stMetric"] div[data-testid="stMetricValue"] { font-size: 15px !important; }
}
</style>
""",
    unsafe_allow_html=True,
)

st.markdown(
    "<div class='hero'>"
    "<h1>Daily Market Briefing</h1>"
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


# ── Trusted financial news domains (allowlist) ───────────────────────────────
_FINANCIAL_DOMAINS = {
    "reuters.com", "marketwatch.com", "wsj.com", "yahoo.com", "finance.yahoo.com",
    "cnbc.com", "bloomberg.com", "ft.com", "barrons.com", "investing.com",
    "seekingalpha.com", "thestreet.com", "businessinsider.com", "fortune.com",
    "economist.com", "morningstar.com", "financialtimes.com", "apnews.com",
    "axios.com", "politico.com", "thehill.com", "bbc.com", "bbc.co.uk",
}

def _is_financial_url(url: str) -> bool:
    """Return True only if the URL belongs to a trusted financial/news domain."""
    if not url:
        return True
    try:
        domain = urlparse(url.lower()).netloc.lstrip("www.")
        return any(domain == d or domain.endswith("." + d) for d in _FINANCIAL_DOMAINS)
    except Exception:
        return True


# ── Morning Call PDF parser ───────────────────────────────────────────────────
def parse_morning_call(pdf_bytes: bytes) -> dict:
    """Parse Bank of Singapore (or similar) Morning Call PDF.
    Extracts: date, regional summaries (US/Europe/Asia), equity viewpoints,
    fixed income bullets, FX views, and recommendation changes table.
    """
    result = {
        "date": "", "source": "Morning Call",
        "regional_summaries": {}, "equity_viewpoints": [],
        "fixed_income": [], "fx_views": [],
        "recommendation_changes": {"upgrades": [], "downgrades": [], "fair_value_changes": []},
        "error": None,
    }
    try:
        import pdfplumber
    except ImportError:
        result["error"] = "pdfplumber not installed — run: pip install pdfplumber"
        return result
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            pages_text, all_tables = [], []
            for page in pdf.pages:
                t = page.extract_text() or ""
                pages_text.append(t)
                tbls = page.extract_tables() or []
                all_tables.extend(tbls)
            full = "\n".join(pages_text)

            # ── Date ──────────────────────────────────────────────────────────
            dm = re.search(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+\d+\s+\w+\s+\d{4}', full)
            if dm:
                result["date"] = dm.group(0)

            # ── Regional summaries ────────────────────────────────────────────
            region_bounds = [
                ("US",           ["Europe", "Asia Pacific", "MONTHLY", "INVESTMENT VIEWPOINTS"]),
                ("Europe",       ["Asia Pacific", "MONTHLY", "INVESTMENT VIEWPOINTS"]),
                ("Asia Pacific", ["MONTHLY", "INVESTMENT VIEWPOINTS", "Fixed income"]),
            ]
            for region, stops in region_bounds:
                stop_pat = "|".join(re.escape(s) for s in stops)
                m = re.search(rf'(?:^|\n){re.escape(region)}\s*\n(.*?)(?={stop_pat})', full, re.DOTALL)
                if m:
                    txt = re.sub(r'\s+', ' ', m.group(1)).strip()[:1200]
                    result["regional_summaries"][region] = txt

            # ── Investment Viewpoints subsections ────────────────────────────
            def extract_bullets(header, stops):
                stop_pat = "|".join(re.escape(s) for s in stops)
                m = re.search(rf'{re.escape(header)}\s*\n(.*?)(?={stop_pat})', full, re.DOTALL)
                if not m:
                    return []
                raw = m.group(1)
                parts = [re.sub(r'\s+', ' ', b).strip() for b in re.split(r'[•·]\s*', raw) if len(b.strip()) > 30]
                return parts[:6]

            result["equity_viewpoints"] = extract_bullets(
                "Equities", ["Fixed income", "Foreign exchange", "INVESTMENT IDEAS", "LATEST RECOMMENDATION"])
            result["fixed_income"] = extract_bullets(
                "Fixed income", ["Foreign exchange", "INVESTMENT IDEAS", "LATEST RECOMMENDATION"])
            result["fx_views"] = extract_bullets(
                "Foreign exchange", ["INVESTMENT IDEAS", "LATEST RECOMMENDATION", "Equities\n"])

            # ── Recommendation changes table ──────────────────────────────────
            rec = result["recommendation_changes"]
            for table in all_tables:
                if not table or len(table) < 2:
                    continue
                flat = " ".join(str(c or "") for row in table for c in row).lower()
                if not any(kw in flat for kw in ["buy", "sell", "hold", "upgrade", "downgrade", "nc"]):
                    continue
                current = None
                for row in table:
                    if not row:
                        continue
                    cells = [str(c or "").strip() for c in row]
                    row_str = " ".join(cells).lower()
                    if "upgrades" in row_str and len(row_str) < 30:
                        current = "upgrades"; continue
                    if "downgrades" in row_str and len(row_str) < 30:
                        current = "downgrades"; continue
                    if "fair value" in row_str and len(row_str) < 40:
                        current = "fair_value_changes"; continue
                    if current and cells[0] and cells[0].lower() not in ("name", "legend", "note", ""):
                        non_empty = [c for c in cells if c]
                        if len(non_empty) >= 3:
                            rec[current].append({
                                "name":       cells[0],
                                "date":       cells[1] if len(cells) > 1 else "",
                                "price":      cells[2] if len(cells) > 2 else "",
                                "currency":   cells[3] if len(cells) > 3 else "",
                                "rating_old": cells[4] if len(cells) > 4 else "",
                                "rating_new": cells[5] if len(cells) > 5 else "",
                                "fv_old":     cells[6] if len(cells) > 6 else "",
                                "fv_new":     cells[7] if len(cells) > 7 else "",
                            })
    except Exception as e:
        result["error"] = str(e)
    return result


def render_morning_call(mc: dict):
    """Render parsed Morning Call data as a Streamlit section."""
    if not mc:
        return
    date_str = mc.get("date", "")
    title = "🏦 Morning Call" + (f" — {date_str}" if date_str else "")
    with st.expander(title, expanded=True):
        if mc.get("error"):
            st.warning(f"Parse issue: {mc['error']}")

        # Regional summaries
        summaries = mc.get("regional_summaries", {})
        if summaries:
            st.markdown("**🌍 Regional Summaries**")
            cols = st.columns(len(summaries))
            for i, (region, text) in enumerate(summaries.items()):
                with cols[i]:
                    st.markdown(f"<div style='font-size:11.5px;font-weight:700;color:#103B73;"
                                f"margin-bottom:4px;'>{region}</div>", unsafe_allow_html=True)
                    st.markdown(f"<div style='font-size:11px;line-height:1.55;color:#334155;'>"
                                f"{text}</div>", unsafe_allow_html=True)
            st.markdown("---")

        # Recommendation changes
        rec = mc.get("recommendation_changes", {})
        has_recs = any(rec.get(k) for k in ["upgrades", "downgrades", "fair_value_changes"])
        if has_recs:
            st.markdown("**📊 Latest Recommendation Changes**")
            for section, label, bg in [
                ("upgrades",           "⬆ Upgrades",           "#E8F5E9"),
                ("downgrades",         "⬇ Downgrades",         "#FFEBEE"),
                ("fair_value_changes", "💰 Fair Value Changes", "#E3F2FD"),
            ]:
                items = rec.get(section, [])
                if not items:
                    continue
                st.markdown(
                    f"<div style='background:{bg};padding:3px 8px;border-radius:4px;"
                    f"font-weight:600;font-size:11.5px;margin:6px 0 2px;'>{label}</div>",
                    unsafe_allow_html=True)
                rows = []
                for it in items:
                    old_r, new_r = it.get("rating_old",""), it.get("rating_new","")
                    old_fv, new_fv = it.get("fv_old",""), it.get("fv_new","")
                    rows.append({
                        "Company":    it.get("name",""),
                        "Price":      f"{it.get('currency','')} {it.get('price','')}".strip(),
                        "Rating":     f"{old_r} → {new_r}" if new_r and new_r != "NC" else old_r,
                        "Fair Value": f"{old_fv} → {new_fv}" if new_fv and new_fv != "NC" else old_fv,
                    })
                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True,
                                 hide_index=True, height=min(36 * len(rows) + 40, 220))
            st.markdown("---")

        # FX views + Fixed income side by side
        fx = mc.get("fx_views", [])
        fi = mc.get("fixed_income", [])
        if fx or fi:
            c1, c2 = st.columns(2)
            with c1:
                if fx:
                    st.markdown("**💱 FX Views**")
                    for v in fx:
                        st.markdown(f"<div style='font-size:11px;margin-bottom:5px;'>• {v}</div>",
                                    unsafe_allow_html=True)
            with c2:
                if fi:
                    st.markdown("**📈 Fixed Income**")
                    for v in fi[:5]:
                        st.markdown(f"<div style='font-size:11px;margin-bottom:5px;'>• {v}</div>",
                                    unsafe_allow_html=True)
            st.markdown("---")

        # Equity viewpoints
        ev = mc.get("equity_viewpoints", [])
        if ev:
            st.markdown("**📈 Equity Viewpoints**")
            for v in ev:
                st.markdown(f"<div style='font-size:11px;margin-bottom:6px;'>• {v}</div>",
                            unsafe_allow_html=True)


# ── Research Library: multi-file upload & parse ───────────────────────────────

def _pdf_first_page_text(pdf_bytes: bytes) -> str:
    """Quick text extract from first 2 pages for type detection."""
    try:
        import pdfplumber
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            return " ".join((pdf.pages[i].extract_text() or "") for i in range(min(2, len(pdf.pages))))
    except Exception:
        return ""


def detect_pdf_type(pdf_bytes: bytes, filename: str) -> str:
    """Detect what kind of research document this is."""
    fn = filename.lower()
    text = _pdf_first_page_text(pdf_bytes).lower()
    if "equity coverage universe" in text or "equity_coverage" in fn:
        return "equity_coverage"
    if ("morning call" in text or "morning_call" in fn or
            ("bank of singapore" in text and ("us\n" in text or "europe\n" in text))):
        return "morning_call"
    if "fixed income" in text and ("coverage universe" in text or "bond list" in text):
        return "fixed_income_coverage"
    if "focus list" in text or ("preferred" in text and "fixed income" in text):
        return "preferred_fi"
    if "monthly investment guide" in text or "building resilience" in text or "wealth equation" in text:
        return "monthly_guide"
    return "generic_research"


def parse_equity_universe(pdf_bytes: bytes) -> dict:
    """Parse the BOS Equity Coverage Universe PDF into a structured stock list."""
    try:
        import pdfplumber
    except ImportError:
        return {"error": "pdfplumber not installed", "stocks": []}

    stocks = []
    known_regions = {"NORTH AMERICA","EUROPE","ASIA PACIFIC","GREATER CHINA",
                     "SINGAPORE","INDONESIA","JAPAN","LATIN AMERICA","MIDDLE EAST"}
    known_ratings = {"Buy","Hold","Sell","UR","Restricted","NC"}
    date_str = ""

    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            # Date from first page
            p0 = pdf.pages[0].extract_text() or ""
            dm = re.search(r'\d{1,2}\s+\w+\s+\d{4}', p0)
            if dm:
                date_str = dm.group(0)

            current_region = ""
            current_sector = ""

            for page in pdf.pages[2:]:   # skip intro pages
                tables = page.extract_tables() or []
                for table in tables:
                    for row in table:
                        if not row or not row[0]:
                            continue
                        cells = [str(c or "").strip() for c in row]
                        name   = cells[0]
                        ticker = cells[2] if len(cells) > 2 else ""
                        rating = cells[4] if len(cells) > 4 else ""

                        # Detect region/sector headers
                        if name.upper() in known_regions:
                            current_region = name.title(); current_sector = ""; continue
                        if (not ticker and rating not in known_ratings
                                and name and not name[0].isdigit()
                                and name not in ("Company Name","Moat","Ticker")):
                            # likely a sector header
                            current_sector = name; continue

                        if ticker and rating in known_ratings and name.isupper():
                            def _f(idx):
                                return cells[idx] if len(cells) > idx else ""
                            stocks.append({
                                "region":      current_region,
                                "sector":      current_sector,
                                "name":        name,
                                "ticker":      ticker,
                                "mkt_cap":     _f(3),
                                "rating":      rating,
                                "currency":    _f(5),
                                "price":       _f(6),
                                "fair_value":  _f(7),
                                "upside":      _f(8),
                                "div_yield":   _f(9),
                                "pe":          _f(10),
                                "pb":          _f(11),
                                "eps_gr":      _f(12),
                                "roe":         _f(13),
                                "risk":        _f(14),
                                "ytd":         _f(15),
                                "esg":         _f(16),
                                "uncertainty": _f(18),
                            })
    except Exception as e:
        return {"error": str(e), "stocks": stocks, "date": date_str}

    return {"stocks": stocks, "date": date_str, "error": None}


def parse_generic_research(pdf_bytes: bytes, filename: str) -> dict:
    """Extract text from any PDF and return structured summary."""
    try:
        import pdfplumber
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            pages_text = []
            for page in pdf.pages[:20]:   # limit to 20 pages
                t = page.extract_text() or ""
                if t.strip():
                    pages_text.append(t)
            full_text = "\n".join(pages_text)
            return {
                "filename": filename,
                "pages": len(pdf.pages),
                "text": full_text[:8000],   # cap for display
                "error": None,
            }
    except Exception as e:
        return {"filename": filename, "pages": 0, "text": "", "error": str(e)}


def auto_detect_and_parse(pdf_bytes: bytes, filename: str) -> dict:
    """Route PDF to correct parser based on content detection."""
    doc_type = detect_pdf_type(pdf_bytes, filename)
    if doc_type == "morning_call":
        result = parse_morning_call(pdf_bytes)
    elif doc_type == "equity_coverage":
        result = parse_equity_universe(pdf_bytes)
    else:
        result = parse_generic_research(pdf_bytes, filename)
    result["_doc_type"] = doc_type
    result["_filename"] = filename
    return result


def autoload_research_from_drive(force_refresh: bool = False) -> tuple[int, int]:
    """Populate st.session_state['research_docs'] with PDFs pulled from
    Google Drive (SNIPER/Research_Processed + Research_Inbox).

    Errored parses are tracked separately in st.session_state['_drive_research_failed']
    so we NEVER hand a malformed doc to build_pdf — the PDF render path only
    ever sees clean, fully-parsed docs.

    Runs at most once per session unless force_refresh=True.
    Returns (loaded_count, failed_count) from this call.
    """
    if _drive_pdf_loader is None:
        return (0, 0)

    # Only pull from Drive once per Streamlit session — users can still upload
    # extras manually, and a 🔄 button lets them force a re-sync explicitly.
    flag = "_drive_research_loaded"
    if st.session_state.get(flag) and not force_refresh:
        return (0, 0)

    try:
        pdfs = _drive_pdf_loader()
    except Exception:
        st.session_state[flag] = True  # don't loop-retry on error
        return (0, 0)

    if "research_docs" not in st.session_state:
        st.session_state["research_docs"] = {}
    if "_drive_research_failed" not in st.session_state:
        st.session_state["_drive_research_failed"] = {}

    loaded = 0
    failed = 0
    for fname, pdf_bytes in (pdfs or {}).items():
        already_loaded = fname in st.session_state["research_docs"]
        already_failed = fname in st.session_state["_drive_research_failed"]
        if not force_refresh and (already_loaded or already_failed):
            continue

        try:
            doc = auto_detect_and_parse(pdf_bytes, fname)
        except Exception as e:
            doc = {"_filename": fname, "error": f"parse failed: {e}"}

        if doc.get("error"):
            # Record the failure so we can surface it in the sidebar, but do
            # NOT put it in research_docs — that dict is consumed by build_pdf
            # and any half-parsed doc will crash the PDF render.
            st.session_state["_drive_research_failed"][fname] = doc.get("error", "parse failed")
            # Clean any stale successful copy if this is a force refresh.
            if force_refresh:
                st.session_state["research_docs"].pop(fname, None)
            failed += 1
        else:
            st.session_state["research_docs"][fname] = doc
            st.session_state["_drive_research_failed"].pop(fname, None)
            loaded += 1

    st.session_state[flag] = True
    return (loaded, failed)


# ── Research context helpers ───────────────────────────────────────────────────

def get_research_context(research_docs: dict) -> str:
    """Distil uploaded research docs into a concise text block for the AI prompt."""
    if not research_docs:
        return ""
    parts = []
    for fname, doc in research_docs.items():
        dtype = doc.get("_doc_type", "generic_research")
        short = fname[:30]
        if dtype == "morning_call":
            # Regional summaries
            for region, txt in (doc.get("regional_summaries") or {}).items():
                if txt:
                    parts.append(f"[{short}|{region}] {txt[:250]}")
            # Equity viewpoints
            for vp in (doc.get("equity_viewpoints") or [])[:4]:
                parts.append(f"[{short}|Equity] {vp[:200]}")
            # Recommendation changes
            rec = doc.get("recommendation_changes") or {}
            for upg in (rec.get("upgrades") or [])[:3]:
                parts.append(f"[{short}|UPGRADE] {upg.get('name','')} → {upg.get('rating_new','')}")
            for dwn in (rec.get("downgrades") or [])[:3]:
                parts.append(f"[{short}|DOWNGRADE] {dwn.get('name','')} → {dwn.get('rating_new','')}")
        elif dtype == "equity_coverage":
            stocks = doc.get("stocks") or []
            buys  = [s["name"] for s in stocks if s.get("rating") == "Buy"][:8]
            sells = [s["name"] for s in stocks if s.get("rating") == "Sell"][:5]
            if buys:
                parts.append(f"[{short}|BUY rated] {', '.join(buys)}")
            if sells:
                parts.append(f"[{short}|SELL rated] {', '.join(sells)}")
        else:
            text = doc.get("text") or ""
            if text:
                parts.append(f"[{short}] {' '.join(text[:500].split())}")
    return "\n".join(parts[:25])


def save_research_snapshot(research_docs: dict, date_str: str) -> None:
    """Save today's equity ratings + recommendation changes to the Gist for tracking."""
    if not research_docs or not GITHUB_TOKEN or not GITHUB_GIST_ID:
        return
    try:
        snapshot = {"date": date_str, "docs": {}}
        for fname, doc in research_docs.items():
            dtype = doc.get("_doc_type", "generic_research")
            entry: dict = {"type": dtype}
            if dtype == "equity_coverage":
                entry["stocks"] = [
                    {"name": s.get("name"), "ticker": s.get("ticker"),
                     "rating": s.get("rating"), "fair_value": s.get("fair_value")}
                    for s in (doc.get("stocks") or [])
                ]
            elif dtype == "morning_call":
                rec = doc.get("recommendation_changes") or {}
                entry["upgrades"]   = rec.get("upgrades", [])
                entry["downgrades"] = rec.get("downgrades", [])
                entry["fv_changes"] = rec.get("fair_value_changes", [])
            snapshot["docs"][fname] = entry

        all_snaps = _load_gist_all()
        key = f"research_{date_str}"
        all_snaps[key] = snapshot
        _save_gist_all(all_snaps)
    except Exception:
        pass


def diff_research_snapshots(today_docs: dict, today_str: str) -> list:
    """Compare today's research against yesterday's. Returns list of change dicts."""
    if not GITHUB_TOKEN or not GITHUB_GIST_ID:
        return []
    try:
        import datetime as _dt
        yesterday = (pd.Timestamp(today_str) - pd.Timedelta(days=1)).date().isoformat()
        all_snaps = _load_gist_all()
        prev = all_snaps.get(f"research_{yesterday}")
        if not prev:
            # Try up to 7 days back
            for d in range(2, 8):
                day = (pd.Timestamp(today_str) - pd.Timedelta(days=d)).date().isoformat()
                prev = all_snaps.get(f"research_{day}")
                if prev:
                    break
        if not prev:
            return []

        changes = []
        # Build previous rating map: {name -> rating}
        prev_ratings = {}
        for fname, entry in (prev.get("docs") or {}).items():
            if entry.get("type") == "equity_coverage":
                for s in (entry.get("stocks") or []):
                    if s.get("name"):
                        prev_ratings[s["name"]] = {
                            "rating": s.get("rating"), "fv": s.get("fair_value"), "src": fname}

        # Compare against today
        for fname, doc in today_docs.items():
            if doc.get("_doc_type") == "equity_coverage":
                for s in (doc.get("stocks") or []):
                    name = s.get("name")
                    if not name:
                        continue
                    prev_info = prev_ratings.get(name)
                    if prev_info:
                        old_r = prev_info.get("rating")
                        new_r = s.get("rating")
                        old_fv = prev_info.get("fv")
                        new_fv = s.get("fair_value")
                        if old_r != new_r:
                            changes.append({"type": "rating", "name": name,
                                            "old": old_r, "new": new_r, "src": fname})
                        elif old_fv and new_fv and old_fv != new_fv:
                            changes.append({"type": "fv", "name": name,
                                            "old": old_fv, "new": new_fv, "src": fname})
            elif doc.get("_doc_type") == "morning_call":
                rec = doc.get("recommendation_changes") or {}
                for upg in (rec.get("upgrades") or []):
                    changes.append({"type": "upgrade", "name": upg.get("name",""),
                                    "old": upg.get("rating_old",""), "new": upg.get("rating_new",""),
                                    "src": fname})
                for dwn in (rec.get("downgrades") or []):
                    changes.append({"type": "downgrade", "name": dwn.get("name",""),
                                    "old": dwn.get("rating_old",""), "new": dwn.get("rating_new",""),
                                    "src": fname})
        return changes
    except Exception:
        return []


def _ticker_to_yahoo_url(ticker: str) -> str:
    """Convert BOS ticker format (e.g. 'META US', '1698 HK') to Yahoo Finance URL."""
    parts = ticker.strip().split()
    if len(parts) < 2:
        return f"https://finance.yahoo.com/quote/{ticker.strip()}"
    sym, mkt = parts[0], parts[-1].upper()
    suffix_map = {
        "US": "",    "HK": ".HK", "JP": ".T",  "LN": ".L",
        "GY": ".DE", "SP": ".SI", "ID": ".JK", "AU": ".AX",
        "SW": ".SW", "FP": ".PA", "IM": ".MI", "SQ": ".SI",
    }
    suffix = suffix_map.get(mkt, f".{mkt}")
    return f"https://finance.yahoo.com/quote/{sym}{suffix}"


def _ticker_to_morningstar_url(ticker: str) -> str:
    """Build Morningstar search URL for a ticker."""
    sym = ticker.strip().split()[0]
    return f"https://www.morningstar.com/search?query={sym}"


def render_equity_universe(data: dict):
    """Render the equity coverage universe as a filterable table."""
    stocks = data.get("stocks", [])
    date_str = data.get("date", "")
    if not stocks:
        st.warning("No stock data extracted.")
        return

    df = pd.DataFrame(stocks)

    # Add links
    df["yahoo_url"]      = df["ticker"].apply(_ticker_to_yahoo_url)
    df["morningstar_url"] = df["ticker"].apply(_ticker_to_morningstar_url)

    # Numeric conversion
    for col in ["upside", "div_yield", "pe", "ytd", "mkt_cap"]:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(",","").str.replace("%",""), errors="coerce")

    st.markdown(f"**📊 Equity Coverage Universe** — {date_str} · {len(df):,} stocks")

    # Summary stats
    rating_counts = df["rating"].value_counts()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🟢 Buy",  int(rating_counts.get("Buy", 0)))
    c2.metric("🟡 Hold", int(rating_counts.get("Hold", 0)))
    c3.metric("🔴 Sell", int(rating_counts.get("Sell", 0)))
    c4.metric("Total",   len(df))

    st.markdown("---")

    # Filters
    fc1, fc2, fc3, fc4 = st.columns([2, 2, 2, 2])
    with fc1:
        sel_rating = st.multiselect("Rating", ["Buy","Hold","Sell","UR"],
                                    default=["Buy"], key="eq_rating_filter")
    with fc2:
        regions = sorted(df["region"].dropna().unique())
        sel_region = st.multiselect("Region", regions, default=[], key="eq_region_filter")
    with fc3:
        sectors = sorted(df["sector"].dropna().unique())
        sel_sector = st.multiselect("Sector", sectors, default=[], key="eq_sector_filter")
    with fc4:
        search = st.text_input("Search name / ticker", key="eq_search")

    # Apply filters
    fdf = df.copy()
    if sel_rating:
        fdf = fdf[fdf["rating"].isin(sel_rating)]
    if sel_region:
        fdf = fdf[fdf["region"].isin(sel_region)]
    if sel_sector:
        fdf = fdf[fdf["sector"].isin(sel_sector)]
    if search:
        mask = (fdf["name"].str.contains(search.upper(), na=False) |
                fdf["ticker"].str.contains(search.upper(), na=False))
        fdf = fdf[mask]

    # Sort by upside descending
    fdf = fdf.sort_values("upside", ascending=False, na_position="last")

    # Display columns
    disp = fdf[["ticker","name","region","sector","rating","currency",
                "price","fair_value","upside","div_yield","pe","ytd","uncertainty",
                "yahoo_url","morningstar_url"]].copy()
    disp.columns = ["Ticker","Company","Region","Sector","Rating","Ccy",
                    "Price","Fair Value","Upside %","Div Yld %","P/E","YTD %","Risk",
                    "Yahoo Finance","Morningstar"]
    disp = disp.reset_index(drop=True)

    st.dataframe(
        disp,
        use_container_width=True,
        height=500,
        hide_index=True,
        column_config={
            "Upside %":     st.column_config.NumberColumn(format="%.0f%%"),
            "Div Yld %":    st.column_config.NumberColumn(format="%.1f%%"),
            "P/E":          st.column_config.NumberColumn(format="%.1f"),
            "YTD %":        st.column_config.NumberColumn(format="%.1f%%"),
            "Rating":       st.column_config.TextColumn(width="small"),
            "Yahoo Finance":  st.column_config.LinkColumn("Yahoo Finance",  display_text="📈 Quote"),
            "Morningstar":    st.column_config.LinkColumn("Morningstar",    display_text="⭐ Research"),
        }
    )
    st.caption(f"Showing {len(fdf):,} of {len(df):,} stocks · "
               "BOS individual reports require portal login · Morningstar links open free research page")


def render_generic_research(data: dict):
    """Render a generic research PDF."""
    filename = data.get("_filename", "Document")
    pages = data.get("pages", 0)
    text = data.get("text", "")
    st.markdown(f"**📄 {filename}** — {pages} pages")
    if text:
        # Show first ~600 chars as preview
        preview = " ".join(text[:600].split())
        st.markdown(f"<div style='font-size:11px;color:#475467;line-height:1.5;'>"
                    f"{preview}…</div>", unsafe_allow_html=True)
        with st.expander("Full extracted text"):
            st.text_area("", text, height=400, key=f"txt_{filename[:20]}")


def render_research_library():
    """Render all uploaded research documents."""
    docs = st.session_state.get("research_docs", {})
    if not docs:
        return

    with st.expander(f"📚 Research Library — {len(docs)} document(s)", expanded=True):
        tabs = st.tabs([d.get("_filename", f"Doc {i+1}")[:30]
                        for i, d in enumerate(docs.values())])
        for tab, doc in zip(tabs, docs.values()):
            with tab:
                dtype = doc.get("_doc_type", "generic_research")
                if dtype == "morning_call":
                    render_morning_call(doc)
                elif dtype == "equity_coverage":
                    render_equity_universe(doc)
                else:
                    render_generic_research(doc)


# ── RSS feeds: free, no API key needed ───────────────────────────────────────
RSS_FEEDS = [
    ("Reuters",      "https://feeds.reuters.com/reuters/businessNews"),
    ("Yahoo Finance","https://finance.yahoo.com/news/rssindex"),
    ("CNBC",         "https://www.cnbc.com/id/10000664/device/rss/rss.html"),
    ("MarketWatch",  "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
    ("Investing.com","https://www.investing.com/rss/news.rss"),
]


@st.cache_data(ttl=900)
def load_news_rss(max_per_feed: int = 8) -> pd.DataFrame:
    """Fetch financial news from free RSS feeds. No API key required."""
    rows = []
    cutoff = datetime.utcnow() - timedelta(hours=36)

    for source, url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:max_per_feed]:
                pub = ""
                ts  = None
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    ts  = datetime(*entry.published_parsed[:6])
                    pub = ts.strftime("%Y-%m-%dT%H:%M:%S")
                    if ts < cutoff:
                        continue          # skip older than 36h

                headline = (entry.get("title") or "").strip()
                link     = entry.get("link") or ""
                summary  = (entry.get("summary") or "").strip()
                # Strip any HTML tags from summary
                summary  = summary.replace("<b>","").replace("</b>","").replace("<p>","").replace("</p>","")

                # Only allow articles from known financial news domains
                if link and not _is_financial_url(link):
                    continue

                if headline:
                    rows.append({
                        "headline":       headline,
                        "source":         source,
                        "published_at":   pub,
                        "url":            link,
                        "why_it_matters": summary[:200] if summary else "",
                        "provider":       "RSS",
                    })
        except Exception:
            continue

    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=900)
def load_news_gmail(max_emails: int = 20, lookback_hours: int = 36) -> pd.DataFrame:
    """Read forwarded FT/Bloomberg/work emails from Gmail via IMAP.
    Requires GMAIL_EMAIL and GMAIL_APP_PASSWORD secrets (Gmail App Password, not main password).
    Returns DataFrame with same columns as load_news_rss().
    """
    if not GMAIL_EMAIL or not GMAIL_APP_PASSWORD:
        return pd.DataFrame()

    import imaplib
    import email as email_lib
    import email.header as email_header
    from email.utils import parsedate_to_datetime

    rows = []
    cutoff = datetime.utcnow() - timedelta(hours=lookback_hours)

    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(GMAIL_EMAIL, GMAIL_APP_PASSWORD)
        mail.select("INBOX")

        # Search last lookback_hours worth of emails
        since_date = cutoff.strftime("%d-%b-%Y")
        status, uids = mail.search(None, f'SINCE "{since_date}"')
        if status != "OK":
            return pd.DataFrame()

        uid_list = uids[0].split()
        # Process most recent first, limit to max_emails
        for uid in reversed(uid_list[-max_emails:]):
            try:
                status, msg_data = mail.fetch(uid, "(RFC822)")
                if status != "OK":
                    continue
                raw = msg_data[0][1]
                msg = email_lib.message_from_bytes(raw)

                # Decode subject
                subj_parts = email_header.decode_header(msg.get("Subject", ""))
                subject = ""
                for part, enc in subj_parts:
                    if isinstance(part, bytes):
                        subject += part.decode(enc or "utf-8", errors="replace")
                    else:
                        subject += str(part)
                subject = subject.strip()
                if not subject or len(subject) < 10:
                    continue

                # Decode sender
                from_raw = msg.get("From", "")
                from_parts = email_header.decode_header(from_raw)
                sender = ""
                for part, enc in from_parts:
                    if isinstance(part, bytes):
                        sender += part.decode(enc or "utf-8", errors="replace")
                    else:
                        sender += str(part)
                sender = sender.strip()

                # Determine readable source name from sender domain
                sender_lower = sender.lower()
                if "ft.com" in sender_lower or "financialtimes" in sender_lower:
                    source_name = "Financial Times"
                elif "bloomberg" in sender_lower:
                    source_name = "Bloomberg"
                elif "wsj.com" in sender_lower or "wsj" in sender_lower:
                    source_name = "WSJ"
                elif "economist" in sender_lower:
                    source_name = "The Economist"
                elif "reuters" in sender_lower:
                    source_name = "Reuters (Email)"
                elif "therundown" in sender_lower or "rundown.ai" in sender_lower:
                    source_name = "The Rundown AI"
                elif "morningbrew" in sender_lower or "morning brew" in sender_lower:
                    source_name = "Morning Brew"
                elif "axios" in sender_lower:
                    source_name = "Axios"
                else:
                    continue  # skip unrecognised senders (EuroMillions, spam, etc.) (EuroMillions, spam, etc.)

                # Parse date
                date_str = msg.get("Date", "")
                ts = None
                pub = ""
                try:
                    ts = parsedate_to_datetime(date_str).replace(tzinfo=None)
                    if ts < cutoff:
                        continue
                    pub = ts.strftime("%Y-%m-%dT%H:%M:%S")
                except Exception:
                    pass  # keep email even if date parse fails

                # Extract a short snippet from the body as context
                body_snippet = ""
                try:
                    if msg.is_multipart():
                        for part in msg.walk():
                            ct = part.get_content_type()
                            if ct == "text/plain":
                                payload = part.get_payload(decode=True)
                                if payload:
                                    body_snippet = payload.decode("utf-8", errors="replace")[:400]
                                    break
                    else:
                        payload = msg.get_payload(decode=True)
                        if payload:
                            body_snippet = payload.decode("utf-8", errors="replace")[:400]
                    # Strip obvious junk
                    body_snippet = " ".join(body_snippet.split())[:200]
                except Exception:
                    body_snippet = ""

                # Use Message-ID as a proxy URL (no real URL in email)
                msg_id = msg.get("Message-ID", "").strip()

                rows.append({
                    "headline":       subject,
                    "source":         source_name,
                    "published_at":   pub,
                    "url":            "",        # emails have no URL
                    "why_it_matters": body_snippet,
                    "provider":       "Email",
                })

            except Exception:
                continue

        mail.logout()

    except Exception:
        # Silently fail — Brief works fine without Gmail
        return pd.DataFrame()

    return pd.DataFrame(rows) if rows else pd.DataFrame()


NEWS_COUNT = 12  # fixed — not user-selectable


@st.cache_data(ttl=900)
def load_news(count=NEWS_COUNT):
    placeholder_items = [
        {"headline": "Oil remains central as geopolitical tension stays elevated",       "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Higher oil prices support inflation concerns and affect rates, equities and currencies.", "provider": "Placeholder"},
        {"headline": "Markets remain sensitive to higher-for-longer rate expectations",  "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "If rates stay elevated, bonds and equities may both face valuation pressure.",            "provider": "Placeholder"},
        {"headline": "Risk sentiment mixed across regions",                              "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Regional leadership remains uneven, which supports diversification.",                     "provider": "Placeholder"},
        {"headline": "Dollar strength weighs on emerging-market assets",                 "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "A strong USD tightens financial conditions in EM economies.",                             "provider": "Placeholder"},
        {"headline": "Gold holds near highs amid central bank demand",                   "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Central bank buying underpins gold as a reserve diversification tool.",                   "provider": "Placeholder"},
        {"headline": "China stimulus expectations support commodity demand",             "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Chinese policy stimulus could lift industrial metals and energy prices.",                  "provider": "Placeholder"},
        {"headline": "European equities outperform on valuation re-rating",             "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Cheaper valuations attract flows when US growth expectations moderate.",                   "provider": "Placeholder"},
        {"headline": "Credit spreads stable; no systemic stress signals",               "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Tight spreads suggest credit markets are not pricing in near-term recession risk.",        "provider": "Placeholder"},
        {"headline": "Crypto volatility elevated; Bitcoin tests key resistance",        "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Bitcoin remains a high-beta risk asset, often amplifying broader sentiment moves.",       "provider": "Placeholder"},
        {"headline": "Swiss franc holds safe-haven bid; EUR/CHF under pressure",        "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "CHF strength can compress Swiss equity earnings and affects EUR-denominated portfolios.", "provider": "Placeholder"},
        {"headline": "Global equities digest mixed macro signals",                      "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Uneven growth signals are keeping cross-asset correlations unstable.",                    "provider": "Placeholder"},
        {"headline": "Bond markets price in fewer rate cuts for 2026",                  "source": "Placeholder", "published_at": "", "url": "", "why_it_matters": "Fewer expected cuts support yields but put pressure on equity valuations.",               "provider": "Placeholder"},
    ]
    placeholder_df = pd.DataFrame(placeholder_items)
    placeholder_df["category"] = "Other"

    def classify(headline: str):
        h = (headline or "").lower()
        if any(k in h for k in ["fed", "ecb", "boe", "snb", "inflation", "treasury", "yield", "rates", "cpi", "ppi", "gdp", "fomc"]):
            return "Macro / Rates"
        if any(k in h for k in ["iran", "war", "ceasefire", "russia", "ukraine", "china", "tariff", "trade", "sanctions", "nato", "geopolit"]):
            return "Geopolitics"
        if any(k in h for k in ["oil", "gold", "copper", "crude", "brent", "wti", "commodity", "gas", "silver", "wheat"]):
            return "Commodities"
        if any(k in h for k in ["bitcoin", "crypto", "ethereum", "blockchain", "defi", "token"]):
            return "Crypto"
        if any(k in h for k in ["earnings", "stock", "shares", "equity", "nasdaq", "s&p", "dow", "ipo", "buyback", "dividend"]):
            return "Equities"
        return "Other"

    def score_row(row):
        h = (row.get("headline") or "").lower()
        score = 0
        # High-importance macro/market keywords (score 3)
        for kw in ["fed","federal reserve","fomc","powell","waller","ecb","boe","snb","rba","boj",
                   "inflation","cpi","ppi","gdp","rate cut","rate hike","interest rate",
                   "treasury","yield","tariff","trade war","sanctions","iran","war","ceasefire",
                   "china","oil","gold","dollar","euro","franc"]:
            if kw in h:
                score += 3
        # Standard financial keywords (score 1)
        for kw in ["economy","rates","earnings","market","equity","stock","nasdaq","s&p","bitcoin","silver","copper"]:
            if kw in h:
                score += 1
        # Boost premium sources (FT, Bloomberg emails are high-quality)
        src = (row.get("source") or "").lower()
        if "financial times" in src or "bloomberg" in src:
            score += 4
        # Penalise minor single-company stories (small/unknown companies)
        minor_signals = ["q1 earnings", "q2 earnings", "q3 earnings", "q4 earnings",
                         "raises dividend", "plans delisting", "reports revenue",
                         "charged with fraud", "bankrupt"]
        if any(s in h for s in minor_signals):
            score -= 3
        if row.get("url"):   score += 1
        if row.get("source"): score += 1
        return score

    # ── Fetch from all sources ────────────────────────────────────────────────
    frames = []

    # Marketaux (paid, most relevant if token set)
    if MARKETAUX_API_TOKEN:
        mdf = load_news_marketaux(count * 3)
        if not mdf.empty:
            frames.append(mdf)

    # Free RSS feeds (always attempt)
    rdf = load_news_rss()
    if not rdf.empty:
        frames.append(rdf)

    # Gmail forwarded emails (FT, Bloomberg, work) — if credentials set
    gdf = load_news_gmail()
    if not gdf.empty:
        frames.append(gdf)

    if not frames:
        return placeholder_df.head(count), {
            "live_news": False, "article_count": 0, "url_count": 0,
            "reason": "No live sources returned data — showing placeholders",
        }

    df = pd.concat(frames, ignore_index=True)

    # Deduplicate by normalised headline
    df["headline_key"] = df["headline"].fillna("").str.lower().str.strip()
    df = df.drop_duplicates(subset=["headline_key"]).copy()
    df = df[df["headline"].str.len() > 15].copy()   # drop junk short entries

    df["category"] = df["headline"].apply(classify)
    df["score"]    = df.apply(score_row, axis=1)
    df = df.sort_values(by=["score", "published_at"], ascending=[False, False])
    df = df.drop(columns=["headline_key", "score"], errors="ignore")

    # Ensure variety: cap per category (Equities capped at 2 to avoid minor company noise)
    cat_caps = {"Macro / Rates": 4, "Geopolitics": 3, "Equities": 2, "Commodities": 2, "Crypto": 1, "Other": 1}
    final_rows, seen = [], set()
    for cat in ["Macro / Rates", "Geopolitics", "Equities", "Commodities", "Crypto", "Other"]:
        for _, row in df[df["category"] == cat].head(cat_caps.get(cat, 2)).iterrows():
            if row["headline"] not in seen:
                seen.add(row["headline"])
                final_rows.append(row)
    for _, row in df[~df["headline"].isin(seen)].iterrows():
        if len(final_rows) >= count:
            break
        final_rows.append(row)

    final_df = pd.DataFrame(final_rows).head(count) if final_rows else df.head(count)
    sources_used = ", ".join(sorted(final_df["provider"].fillna("").unique()))

    return final_df, {
        "live_news":     True,
        "article_count": len(final_df),
        "url_count":     int(final_df["url"].fillna("").astype(str).str.len().gt(0).sum()),
        "reason":        f"Live: {sources_used}",
    }


def build_local_news_summary(news_df):
    if news_df is None or news_df.empty:
        return "No news headlines were available, so no summary could be generated."

    groups = {}
    for _, row in news_df.iterrows():
        cat = row.get("category", "Other")
        # Clean headlines through the same media-y filter used on the bullet
        # fallback path, so the recap doesn't carry "Wall Street's Super Bowl
        # Wednesday:" or "Markets morning briefing:" prefixes either.
        cleaned = _clean_headline_for_bullet(row.get("headline", ""))
        if cleaned:
            groups.setdefault(cat, []).append(cleaned)

    ordered = []
    for cat in ["Macro / Rates", "Geopolitics", "Equities", "Commodities", "Crypto", "Other"]:
        if cat in groups:
            ordered.append(f"{cat}: " + "; ".join(groups[cat][:2]))
    return " | ".join(ordered[:4])


def try_gemini_model(model_name, payload):
    """Call one Gemini model. Returns requests.Response."""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
    return requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json={
            "contents": [{"parts": [{"text": payload}]}],
            "generationConfig": {"temperature": 0.2, "maxOutputTokens": 2048},
        },
        timeout=60,
    )


def _safe_json_dumps(obj) -> str:
    """json.dumps that converts numpy/pandas scalars to native Python types."""
    import math
    class SafeEncoder(json.JSONEncoder):
        def default(self, o):
            if hasattr(o, "item"):        # numpy scalar
                return o.item()
            if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
                return None
            return super().default(o)
    return json.dumps(obj, cls=SafeEncoder)


def _strip_json_fences(raw: str) -> str:
    """Remove ```json / ``` markdown fences that models sometimes add."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1]
    if raw.endswith("```"):
        raw = raw.rsplit("```", 1)[0]
    return raw.strip()


# ── Deterministic headline cleanup for the WHAT'S MOVING MARKETS fallback ────
# When the Gemini→Groq cascade fails, the brief used to dump raw news headlines
# verbatim — which let media-y phrasing ("Wall Street's Super Bowl Wednesday",
# "Markets morning briefing:") leak through to clients despite the prompt
# work elsewhere. This pass is a low-tech but always-on safety net: no AI,
# pure string manipulation, predictable output.
_MEDIA_PREFIXES = [
    # Order matters — longer/more-specific prefixes first.
    "Wall Street's Super Bowl Wednesday:",
    "Wall Street's Super Bowl:",
    "Markets morning briefing:",
    "Morning briefing:",
    "Stock market today:",
    "Stocks today:",
    "Market wrap:",
    "Mid-day update:",
    "Closing bell:",
    "Live updates:",
    "Live blog:",
    "Breaking:",
    "Exclusive:",
]
# Tabloid / sensational vocabulary to strip wholesale (case-insensitive).
_MEDIA_PHRASES = [
    "all eyes on ",
    "Super Bowl ",
    "make-or-break ",
    "make or break ",
    "mega week ",
    "showdown ",
    "feast or famine ",
    "roller coaster ",
    "all-eyes-on ",
]

def _clean_headline_for_bullet(text: str) -> str:
    """Strip media-y prefixes / phrases / trailing artefacts from a headline
    so it reads as a sober factual line in the brief's WHAT'S MOVING MARKETS
    section. Used only on the fallback path when AI bullets aren't available.
    Pure string manipulation — never makes anything up."""
    if not isinstance(text, str):
        return ""
    s = text.strip()
    if not s:
        return s
    # Drop leading media prefixes (e.g. "Markets morning briefing: …").
    for pref in _MEDIA_PREFIXES:
        if s.lower().startswith(pref.lower()):
            s = s[len(pref):].lstrip(" :-—–")
            break
    # Strip tabloid phrases inside the body (case-insensitive).
    low = s.lower()
    for phrase in _MEDIA_PHRASES:
        idx = low.find(phrase.lower())
        if idx != -1:
            s = (s[:idx] + s[idx + len(phrase):]).strip()
            low = s.lower()
    # Trailing artefacts: ellipses, source-of-source markers, …
    while s.endswith(("...", "…", " -", " –", " —")):
        s = s.rstrip(".… -–—").strip()
    # Capitalise first letter if we trimmed it off via a prefix removal.
    if s and not s[0].isupper() and s[0].isalpha():
        s = s[0].upper() + s[1:]
    return s


# ── House-style validator (post-AI scrubber) ────────────────────────────────
# Catches banned phrases the prompt rules occasionally miss. Conservative —
# only rewrites EXACT patterns we know are wrong; doesn't touch unfamiliar
# text. Each substitution gets logged for sidebar visibility, so we can see
# what's slipping past the prompt and tune accordingly.
#
# Pairs are (compiled_regex, replacement_string).
_HOUSE_STYLE_PATTERNS: list[tuple[re.Pattern, str]] = [
    # ── Technical jargon — replace with plain-English equivalents ────────────
    (re.compile(r"\bterminal[- ]rate(?:\s+pricing)?(?:\s+higher)?\b", re.IGNORECASE),
     "less room for central banks to cut rates"),
    (re.compile(r"\bbreakeven inflation\b", re.IGNORECASE),
     "market-implied inflation"),
    (re.compile(r"\bbreakevens?\s+widened\b", re.IGNORECASE),
     "market-implied inflation rose"),
    (re.compile(r"\bforward\s+OIS\s+curve\b", re.IGNORECASE),
     "market-implied rate path"),
    (re.compile(r"\bOIS[- ]implied\b", re.IGNORECASE),
     "market-implied"),
    (re.compile(r"\bbelly\s+of\s+the\s+curve\b", re.IGNORECASE),
     "medium-dated bonds"),
    (re.compile(r"\brisk[- ]off\b", re.IGNORECASE),
     "cautious"),
    (re.compile(r"\brisk[- ]on\b", re.IGNORECASE),
     "improved risk appetite"),
    (re.compile(r"\bduration risk\b", re.IGNORECASE),
     "interest-rate sensitivity"),
    (re.compile(r"\bcarry decomposition\b", re.IGNORECASE),
     "return analysis"),
    (re.compile(r"\bhigh[- ]beta\b", re.IGNORECASE),
     "market-sensitive"),

    # ── Statistical / meta language (mostly Chart of the Day) ───────────────
    # "1.6× the usual daily variation" → "well above its normal daily range"
    (re.compile(
        r"\b\d+(?:\.\d+)?\s*[xX×]\s*(?:the\s+)?"
        r"(?:usual|normal|typical|average)\s+(?:daily\s+)?"
        r"(?:variation|volatility|range|move|movement)\b",
        re.IGNORECASE,
    ), "well above its normal daily range"),
    # "z-score of 1.58" or just "z-score" alone
    (re.compile(r"\bwith\s+a\s+z[- ]score\s+of\s+[\d.]+\b", re.IGNORECASE),
     "in an unusually large move"),
    (re.compile(r"\bz[- ]score\b", re.IGNORECASE),
     "an unusually large move"),
    # "a 2.5 sigma move" → "an unusually large move" (consume the leading
    # article and the trailing "move" so article agreement and word
    # repetition are both clean).
    (re.compile(r"\b(?:a|an)\s+\d+(?:\.\d+)?\s*sigma\s+move\b", re.IGNORECASE),
     "an unusually large move"),
    (re.compile(r"\b\d+(?:\.\d+)?\s*sigma\s+move\b", re.IGNORECASE),
     "an unusually large move"),
    (re.compile(r"\b\d+(?:\.\d+)?\s*sigma\b", re.IGNORECASE),
     "an unusually large move"),
    (re.compile(r"\bstatistical\s+deviation\b", re.IGNORECASE),
     "unusual move"),
    (re.compile(r"\bstandard\s+deviations?\b", re.IGNORECASE),
     "normal daily range"),
    (re.compile(r"\btop[- ]news[- ]keywords?\b", re.IGNORECASE),
     "headline themes"),
    (re.compile(r"\btop_news_keywords?\b"),
     "headline themes"),
    (re.compile(r"\btop[_ ]movers(?:[_ ]by[_ ]zscore)?\b", re.IGNORECASE),
     "biggest movers"),
    (re.compile(r"\bd1_pct\b"), "daily move"),

    # ── Filler phrasings ─────────────────────────────────────────────────────
    (re.compile(r"\bas\s+investors\s+(?:assess|monitor|watch|wait|await)\b", re.IGNORECASE),
     "as the market focuses on"),
    (re.compile(r"\binvestors\s+are\s+watching\b", re.IGNORECASE),
     "the market is focused on"),
    # "amid concerns" → "with concerns about" — but if "about" already
    # follows, just remove "amid " and let the existing "about" do the work,
    # otherwise we get "with concerns about about ...".
    (re.compile(r"\bamid\s+concerns\s+about\b", re.IGNORECASE),
     "with concerns about"),
    (re.compile(r"\bamid\s+concerns\b", re.IGNORECASE),
     "with concerns about"),
    (re.compile(r"\bamid\s+uncertainty\b", re.IGNORECASE),
     "with uncertainty"),
    (re.compile(r"\ball[- ]eyes[- ]on\b", re.IGNORECASE),
     "focus on"),
    (re.compile(r"\bWall\s+Street's\s+Super\s+Bowl(?:\s+Wednesday)?\b", re.IGNORECASE),
     "concentrated US tech earnings session"),
    (re.compile(r"\bSuper\s+Bowl\s+Wednesday\b", re.IGNORECASE),
     "concentrated earnings session"),
    (re.compile(r"\bmake[- ]or[- ]break\b", re.IGNORECASE),
     "key"),
    (re.compile(r"\bmega\s+week\b", re.IGNORECASE),
     "key week"),
    # Generic "may impact X" filler the reviewer flagged repeatedly
    (re.compile(r"\bmay\s+impact\s+inflation\s+and\s+(?:economic\s+)?growth\b", re.IGNORECASE),
     "could affect inflation and growth"),
    (re.compile(r"\bmay\s+affect\s+the\s+global\s+economy\b", re.IGNORECASE),
     "could affect global growth"),
    (re.compile(r"\bmay\s+influence\s+trade\s+and\s+investment\s+decisions\b", re.IGNORECASE),
     "could affect cross-border flows"),
    (re.compile(r"\bhas\s+implications\s+for\s+markets\b", re.IGNORECASE),
     "matters for cross-asset positioning"),

    # ── Vague attributions (CotD reviewer flagged these) ─────────────────────
    (re.compile(r"\bdriven\s+by\s+(?:an\s+)?improving\s+economic\s+outlook\b", re.IGNORECASE),
     "supported by today's news flow"),
    (re.compile(r"\bhelped\s+by\s+favou?rable\s+conditions\b", re.IGNORECASE),
     "supported by today's news flow"),
    (re.compile(r"\bsupported\s+by\s+positive\s+sentiment\b", re.IGNORECASE),
     "supported by improved risk appetite"),
    (re.compile(r"\bon\s+global\s+growth\s+optimism\b", re.IGNORECASE),
     "on improved growth expectations"),
    (re.compile(r"\bamid\s+a\s+constructive\s+backdrop\b", re.IGNORECASE),
     "in a constructive market environment"),

    # ── Geopolitical absolutism ──────────────────────────────────────────────
    (re.compile(
        r"\b(?:US[- ]Iran\s+)?peace\s+talks\s+"
        r"(?:were|are|have\s+been)?\s*"
        r"(?:reportedly\s+)?"
        r"(?:canceled|cancelled|called\s+off)\b",
        re.IGNORECASE,
    ), "hopes for near-term US-Iran de-escalation weakened"),
    (re.compile(r"\b(?:the\s+)?war\s+(?:has\s+)?ended\b", re.IGNORECASE),
     "tensions reportedly eased"),
    (re.compile(r"\bceasefire\s+(?:was\s+|has\s+been\s+)?agreed\b", re.IGNORECASE),
     "reports of de-escalation"),
    (re.compile(r"\bsanctions\s+(?:were\s+|have\s+been\s+)?lifted\b", re.IGNORECASE),
     "reports of sanctions review"),

    # ── Powell "last meeting" claim (unverified) ─────────────────────────────
    (re.compile(r"\bPowell's\s+last\s+(?:Fed\s+)?meeting\b", re.IGNORECASE),
     "Powell's upcoming Fed meeting"),

    # ── Single-stock framing (reviewer flagged Qualcomm/Nvidia/Verizon) ─────
    # Rewrite tabloid-style mega-cap headlines to sector-level framing.
    # The brief discusses market behaviour, not single-stock storylines.
    (re.compile(r"\b(?:Qualcomm|Nvidia|NVDA|QCOM)\s+stock\s+soars\b", re.IGNORECASE),
     "AI and semiconductor shares are firm"),
    (re.compile(r"\b(?:Nvidia|NVDA)\s+tops\s+\$\d+(?:\.\d+)?\s*trillion(?:\s+again)?\b", re.IGNORECASE),
     "AI and semiconductor shares lead technology gains"),
    (re.compile(r"\b(?:Apple|Microsoft|Alphabet|Google|Meta|Amazon|Tesla|Nvidia|Qualcomm)\s+stock\s+(?:soars|surges|jumps|rallies)\b", re.IGNORECASE),
     "mega-cap technology shares are supportive"),

    # ── Truncation artefacts ─────────────────────────────────────────────────
    # Catch sentences that end on a dangling conjunction with a period
    # appended (e.g. "Watch whether this move continues or."). These are
    # almost always artefacts of an upstream char-cut — strip the dangle
    # and the orphan period.
    (re.compile(r",?\s+(?:or|and|but|while|whether|with|to|in|of)\s*\.\s*$",
                re.IGNORECASE), "."),
    (re.compile(r",?\s+(?:or|and|but|while|whether|with|to|in|of)\s*$",
                re.IGNORECASE), "."),

    # ── Advice phrasings ─────────────────────────────────────────────────────
    (re.compile(r"\binvestors\s+should\b", re.IGNORECASE),
     "the data suggests"),
    (re.compile(r"\bwe\s+recommend\b", re.IGNORECASE),
     "research suggests"),
    (re.compile(r"\bthis\s+is\s+an?\s+opportunity\s+to\b", re.IGNORECASE),
     "the data points to"),
    (re.compile(r"\btime\s+to\s+(?:buy|sell|consider)\b", re.IGNORECASE),
     "worth monitoring"),
]


def _scrub_ai_text(text: str) -> tuple[str, list[str]]:
    """Apply the banned-phrase regex pass to a piece of AI-generated text.
    Returns (cleaned_text, list_of_human_readable_replacement_notes).

    This is a safety net for the prompt rules — when the model occasionally
    produces a banned phrase despite the instructions, the validator
    rewrites it before render. Conservative: only touches exact patterns
    we know are wrong. Anything not in the pattern table passes through
    unchanged. Result is post-processed to collapse any double-spaces and
    fix space-before-punctuation that substitutions can leave behind.
    """
    if not isinstance(text, str) or not text.strip():
        return (text or ""), []
    cleaned = text
    fixes: list[str] = []
    for pat, repl in _HOUSE_STYLE_PATTERNS:
        new_cleaned, n = pat.subn(repl, cleaned)
        if n:
            # Show the first match so we can see what got rewritten.
            sample = pat.search(cleaned)
            sample_str = sample.group(0) if sample else pat.pattern
            fixes.append(f"{sample_str!r} → {repl!r} ({n}x)")
            cleaned = new_cleaned
    # Whitespace / punctuation cleanup after substitutions.
    cleaned = re.sub(r" {2,}", " ", cleaned)
    cleaned = re.sub(r"\s+([.,;:!?])", r"\1", cleaned)
    cleaned = cleaned.strip()
    return cleaned, fixes


def _smart_truncate_at_sentence(text: str, max_chars: int) -> str:
    """Truncate text at the last complete sentence boundary at or before
    max_chars. Falls back to the last word boundary if no sentence end is
    available, then strips dangling conjunctions/prepositions so the result
    never reads like a chopped fragment.

    The reviewer flagged a CotD output cut mid-clause: 'Watch whether this
    move continues or.' — that came from a naive [:350] cut on a longer
    paragraph. This helper makes the truncation aware of sentence and word
    boundaries so the close always reads as complete copy.
    """
    if not isinstance(text, str):
        return ""
    text = text.strip()
    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    # Prefer a sentence boundary if one exists at >= 60% of allowed length.
    best = -1
    for terminator in (". ", "! ", "? "):
        idx = truncated.rfind(terminator)
        if idx > best:
            best = idx
    if best >= int(max_chars * 0.6):
        return text[:best + 1].rstrip()
    # No good sentence boundary — fall back to the last word boundary.
    last_space = truncated.rfind(" ")
    if last_space > 0:
        truncated = truncated[:last_space]
    truncated = truncated.rstrip(",;:")
    # Strip dangling conjunctions / prepositions / articles that signal an
    # incomplete clause. Iterating because some endings are stacked
    # ("...as the move continues or whether the").
    DANGLING = (
        " or", " and", " but", " while", " whether", " with", " of",
        " to", " in", " on", " as", " from", " for", " by", " at",
        " a", " an", " the", " is", " are", " was", " were", " has",
        " have", " be", " been",
    )
    changed = True
    while changed:
        changed = False
        for trail in DANGLING:
            if truncated.lower().endswith(trail):
                truncated = truncated[: -len(trail)].rstrip(",;: ")
                changed = True
                break
    if truncated and truncated[-1] not in ".!":
        truncated += "."
    return truncated


def _scrub_writing_dict(out: dict) -> list[str]:
    """Scrub the AI-returned `writing` dict in-place. Returns a flat log of
    every substitution made across all string fields, for surfacing in
    diagnostics. Mutates `out` so callers see the cleaned strings."""
    log: list[str] = []
    if not isinstance(out, dict):
        return log
    # Top-level string fields.
    for fld in ("headline", "subheadline", "news_summary"):
        v = out.get(fld)
        if isinstance(v, str):
            cleaned, fixes = _scrub_ai_text(v)
            if cleaned != v:
                out[fld] = cleaned
                log.extend(f"{fld}: {fx}" for fx in fixes)
    # news_bullets (list of strings).
    nb = out.get("news_bullets")
    if isinstance(nb, list):
        new_list = []
        for i, b in enumerate(nb):
            if isinstance(b, str):
                cleaned, fixes = _scrub_ai_text(b)
                new_list.append(cleaned)
                log.extend(f"news_bullets[{i}]: {fx}" for fx in fixes)
            else:
                new_list.append(b)
        out["news_bullets"] = new_list
    # portfolio_implications (list of strings).
    pi = out.get("portfolio_implications")
    if isinstance(pi, list):
        new_list = []
        for i, b in enumerate(pi):
            if isinstance(b, str):
                cleaned, fixes = _scrub_ai_text(b)
                new_list.append(cleaned)
                log.extend(f"portfolio_implications[{i}]: {fx}" for fx in fixes)
            else:
                new_list.append(b)
        out["portfolio_implications"] = new_list
    # executive_summary {lead, bullets}.
    es = out.get("executive_summary")
    if isinstance(es, dict):
        if isinstance(es.get("lead"), str):
            cleaned, fixes = _scrub_ai_text(es["lead"])
            if cleaned != es["lead"]:
                es["lead"] = cleaned
                log.extend(f"executive_summary.lead: {fx}" for fx in fixes)
        if isinstance(es.get("bullets"), list):
            new_b = []
            for i, b in enumerate(es["bullets"]):
                if isinstance(b, str):
                    cleaned, fixes = _scrub_ai_text(b)
                    new_b.append(cleaned)
                    log.extend(f"executive_summary.bullets[{i}]: {fx}" for fx in fixes)
                else:
                    new_b.append(b)
            es["bullets"] = new_b
    return log


def _dedupe_headlines(headlines: list[str], limit: int = 5) -> list[str]:
    """Drop near-duplicate headlines (same first 6 significant words). The raw
    news feed often carries the same story from multiple sources with slightly
    different wording; client newsletter should only show each story once."""
    seen_keys: set[str] = set()
    out: list[str] = []
    for h in headlines:
        if not h:
            continue
        words = [w for w in h.lower().split() if len(w) > 2][:6]
        key = " ".join(words)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        out.append(h)
        if len(out) >= limit:
            break
    return out
    return requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json={
            "contents": [{"parts": [{"text": payload}]}],
            "generationConfig": {"temperature": 0.2, "maxOutputTokens": 2048},
        },
        timeout=60,
    )


def try_groq(payload_obj: dict):
    """Call Groq OpenAI-compatible endpoint. payload_obj is the already-parsed
    dict. Generic: pass every key through as labelled JSON so Groq sees the
    whole payload regardless of shape (news brief, research summarisation,
    chart-of-day, etc.)."""
    instruction = payload_obj.get("instruction", "")
    # Dump every NON-instruction key as a labelled JSON block.
    data_blocks = []
    for key, val in payload_obj.items():
        if key == "instruction":
            continue
        try:
            rendered = json.dumps(val, ensure_ascii=False, default=str)
        except Exception:
            rendered = str(val)
        data_blocks.append(f"{key}:\n{rendered}")

    user_msg = instruction
    if data_blocks:
        user_msg = f"{instruction}\n\n" + "\n\n".join(data_blocks)

    r = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model":       GROQ_MODEL,
            "messages":    [{"role": "user", "content": user_msg}],
            "temperature": 0.2,
            "max_tokens":  2048,
        },
        timeout=60,
    )
    return r


@st.cache_data(ttl=1800, show_spinner=False)
def ai_generate_json(payload: str):
    """Try Gemini first (all fallback models), then Groq. Returns (parsed_dict, reason_str)."""
    errors = []

    # ── 1. Gemini chain ───────────────────────────────────────────────────────
    if GEMINI_API_KEY:
        models_to_try = [GEMINI_MODEL] + [m for m in GEMINI_FALLBACK_MODELS if m != GEMINI_MODEL]
        for model_name in models_to_try:
            for attempt in range(2):
                try:
                    r = try_gemini_model(model_name, payload)
                    if r.ok:
                        data       = r.json()
                        if data.get("promptFeedback", {}).get("blockReason"):
                            errors.append(f"Gemini/{model_name}: blocked")
                            break
                        candidates = data.get("candidates", [])
                        if not candidates:
                            errors.append(f"Gemini/{model_name}: no candidates")
                            break
                        raw     = "".join(p.get("text","") for p in candidates[0].get("content",{}).get("parts",[])).strip()
                        cleaned = _strip_json_fences(raw)
                        if not cleaned:
                            errors.append(f"Gemini/{model_name}: empty")
                            break
                        try:
                            return json.loads(cleaned), f"Gemini OK ({model_name})"
                        except Exception:
                            errors.append(f"Gemini/{model_name}: bad JSON")
                            break
                    else:
                        if r.status_code == 429:
                            errors.append(f"Gemini/{model_name}: 429 quota")
                            break          # skip to next model immediately
                        if r.status_code in {500, 503} and attempt == 0:
                            time.sleep(3)
                            continue
                        errors.append(f"Gemini/{model_name}: HTTP {r.status_code}")
                        break
                except Exception as e:
                    errors.append(f"Gemini/{model_name}: {type(e).__name__}")
                    if attempt == 0:
                        time.sleep(2)
                    else:
                        break

    # ── 2. Groq fallback ──────────────────────────────────────────────────────
    if GROQ_API_KEY:
        try:
            payload_obj = json.loads(payload)
            r = try_groq(payload_obj)
            if r.ok:
                raw     = r.json()["choices"][0]["message"]["content"].strip()
                cleaned = _strip_json_fences(raw)
                return json.loads(cleaned), f"Groq OK ({GROQ_MODEL})"
            else:
                errors.append(f"Groq: HTTP {r.status_code}")
        except Exception as e:
            errors.append(f"Groq: {type(e).__name__}: {str(e)[:80]}")

    return None, "AI failed: " + " | ".join(errors[:6])


def _ai_generate_json_uncached(payload: str):
    """Same as ai_generate_json but NOT wrapped in @st.cache_data. Used for
    the research-summary loop where we don't want stale None responses from
    earlier failures to be served for 30 minutes.

    Smart escalation: on a Gemini 429 (quota) or a string of 404s (dead
    models), break out of the Gemini loop entirely and jump to Groq. No
    point burning API calls on deprecated models."""
    errors = []
    groq_shortcut = False  # set True to skip remaining Gemini attempts

    if GEMINI_API_KEY and not groq_shortcut:
        models_to_try = [GEMINI_MODEL] + [m for m in GEMINI_FALLBACK_MODELS if m != GEMINI_MODEL]
        consecutive_404s = 0
        for model_name in models_to_try:
            if groq_shortcut:
                break
            for attempt in range(2):
                try:
                    r = try_gemini_model(model_name, payload)
                    if r.ok:
                        consecutive_404s = 0
                        data = r.json()
                        if data.get("promptFeedback", {}).get("blockReason"):
                            errors.append(f"Gemini/{model_name}: blocked")
                            break
                        candidates = data.get("candidates", [])
                        if not candidates:
                            errors.append(f"Gemini/{model_name}: no candidates")
                            break
                        raw = "".join(
                            p.get("text", "") for p in
                            candidates[0].get("content", {}).get("parts", [])
                        ).strip()
                        cleaned = _strip_json_fences(raw)
                        if not cleaned:
                            errors.append(f"Gemini/{model_name}: empty")
                            break
                        try:
                            return json.loads(cleaned), f"Gemini OK ({model_name})"
                        except Exception:
                            errors.append(f"Gemini/{model_name}: bad JSON")
                            break
                    else:
                        if r.status_code == 429:
                            # Primary model quota-hit → skip the rest of Gemini,
                            # go straight to Groq. We'd only burn time on more
                            # 429s or on deprecated-model 404s.
                            errors.append(f"Gemini/{model_name}: 429 quota → Groq")
                            groq_shortcut = True
                            break
                        if r.status_code == 404:
                            # Deprecated model. After 2 404s in a row, bail to Groq.
                            errors.append(f"Gemini/{model_name}: 404")
                            consecutive_404s += 1
                            if consecutive_404s >= 2:
                                groq_shortcut = True
                            break
                        if r.status_code in {500, 503} and attempt == 0:
                            time.sleep(3)
                            continue
                        errors.append(f"Gemini/{model_name}: HTTP {r.status_code}")
                        break
                except Exception as e:
                    errors.append(f"Gemini/{model_name}: {type(e).__name__}")
                    if attempt == 0:
                        time.sleep(2)
                    else:
                        break

    if GROQ_API_KEY:
        # Try primary Groq model, then fall back to the small fast model if
        # the large one hits TPM / RPM limits. The small model has 10-20x
        # higher token budget on free tier, so it reliably absorbs overflow.
        groq_models = [GROQ_MODEL]
        if "llama-3.1-8b-instant" not in GROQ_MODEL:
            groq_models.append("llama-3.1-8b-instant")

        for gm in groq_models:
            try:
                payload_obj = json.loads(payload)
                r = _try_groq_model(payload_obj, gm)
                if r.ok:
                    raw = r.json()["choices"][0]["message"]["content"].strip()
                    cleaned = _strip_json_fences(raw)
                    try:
                        return json.loads(cleaned), f"Groq OK ({gm})"
                    except Exception:
                        errors.append(f"Groq/{gm}: bad JSON")
                        continue
                else:
                    errors.append(f"Groq/{gm}: HTTP {r.status_code}")
                    # On 429, keep trying the smaller model. On other errors, stop.
                    if r.status_code != 429:
                        break
            except Exception as e:
                errors.append(f"Groq/{gm}: {type(e).__name__}: {str(e)[:60]}")

    return None, "AI failed: " + " | ".join(errors[:8])


def _try_groq_model(payload_obj: dict, model_name: str):
    """Call a specific Groq model by name (overrides GROQ_MODEL). Returns the
    requests.Response so the caller handles status codes."""
    instruction = payload_obj.get("instruction", "")
    data_blocks = []
    for key, val in payload_obj.items():
        if key == "instruction":
            continue
        try:
            rendered = json.dumps(val, ensure_ascii=False, default=str)
        except Exception:
            rendered = str(val)
        data_blocks.append(f"{key}:\n{rendered}")

    user_msg = instruction + ("\n\n" + "\n\n".join(data_blocks) if data_blocks else "")

    return requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model":       model_name,
            "messages":    [{"role": "user", "content": user_msg}],
            "temperature": 0.2,
            "max_tokens":  2048,
        },
        timeout=60,
    )


def build_writing(news_df, snapshot, use_gemini, research_context=""):
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
        "executive_summary": {"lead": "", "bullets": []},
        "portfolio_implications": [],
    }

    if not use_gemini:
        return fallback, {"gemini_used": False, "reason": "Checkbox off"}

    try:
        # Build a readable market snapshot keyed by asset name with d1 moves
        snap_rows = []
        for _, row in snapshot.iterrows():
            d1 = row.get("d1")
            ytd = row.get("ytd")
            if d1 is not None and not pd.isna(d1):
                snap_rows.append({
                    "asset":  str(row.get("label","")),
                    "group":  str(row.get("group","")),
                    "d1_pct": round(float(d1), 2),
                    "ytd_pct": round(float(ytd), 2) if ytd is not None and not pd.isna(ytd) else None,
                })
        snap_rows = snap_rows[:20]

        head_rows = (
            news_df[["headline", "source", "category"]].fillna("").to_dict(orient="records")
            if news_df is not None and not news_df.empty else []
        )
        research_note = (
            " Where a headline's topic is directly addressed in research_context, "
            "you MAY reference a specific analyst view or rating change — but "
            "ONLY if that exact view/rating appears verbatim in research_context. "
            "Quote the source bank name exactly as it appears in research_context. "
            "If research_context doesn't cover a headline, do NOT invent an "
            "analyst angle to pad the bullet."
            if research_context else ""
        )
        payload_dict = {
            "instruction": (
                "Return ONLY raw JSON — no markdown, no code fences, no preamble. "
                "Keys required: headline, subheadline, news_summary, news_bullets, "
                "executive_summary, portfolio_implications. "
                "\n\n"
                "HARD GROUNDING RULES — violations corrupt the brief:\n"
                "* NEVER invent facts. Every factual claim — event, bank name, "
                "ticker, price target, rating change, analyst call — MUST be "
                "traceable to the supplied 'headlines' or 'research_context' "
                "inputs. If it's not in the input, do not mention it.\n"
                "* NEVER use placeholders like 'X', 'Company Y', 'Bank Z'. If "
                "you don't have a concrete name/number from the input, omit "
                "that bullet entirely.\n"
                "* NEVER reference analysts, rating changes, or price targets "
                "unless they appear verbatim in the input. No 'Morgan Stanley "
                "upgraded …' unless Morgan Stanley is in the headlines or "
                "research_context as actually saying that.\n"
                "* Percent moves MUST come from market_snapshot.d1_pct — do "
                "NOT estimate or round to a different number. Quote the exact "
                "figure.\n"
                "* If a headline is vague or you're not sure of a detail, "
                "keep the bullet short and factual rather than embellishing.\n"
                "* NEVER make temporal/historical claims unless they appear "
                "verbatim in the headlines. Forbidden phrasings without source "
                "evidence: 'Powell's last meeting', 'final rate decision', "
                "'farewell', 'first since YYYY', 'highest in N years', "
                "'biggest move ever', 'record close'. If the headline doesn't "
                "say it, do not say it.\n"
                "* NEVER state geopolitical events as definite outcomes — "
                "even with the word 'reportedly' as a softener. The "
                "following phrasings are HARD-FORBIDDEN regardless of what "
                "any individual headline appears to say:\n"
                "  - 'peace talks were canceled'  →  use 'hopes for "
                "near-term de-escalation weakened'\n"
                "  - 'peace talks were reportedly canceled'  →  same, the "
                "'reportedly' does NOT make it acceptable\n"
                "  - 'war ended' / 'war started'  →  use 'tensions "
                "intensified' / 'tensions eased' tied to a specific event\n"
                "  - 'ceasefire agreed'  →  use 'reports of de-escalation "
                "talks' or quote the headline source explicitly\n"
                "  - 'sanctions lifted'  →  use 'reports of sanctions "
                "review' unless the headline is explicit\n"
                "  - 'X attacked Y'  →  use 'reports of strikes between "
                "X and Y'\n"
                "Frame ALL geopolitical content as MARKET REACTIONS to "
                "evolving narratives, not as confirmed events. Bad: "
                "'oil rose after US-Iran peace talks were canceled'. "
                "Good: 'oil rose as hopes for near-term US-Iran "
                "de-escalation weakened'. The brief reports market behaviour "
                "tied to news flow; it does not adjudicate the news.\n"
                "\n"
                "AUDIENCE — this is read by educated private-bank clients, "
                "NOT by sell-side fixed-income desk specialists. The brief "
                "is at INVESTOR-LETTER level, not trading-desk research level. "
                "\n"
                "HARD-FORBIDDEN TECHNICAL JARGON. Do NOT use any of these "
                "phrases — substitute the plain-English version every time:\n"
                "* 'terminal-rate pricing' / 'terminal rate pricing' / "
                "'pushing terminal rates higher'  →  use 'less room for the "
                "ECB/Fed to cut rates' or 'narrowing the path for rate cuts'\n"
                "* 'breakevens widened' / 'breakeven inflation rose'  →  use "
                "'market-implied inflation rose'\n"
                "* 'duration risk' / 'long duration'  →  use 'sensitivity "
                "of bond prices to rate moves' or 'long-dated bonds'\n"
                "* 'forward OIS curve' / 'OIS-implied'  →  use 'market-implied "
                "rate path'\n"
                "* 'belly of the curve' / 'curve steepener' / 'curve flattener'"
                "  →  describe as 'medium-dated bonds' / 'short rates fell "
                "more than long' etc.\n"
                "* 'carry decomposition' / 'carry trade'  →  describe what "
                "the trade IS in plain terms\n"
                "* 'risk-off' / 'risk-on'  →  use 'cautious / defensive "
                "positioning' or 'risk appetite improved'\n"
                "* 'beta to risk' / 'high-beta'  →  use 'sensitive to market "
                "moves'\n"
                "If a technical term is unavoidable AND not on the banned "
                "list, attach a 5-8 word plain explanation in parentheses on "
                "first use. Test: would a sophisticated client without a "
                "finance degree understand this on first read?\n"
                "\n"
                "HOUSE STYLE — this is a private-bank client newsletter, "
                "not a media wire. Apply rigorously:\n"
                "* OUTPUT 4 to 5 bullets, NOT more. Quality over volume. "
                "Drop to 4 (or even 3) if the day's themes overlap.\n"
                "* DO NOT REPEAT THEMES across bullets. If oil, ECB easing, "
                "and inflation are interconnected, COMBINE them into a single "
                "tighter bullet rather than producing two near-duplicates. "
                "Bad: bullet 1 = 'higher oil pressures ECB easing path', "
                "bullet 2 = 'inflation risk reduces room for rate cuts'. "
                "Good: a single bullet 'Higher oil keeps inflation risk live "
                "and narrows the room for the ECB to cut rates'.\n"
                "* VARY sentence structure across bullets. Do NOT open every "
                "bullet with the same template. Specifically, do NOT use the "
                "pattern '<headline>, with <index> up X%, as investors <verb>...' "
                "more than once across the bullet set.\n"
                "* BAN filler phrases: 'as investors assess', 'as investors "
                "monitor', 'as investors watch', 'investors are watching', "
                "'amid concerns', 'amid uncertainty', 'amid expectations', "
                "'all eyes on', 'investors await'. Replace with concrete "
                "subject-verb-object writing.\n"
                "* BAN advice language: 'investors should', 'we recommend', "
                "'this is an opportunity', 'consider buying', 'time to'. "
                "This brief reports; it does not advise.\n"
                "* BAN tabloid framing: 'Super Bowl', 'mega week', 'make-or-"
                "break', 'battle', 'showdown', 'feast or famine', 'roller "
                "coaster'. Replace with sober factual language.\n"
                "* PREFER the active voice and concrete subjects. Example of "
                "house style: 'US equities were supported by technology "
                "strength ahead of major earnings. The Nasdaq 100 led, rising "
                "1.95%, while geopolitical talks remained the second-order "
                "driver.' Avoid: 'with Nasdaq up 1.95% as investors monitor "
                "US-Iran negotiations.'\n"
                "\n"
                "news_bullets: 4 to 5 plain-English bullets.\n"
                "\n"
                "BULLET TOPIC SELECTION — bias toward macro / cross-asset, "
                "NOT individual stocks. The brief is a market briefing, not "
                "a stock-pick newsletter. Prefer headlines about: central "
                "banks, payrolls / CPI / PMI / GDP releases, oil and "
                "commodities, currencies, geopolitics affecting markets, "
                "credit spreads, broad sector themes (e.g. 'mega-cap "
                "technology earnings season'). Single-stock items (e.g. "
                "'Verizon Q1 results', 'Booking Holdings price target cut') "
                "should NOT take a bullet slot UNLESS the move is "
                "index-moving (a Mag-7 mega-cap earnings print, a >$50bn "
                "M&A deal, a sector-defining product launch). When in doubt, "
                "drop the single-stock item and use the slot for a macro "
                "point. If only single-stock headlines are available, drop "
                "to 3 bullets rather than padding.\n"
                "\n"
                "FRAME MEGA-CAP MOVES AT THE SECTOR LEVEL, NOT THE COMPANY "
                "LEVEL. Even when an individual mega-cap (Nvidia, Apple, "
                "Microsoft, Amazon, Alphabet, Meta, Tesla, Qualcomm, etc.) "
                "is the catalyst, the bullet must speak about the "
                "sector/index, not the company. The brief discusses market "
                "behaviour, not single-stock storylines. Bad: 'Qualcomm "
                "stock soars as Nvidia tops $5 trillion again' (sounds "
                "like a tabloid headline). Good: 'AI and semiconductor "
                "shares continue to support technology sentiment ahead of "
                "major earnings.' If the headline is mega-cap-specific, "
                "rewrite it as: 'Mega-cap technology strength supports "
                "equities ahead of earnings, with the Nasdaq 100 up 1.95%.' "
                "Do NOT name the individual company unless its earnings "
                "print is THE day's main scheduled event.\n"
                "\n"
                "Each bullet must "
                "(1) name the driver (from a supplied headline), "
                "(2) state the market impact using ACTUAL numbers from market_snapshot "
                "where relevant (e.g. 'S&P 500 +0.80%, Nasdaq +1.40%'), "
                "(3) state a SPECIFIC causal mechanism — not a generic 'may impact' "
                "filler. The mechanism must name a real channel "
                "(rate-cut path, disinflation narrative, equity-risk premium, "
                "credit spreads, growth forecasts, terminal-rate expectations, "
                "currency-hedging cost, refinancing wall, earnings revisions). "
                "Add a parenthetical clarification for any jargon "
                "(e.g. 'Treasury yields fell (meaning existing bond PRICES ROSE)').\n"
                "\n"
                "BANNED FILLER CLAUSES (do not produce these — they teach "
                "clients nothing):\n"
                "* 'may impact inflation and economic growth'\n"
                "* 'may affect the global economy'\n"
                "* 'may influence trade and investment decisions'\n"
                "* 'has implications for markets'\n"
                "* 'macroeconomic factors at play'\n"
                "* 'global developments'\n"
                "* 'investors are watching'\n"
                "Replace these with concrete mechanisms. Example of GOOD style: "
                "'Higher oil prices complicate the disinflation narrative, "
                "narrowing the path for ECB easing and pushing the euro-area "
                "terminal-rate pricing higher.' Example of BAD style: 'Higher "
                "oil prices may impact inflation and growth.'\n"
                "\n"
                "portfolio_implications: 3 to 4 cross-asset implication "
                "bullets at the macro level — NOT one-per-research-row, NOT "
                "individualized advice. Each bullet captures a single broad "
                "consequence of the day's themes for diversified portfolios. "
                "Each bullet ≤ 18 words, ends with a period. Worked examples:\n"
                "* 'Oil strength supports inflation hedges but pressures "
                "consumers and import-sensitive economies.'\n"
                "* 'Technology earnings remain the key test for equity "
                "momentum this week.'\n"
                "* 'A more resilient USD could pressure emerging markets "
                "and non-US currencies.'\n"
                "* 'High-yield credit looks less attractive while oil "
                "volatility and rate uncertainty persist.'\n"
                "Same grounding rules apply (only headlines / market_snapshot "
                "/ research_context; no advice phrasing; no definitive "
                "geopolitical claims). The implications must follow from the "
                "day's actual data, not from general market knowledge.\n"
                "\n"
                "executive_summary: the top-of-brief investment synopsis. "
                "Shape: {\"lead\": \"<2 sentence today's-message paragraph>\", "
                "\"bullets\": [\"<bullet 1>\", \"<bullet 2>\", \"<bullet 3>\"]}. "
                "\n"
                "The lead is TWO sentences (35-55 words total) written in "
                "investment-professional voice. Sentence 1 names the prevailing "
                "market tone with a colour adjective AND the dominant driver(s). "
                "Sentence 2 names the next-watch items (earnings / central "
                "banks / macro data / specific events).\n"
                "\n"
                "TONE ADJECTIVE MUST MATCH THE DATA. Look at market_snapshot:\n"
                "* Most major equity indices green AND vol low → 'constructive', "
                "'resilient', 'risk-on'.\n"
                "* Equities mixed, some up some down → 'mixed but resilient' "
                "(if leaders are up) or 'mixed with crosscurrents'.\n"
                "* Most indices red, vol up → 'cautious', 'defensive', "
                "'risk-off'.\n"
                "Do NOT call the day 'cautious' if S&P, Nasdaq, and global "
                "equities are positive — that contradicts the tape. Pick the "
                "adjective FROM the data, not from a generic vocabulary.\n"
                "\n"
                "SEPARATE DIVERGENT DRIVERS. When two drivers push in OPPOSITE "
                "directions (oil pressures, tech supports), use a 'while/with' "
                "framing — never collapse them into 'driven by X and Y'. "
                "Bad: 'driven by elevated oil prices and technology strength' "
                "(implies both push the same way). "
                "Good: 'as technology strength supports equities while elevated "
                "oil prices keep inflation risk in focus'.\n"
                "\n"
                "INTRA-CLASS DIVERGENCE — handle oil benchmarks specially. "
                "When WTI Crude and Brent Crude move in opposite directions "
                "in the day's market_snapshot (one positive, one negative), "
                "or when their 1D values differ by more than ~3 percentage "
                "points, do NOT describe oil as moving in one direction. "
                "Instead use level-based framing tied to the LEVEL, not the "
                "1D move: 'oil remains elevated despite mixed daily moves "
                "in crude benchmarks' or 'crude benchmarks were mixed "
                "intraday but levels stayed elevated'. The same applies to "
                "Gold vs Silver if they diverge sharply.\n"
                "\n"
                "Avoid bland openings like 'Markets are mixed today' — be "
                "specific about WHY. Example of GOOD lead: 'Global markets "
                "start the week with a mixed but resilient tone, as technology "
                "strength supports equities while elevated oil prices keep "
                "inflation risk in focus. Attention turns to mega-cap earnings, "
                "central-bank communication, and Friday's US payrolls print.'\n"
                "\n"
                "The 3 bullets are: (1) the dominant equity-market driver, "
                "(2) the principal cross-asset risk to monitor, (3) the key "
                "data/event on the calendar. Each bullet is SHORT — strictly "
                "8 to 12 words — written as a punchy at-a-glance line, ends "
                "with a period, and contains a specific noun (sector, asset, "
                "data print, central bank). Never abstract phrasing like "
                "'macroeconomic factors' or 'global developments'.\n"
                "\n"
                "Worked examples of GOOD bullets (8-12 words each):\n"
                "* 'Technology resilience is supporting equity sentiment.'\n"
                "* 'Oil remains the main cross-asset risk this week.'\n"
                "* 'US payrolls are the key macro data point on Friday.'\n"
                "Worked examples of BAD bullets (too long, too vague, "
                "preachy):\n"
                "* 'Investors should monitor technology stocks given the "
                "earnings calendar and broader macro environment, with the "
                "Nasdaq 100 rising 1.95% as the principal driver.' (too long)\n"
                "* 'Macroeconomic factors remain in focus.' (too vague — no "
                "specific noun)\n"
                "* 'Investors should consider the implications of higher "
                "oil.' (advice phrasing)\n"
                "\n"
                "Same grounding rules as the rest of the brief: only use "
                "facts from headlines / market_snapshot / research_context. "
                "No advice phrasing ('investors should…'). No definitive "
                "geopolitical claims ('peace talks were canceled') unless a "
                "headline says so verbatim — use hedged framing ('hopes for "
                "de-escalation faded', 'talks reportedly stalled')."
                + research_note +
                " Be factual and concise. No preamble, no filler."
            ),
            "headlines":       head_rows,
            "market_snapshot": snap_rows,
        }
        if research_context:
            payload_dict["research_context"] = research_context
        payload = _safe_json_dumps(payload_dict)
    except Exception as e:
        return {**fallback, "news_bullets": [], "article_angles": []}, {"gemini_used": False, "reason": f"Payload build error: {e}"}

    out, reason = ai_generate_json(payload)
    # House-style validator: scrub banned phrases before they reach render.
    # Mutates `out` in place. Log is stashed in session_state for sidebar
    # diagnostics so we can see what's slipping past the prompt rules.
    if isinstance(out, dict):
        scrub_log = _scrub_writing_dict(out)
        if scrub_log:
            try:
                import streamlit as _st
                _st.session_state["_house_style_scrub_log"] = scrub_log
            except Exception:
                pass
    if isinstance(out, dict) and isinstance(out.get("news_bullets"), list) and len(out["news_bullets"]) >= 3:
        # Normalise executive_summary into {lead: str, bullets: list[str]} —
        # tolerate the model returning a flat list, a single string, or
        # nothing at all.
        es_raw = out.get("executive_summary")
        if isinstance(es_raw, dict):
            es = {
                "lead": str(es_raw.get("lead", "") or "").strip(),
                "bullets": [
                    str(b).strip()
                    for b in (es_raw.get("bullets") or [])
                    if isinstance(b, str) and b.strip()
                ][:3],
            }
        elif isinstance(es_raw, list):
            es = {"lead": "", "bullets": [str(b).strip() for b in es_raw if isinstance(b, str)][:3]}
        elif isinstance(es_raw, str):
            es = {"lead": es_raw.strip(), "bullets": []}
        else:
            es = {"lead": "", "bullets": []}
        # Normalise portfolio_implications into a list[str]. Tolerate the
        # model returning a single string, a dict-list, or nothing.
        pi_raw = out.get("portfolio_implications")
        if isinstance(pi_raw, list):
            pi_list = [str(x).strip() for x in pi_raw if isinstance(x, str) and x.strip()][:4]
        elif isinstance(pi_raw, str) and pi_raw.strip():
            # Sometimes the model returns a single concatenated string.
            pi_list = [pi_raw.strip()]
        elif isinstance(pi_raw, dict):
            # E.g. {"bullets": [...]} — fish the list out.
            pi_list = [
                str(x).strip()
                for x in (pi_raw.get("bullets") or pi_raw.get("items") or [])
                if isinstance(x, str) and x.strip()
            ][:4]
        else:
            pi_list = []
        return (
            {
                "headline":          out.get("headline")    or fallback["headline"],
                "subheadline":       out.get("subheadline") or fallback["subheadline"],
                "news_summary":      out.get("news_summary") or fallback["news_summary"],
                "what_matters":      [],
                "news_bullets":      out.get("news_bullets") or [],
                "article_angles":    out.get("article_angles") or [],
                "executive_summary": es,
                "portfolio_implications": pi_list,
            },
            {"gemini_used": True, "reason": reason},
        )

    return {**fallback, "news_bullets": [], "article_angles": []}, {"gemini_used": False, "reason": reason}


def build_research_themes(research_docs: dict, use_gemini: bool = True) -> dict:
    """Per-bank AI summary of each uploaded research PDF.

    Simple and direct: one Gemini call per document. Feed it the PDF text,
    ask for 3-5 bullets summarising that specific bank's document. Render
    whatever comes back, no post-validator. The prompt is the whole defence
    against hallucination (and it's tight — 'use only text from this doc').

    Returns {"banks": [{"bank": "UBS", "bullets": [...]}, ...]}.
    """
    empty = {"banks": [], "themes": []}
    if not research_docs:
        return empty
    if not use_gemini or not GEMINI_API_KEY:
        return empty

    def _infer_bank(fname: str) -> str:
        fn = fname.lower()
        # Bank of Singapore publication families. Confirmed 2026-04-27 — the
        # CIO_WEEKLY_* and FX_WEEKLY_* documents that previously rendered as
        # raw filenames are BoS publications.
        if "morning call" in fn:                        return "BoS"
        if "cio_weekly"   in fn or "cio weekly" in fn:  return "BoS CIO"
        if "fx_weekly"    in fn or "fx weekly"  in fn:  return "BoS FX"
        if "barclays"     in fn:                        return "Barclays"
        if "daily europe" in fn:                        return "UBS"
        if "equity_coverage" in fn or "universe" in fn: return "UBS Universe"
        if "dmo"          in fn or "ocbc" in fn:        return "OCBC"
        if "goldman"      in fn:                        return "Goldman"
        if "jpmorgan"     in fn or fn.startswith("jpm"): return "JPMorgan"
        return fname.split(".")[0][:24]

    today_str = datetime.now().strftime("%Y-%m-%d")
    per_bank: dict[str, list[str]] = {}
    # Parallel structure: structured items {key_message, portfolio_implication}.
    # Falls back to plain bullets if the model doesn't return the structured shape.
    per_bank_items: dict[str, list[dict]] = {}
    debug_info: list[str] = []

    # Space calls out to stay under Gemini's 15 RPM free-tier limit.
    # Brief already spends 2-3 Gemini calls on news before reaching here.
    _call_idx = 0

    def _extract_items_deep(obj) -> list[dict]:
        """Find structured {key_message, portfolio_implication} items in
        the model response. Tolerates field-name variations the model
        sometimes produces (message/implication, view/portfolio_view, etc.).
        Returns a list of dicts with at minimum a 'key_message' field."""
        out: list[dict] = []
        def _first_str(d: dict, *keys: str) -> str:
            for k in keys:
                v = d.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            return ""
        def _norm_one(d) -> dict | None:
            if not isinstance(d, dict):
                return None
            km = _first_str(d, "key_message", "message", "view", "summary", "point")
            pi = _first_str(d, "portfolio_implication", "implication",
                            "portfolio_view", "consequence")
            if not km:
                return None
            return {"key_message": km, "portfolio_implication": pi}
        if isinstance(obj, list):
            for it in obj:
                norm = _norm_one(it)
                if norm:
                    out.append(norm)
            return out
        if isinstance(obj, dict):
            for key in ("items", "views", "key_messages", "highlights", "messages"):
                if key in obj and isinstance(obj[key], list):
                    sub = _extract_items_deep(obj[key])
                    if sub:
                        return sub
            # Sometimes the whole object IS one item — try that.
            single = _norm_one(obj)
            if single:
                return [single]
            # Recurse one level for nested {"bank": {"items": [...]}}
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    sub = _extract_items_deep(v)
                    if sub:
                        return sub
        return []

    def _extract_bullets_deep(obj) -> list[str]:
        """Find bullets anywhere in Gemini's response — sometimes the model
        nests them under varied keys (summary, points, key_points, highlights)
        or wraps them in a top-level {"bank": {...}, "bullets": [...]} dict."""
        if isinstance(obj, list):
            strs = [x for x in obj if isinstance(x, str) and x.strip()]
            if strs:
                return strs
            # list of dicts? try to find a 'text'/'bullet'/'view' in each
            extracted = []
            for item in obj:
                if isinstance(item, dict):
                    v = (item.get("text") or item.get("bullet")
                         or item.get("view") or item.get("point")
                         or item.get("summary"))
                    if isinstance(v, str) and v.strip():
                        extracted.append(v.strip())
            return extracted
        if isinstance(obj, dict):
            for key in ("bullets", "summary", "summaries", "points",
                        "key_points", "highlights", "main_points",
                        "takeaways", "insights"):
                if key in obj:
                    out = _extract_bullets_deep(obj[key])
                    if out:
                        return out
            # Recurse one level deep for nested {"bank": {"bullets": [...]}}
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    out = _extract_bullets_deep(v)
                    if out:
                        return out
        return []

    for fname, rdoc in research_docs.items():
        if rdoc.get("error"):
            continue
        bank = _infer_bank(fname)
        source_text = (rdoc.get("text") or "").strip()

        # Morning_call docs may have empty `text` but rich structured fields —
        # reconstruct text from those so Gemini has something to summarise.
        if not source_text and rdoc.get("_doc_type") == "morning_call":
            parts = []
            for reg, txt in (rdoc.get("regional_summaries") or {}).items():
                if txt:
                    parts.append(f"[{reg}] {txt}")
            for vp in (rdoc.get("equity_viewpoints") or []):
                parts.append(f"[equity viewpoint] {vp}")
            rec = rdoc.get("recommendation_changes") or {}
            for upg in (rec.get("upgrades") or []):
                parts.append(
                    f"UPGRADE: {upg.get('name','')} "
                    f"{upg.get('rating_old','')} to {upg.get('rating_new','')}"
                )
            for dwn in (rec.get("downgrades") or []):
                parts.append(
                    f"DOWNGRADE: {dwn.get('name','')} "
                    f"{dwn.get('rating_old','')} to {dwn.get('rating_new','')}"
                )
            source_text = "\n".join(parts)

        # Equity coverage: flatten the stocks table into a text blob so
        # Gemini can summarise the key calls (biggest upsides, biggest
        # downsides, by sector / region).
        if not source_text and rdoc.get("_doc_type") == "equity_coverage":
            stocks = rdoc.get("stocks") or []
            lines = []
            for s in stocks[:60]:
                lines.append(
                    f"{s.get('rating','')} {s.get('ticker','')} "
                    f"{s.get('name','')} upside={s.get('upside','')}% "
                    f"yield={s.get('div_yield','')}% pe={s.get('pe','')}"
                )
            source_text = "\n".join(lines)

        if not source_text:
            debug_info.append(f"{bank}: no text")
            continue

        payload = _safe_json_dumps({
            "instruction": (
                f"Today is {today_str}. Below is the text of a single "
                f"broker research document from {bank}. Produce 3 to 4 "
                "concrete bullets summarising the main views in this "
                "document. Each bullet is one tight sentence, 12-22 "
                "words, with specific numbers, tickers, levels, or rating "
                "changes where the document has them.\n"
                "\n"
                "RULES:\n"
                "1. Use ONLY information that appears in the document text "
                "below. Do not add anything from general knowledge.\n"
                "2. Do not invent numbers, price targets, ratings, analyst "
                "names, or forecasts not in the text.\n"
                "3. Do not mix up central banks (Fed is not ECB).\n"
                "4. If the document is thin, return fewer bullets (even 1).\n"
                "5. Faithfully paraphrase the document — including its "
                "ratings/calls/recommendations — but do NOT sensationalise. "
                "Avoid tabloid framing ('Super Bowl', 'showdown', 'mega "
                "week', 'all eyes on'). Avoid filler ('amid concerns', "
                "'investors are watching'). Use sober, concrete writing.\n"
                "6. Attribute the bank's calls ('UBS reiterates Buy on X', "
                "'BoS upgrades Y') rather than asserting recommendations as "
                "your own view.\n"
                "\n"
                "OUTPUT: raw JSON only, no markdown. "
                "Shape: {\"bullets\": [\"bullet 1\", \"bullet 2\", ...]}"
            ),
            "bank": bank,
            # 2500 chars ≈ 700 tokens — stays well under Groq's 6k TPM free-tier
            # budget when all 5 bank calls fire within the same minute.
            "document_text": source_text[:2500],
        })

        # Space Gemini calls out to stay under 15 RPM. First call no delay,
        # then 4.5s between calls — that's 13 calls/min max before even
        # counting the 2-3 the Brief has already spent on news / COTD.
        if _call_idx > 0:
            time.sleep(4.5)
        _call_idx += 1

        try:
            out, reason = _ai_generate_json_uncached(payload)
        except Exception as e:
            debug_info.append(f"{bank}: exception {type(e).__name__}")
            continue

        if not isinstance(out, dict):
            # Full reason — shows both Gemini AND Groq errors.
            debug_info.append(f"{bank}: no JSON ({str(reason)[:220]})")
            continue

        # Prefer the structured {key_message, portfolio_implication} shape.
        # Fall back to flat bullets only if no items came back.
        items_found = _extract_items_deep(out)

        if items_found:
            cleaned_items = [
                {
                    "key_message": it["key_message"][:240].strip(),
                    "portfolio_implication": (it.get("portfolio_implication") or "")[:160].strip(),
                }
                for it in items_found[:4]
                if it.get("key_message")
            ]
            if cleaned_items:
                per_bank_items.setdefault(bank, []).extend(cleaned_items)
                # Also populate flat bullets for downstream consumers that
                # still expect the old shape (e.g. _research_themes_debug).
                per_bank.setdefault(bank, []).extend(
                    it["key_message"] for it in cleaned_items
                )
                debug_info.append(f"{bank}: {len(cleaned_items)} items ok")
                continue

        # No structured items — try flat bullets as fallback.
        bullets_found = _extract_bullets_deep(out)
        if not bullets_found:
            keys = list(out.keys())[:6]
            debug_info.append(f"{bank}: no items/bullets found in keys={keys}")
            continue

        cleaned = [s for s in bullets_found[:5] if s.strip()]
        if cleaned:
            per_bank.setdefault(bank, []).extend(cleaned)
            # Synthesize items with empty implication so render path works uniformly.
            per_bank_items.setdefault(bank, []).extend(
                {"key_message": s, "portfolio_implication": ""} for s in cleaned[:4]
            )
            debug_info.append(f"{bank}: {len(cleaned)} bullets-only ok")

    # Diagnostic — survives in session_state so sidebar can show what happened.
    try:
        import streamlit as _st
        _st.session_state["_research_themes_debug"] = {
            "source": "per-doc gemini",
            "banks_seen": len(research_docs),
            "banks_rendered": list(per_bank.keys()),
            "notes": debug_info[:12],
        }
    except Exception:
        pass

    if not per_bank:
        return empty

    banks_out = [
        {
            "bank":    bank,
            "bullets": per_bank[bank][:5],            # legacy shape, kept
            "items":   per_bank_items.get(bank, [])[:4],  # new structured shape
        }
        for bank in sorted(per_bank.keys())
    ]
    return {"banks": banks_out, "themes": []}


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
            elif key == "msci_em":
                s, _ = fetch_yf_series_with_fallback(MSCI_EM_TICKERS, "MSCI EM")
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

    # Each bond proxy now has a ticker cascade so we don't render an N/A row
    # when a single ticker is unavailable on yfinance. Specifically EUR Bonds
    # — IEAG (US-listed) sometimes has thin or short history; IEAC.L (LSE)
    # and AGGH.MI (Borsa Italiana) cover the same exposure with longer
    # series typically available.
    bond_proxies = [
        ("global_bonds", "Global Bonds",
         ["BNDW", "AGGG.L", "VAGF.L"],
         "Global aggregate bond ETF proxy"),
        ("usd_bonds",    "USD Bonds",
         ["BND", "AGG", "IUSB"],
         "US aggregate bond ETF proxy"),
        ("eur_bonds",    "EUR Bonds",
         ["IEAG", "IEAC.L", "AGGH.MI", "EUNH.DE"],
         "EUR investment-grade bond ETF proxy"),
    ]

    for key, label, tickers, desc in bond_proxies:
        try:
            s, _ = fetch_yf_series_with_fallback(tickers, label)
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
    # Rolling 7-day return (T vs T-7) instead of week-to-date.
    # WTD reads +0.00% across the board on Mondays, which the reviewer flagged
    # as uninformative. The column key stays "wtd" to avoid plumbing changes;
    # the displayed header is updated to "7d" in _dtbl().
    seven_days_ago = today - pd.Timedelta(days=7)

    # NOTE: an earlier version of this loop suppressed d1 when a series'
    # last data point looked stale (>1 calendar day older than the global
    # latest). That was over-aggressive — Friday-close US index data on a
    # Monday morning Europe-time run was getting flagged as stale even
    # though it's the correct "1D" reference for an EU reader at that hour.
    # Reverted 2026-04-27. The trade-off is that occasional WTI/Brent
    # feed-lag divergence will leak through, but blanking core indices'
    # 1D column is a much worse credibility hit than the rare divergence.
    # The narrative-side prompt now handles oil divergence by describing
    # "mixed daily benchmark moves" rather than pretending they agree.

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
                "wtd": pct_change(latest, value_on_or_before(series, seven_days_ago)),
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
    """Nominate the most interesting chart using AI (Gemini→Groq), with a rich local fallback."""
    if history.empty:
        return None

    # Find biggest movers (z-score of latest value vs 30-day rolling mean)
    movers = []
    for key, g in history.groupby("key"):
        g = g.sort_values("date").set_index("date")
        cutoff = g.index.max() - pd.Timedelta(days=30)
        recent = g["value"][g.index >= cutoff].dropna()
        if len(recent) < 5:
            continue
        mean, std = recent.mean(), recent.std()
        if std == 0:
            continue
        latest = float(recent.iloc[-1])
        prev   = float(recent.iloc[-2]) if len(recent) >= 2 else latest
        z      = abs((latest - mean) / std)
        d1_pct = (latest / prev - 1) * 100 if prev != 0 else 0
        lbl    = g["label"].iloc[0] if "label" in g.columns else key
        movers.append({
            "key": key, "label": lbl,
            "zscore": round(z, 2),
            "d1_pct": round(d1_pct, 2),
            "latest": round(latest, 4),
        })

    movers = sorted(movers, key=lambda x: x["zscore"], reverse=True)[:6]

    # Count most-mentioned keywords in news
    kw_counts: dict = {}
    if news_df is not None and not news_df.empty:
        all_headlines = " ".join(news_df["headline"].fillna("").str.lower().tolist())
        for kw in ["iran", "fed", "ecb", "china", "tariff", "oil", "gold", "dollar",
                   "inflation", "rate", "war", "ceasefire", "bitcoin", "recession", "nasdaq", "tech"]:
            cnt = all_headlines.count(kw)
            if cnt > 0:
                kw_counts[kw] = cnt
    top_kws = sorted(kw_counts.items(), key=lambda x: x[1], reverse=True)[:4]

    # Try AI (Gemini→Groq)
    if GEMINI_API_KEY or GROQ_API_KEY:
        try:
            payload = _safe_json_dumps({
                "instruction": (
                    "Return ONLY raw JSON — no markdown, no code fences, no preamble. "
                    "You are a financial analyst writing a morning brief. "
                    "Pick ONE chart of the day from the candidates. Choose the most interesting "
                    "for an investor today — consider both the unusual statistical move AND the top news themes. "
                    "\n\n"
                    "HARD GROUNDING RULES:\n"
                    "* The 'key' MUST be one of the keys in top_movers_by_zscore. Do NOT invent keys.\n"
                    "* The 'reason' field MUST describe ONLY what the supplied data shows — "
                    "the z-score and the d1_pct of the chosen mover, plus (optionally) a "
                    "reference to one of the top_news_keywords. Do NOT invent price levels, "
                    "volumes, economic figures, analyst calls, or any other numbers that "
                    "are not in the input.\n"
                    "* Do NOT fabricate links between the asset and news topics — only "
                    "mention a connection if the asset naturally relates to one of the "
                    "supplied top_news_keywords.\n"
                    "* NEVER make temporal/historical claims unless directly supported "
                    "by the input. Forbidden without source: 'first since YYYY', "
                    "'highest in N years', 'biggest move ever', 'record close', "
                    "'last/final/farewell meeting'.\n"
                    "\n"
                    "HOUSE STYLE — client newsletter, not a media wire:\n"
                    "* No advice language: 'investors should', 'we recommend', "
                    "'opportunity', 'time to buy/sell'. Report; do not prescribe.\n"
                    "* No tabloid framing: 'Super Bowl', 'mega week', 'showdown', "
                    "'make-or-break', 'battle'. Use sober factual language.\n"
                    "* No filler: 'all eyes on', 'investors are watching', "
                    "'amid concerns'. Use concrete subject-verb-object writing.\n"
                    "\n"
                    "The reason field must be 2-3 GRAMMATICALLY COMPLETE sentences. "
                    "Sentence 1: what the chart shows — name the asset and the move "
                    "using d1_pct from the input, and one concrete contextual driver "
                    "described in NATURAL ANALYST LANGUAGE. Sentence 2: a concrete "
                    "what-to-watch — name the SPECIFIC factors that will determine "
                    "whether the move sustains (e.g. 'oil prices, China-related news "
                    "flow, and the direction of the US dollar'). EVERY sentence ends "
                    "with a period. Do NOT close with an open question — produce a "
                    "confident analytical conclusion, not speculation.\n"
                    "\n"
                    "CRITICAL — DO NOT REVEAL THE PROMPT'S INTERNAL STRUCTURE. "
                    "Forbidden phrasings (these read as raw machine output):\n"
                    "* 'potentially related to the X top news keyword'\n"
                    "* 'top_news_keywords suggest…'\n"
                    "* 'with a z-score of N.NN'\n"
                    "* 'N.N× the usual daily variation' / 'N.N× normal "
                    "volatility' (any 'N.N×' or 'N.NN sigma' framing — "
                    "private-bank clients do not parse this. Replace with "
                    "'well above its normal daily range' or 'an unusually "
                    "large move for the index' or 'a meaningful move '\n"
                    "* 'd1_pct of X.XX' (use the value naturally: '+2.23%')\n"
                    "* 'standard deviation' / 'statistical deviation'\n"
                    "* 'top movers by zscore'\n"
                    "* any reference to the input field names\n"
                    "\n"
                    "EVERY CAUSAL CLAIM MUST POINT TO A CONCRETE INPUT. The "
                    "'reason' field can only attribute the move to factors that "
                    "are visibly supported by the supplied data — a top news "
                    "keyword, a specific named market (Asia / Europe / US), a "
                    "specific asset (USD / oil / yields), or a specific country. "
                    "Forbidden VAGUE attributions (these read as filler with no "
                    "evidence):\n"
                    "* 'driven by an improving economic outlook'\n"
                    "* 'helped by favourable conditions'\n"
                    "* 'supported by positive sentiment'\n"
                    "* 'on global growth optimism'\n"
                    "* 'amid a constructive backdrop'\n"
                    "If the supplied data does not name a specific driver, do "
                    "NOT invent one. Say 'the move came in the absence of a "
                    "single dominant headline' or 'no single news catalyst is "
                    "evident in today's flow' instead.\n"
                    "Write as if you are an analyst summarising the day, not as if "
                    "you are processing structured input. Bad close (machine-like): "
                    "'What will be the impact of global events on emerging markets?'. "
                    "Bad mid (meta-reference): 'helped by oil, the top news keyword'. "
                    "Good close (analyst): 'The sustainability of the move depends "
                    "on oil prices, China-related news flow, and the direction of "
                    "the US dollar.'\n"
                    "Required JSON: {\"key\": \"<asset_key>\", \"label\": \"<asset_label>\", "
                    "\"reason\": \"<2-3 sentence analyst commentary, all periods, "
                    "no meta-references, no open questions>\", "
                    "\"timeframe_days\": <30|60|90|180>}"
                ),
                "top_movers_by_zscore": movers,
                "top_news_keywords":    [{"keyword": k, "mentions": v} for k, v in top_kws],
            })
            out, _ = ai_generate_json(payload)
            if isinstance(out, dict) and out.get("key") and out.get("reason"):
                # Scrub banned phrases in the reason text (z-score / "1.6×
                # the usual daily variation" / vague attributions / etc.)
                # before returning. The reason field is the only string
                # output that ships to clients from this function.
                reason_str = out.get("reason") or ""
                if isinstance(reason_str, str):
                    cleaned, fixes = _scrub_ai_text(reason_str)
                    if cleaned != reason_str:
                        out["reason"] = cleaned
                        try:
                            import streamlit as _st
                            existing = _st.session_state.get(
                                "_house_style_scrub_log", []
                            )
                            existing.extend(f"cotd.reason: {fx}" for fx in fixes)
                            _st.session_state["_house_style_scrub_log"] = existing
                        except Exception:
                            pass
                # Only accept keys that actually exist in history — ignore invented tickers
                valid_keys = set(history["key"].unique())
                ai_key = out.get("key", "")
                if ai_key in valid_keys:
                    return out
                # AI returned an unknown key — try to match a mover by label
                ai_label = (out.get("label","") or "").lower()
                for m in movers:
                    if m["label"].lower() == ai_label:
                        return {**out, "key": m["key"], "label": m["label"]}
                # No match — fall through to local fallback
        except Exception:
            pass

    # Rich local fallback — build a proper explanation without AI
    if not movers:
        return None
    m    = movers[0]
    rank = 1
    d1s  = f"{m['d1_pct']:+.2f}%" if m['d1_pct'] != 0 else "flat"
    z    = m['zscore']
    magnitude = "extremely" if z > 3 else ("significantly" if z > 2 else "notably")

    # Check if top news keywords relate to this asset
    asset_kws = {
        "gold":     ["gold","inflation","war","iran","safe"],
        "bitcoin":  ["bitcoin","crypto","risk"],
        "wti":      ["oil","iran","war","ceasefire","crude"],
        "brent":    ["oil","iran","war","ceasefire","crude"],
        "us10y":    ["fed","rate","inflation","treasury","fomc"],
        "nasdaq100":["tech","nasdaq","ai","rate"],
        "sp500":    ["earnings","fed","economy","recession"],
        "eurusd":   ["ecb","dollar","euro"],
    }.get(m["key"], [])
    news_link = ""
    if asset_kws and top_kws:
        matched = [kw for kw, _ in top_kws if any(a in kw for a in asset_kws)]
        if matched:
            news_link = f" Today's headlines are dominated by '{matched[0]}'-related news, which directly affects this asset."

    reason = (
        f"{m['label']} is {magnitude} outside its normal 30-day range today "
        f"(statistical deviation: {z:.1f}× the usual daily variation), moving {d1s} in the latest session. "
        f"It ranked #{rank} out of all tracked assets for unusual price behaviour.{news_link} "
        f"Watch whether this move continues or reverts over the next few sessions."
    )
    return {"key": m["key"], "label": m["label"], "reason": reason, "timeframe_days": 60}


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

    Trading-terminal layout (redesigned 2026-04-24 for compactness):
      Row 1 (top):    label (small, left)      ·  1D change (small, right)
      Row 2 (middle): value (big, left)         ·  YTD change (small, right)
      Row 3 (bottom): sparkline, fills remainder

    Larger top margin so text has exclusive space (no chart-line showing through
    text). Cards are the same outer size so the grid doesn't break.
    """
    H, MT, MB, ML, MR = 160, 74, 10, 10, 10

    if snapshot_row.empty:
        fig = go.Figure()
        fig.update_layout(
            height=H, margin=dict(l=ML, r=MR, t=MT, b=MB),
            plot_bgcolor="#F8FAFC", paper_bgcolor="#F8FAFC",
            showlegend=False, xaxis=dict(visible=False), yaxis=dict(visible=False),
            annotations=[dict(x=0.5, y=0.82, xref="paper", yref="paper",
                              text=f"<b>{item['label']}</b><br><span style='color:#9AA8B7'>No data</span>",
                              font=dict(size=9, color="#475467"), showarrow=False)],
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
            tickformat="%d %b", tickfont=dict(size=7, color="#9AA8B7"),
            nticks=3, showticklabels=True, automargin=True,
        ),
        yaxis=dict(
            showgrid=False, showline=False,
            showticklabels=False,   # hide y-axis numbers — sparkline is for shape only
            automargin=False,
        ),
        # Plot area: y ∈ [0.0625, 0.5375]. Text annotations live above 0.54.
        # Row 1 top y=1.02 : label (left) + 1D change (right), accent-coloured
        # Row 2 top y=0.80 : value (left, big) + YTD change (right, small)
        annotations=[
            # Row 1 — label + 1D on same line
            dict(x=0.03, y=1.02, xref="paper", yref="paper",
                 xanchor="left", yanchor="top",
                 text=f"<b>{lbl}</b>",
                 font=dict(size=9.5, color="#475467"), showarrow=False),
            dict(x=0.97, y=1.02, xref="paper", yref="paper",
                 xanchor="right", yanchor="top",
                 text=f"<b>1D {d1_str}</b>",
                 font=dict(size=9, color=accent if accent != "#94A3B8" else "#475467"),
                 showarrow=False),
            # Row 2 — value (big) + YTD (small)
            dict(x=0.03, y=0.82, xref="paper", yref="paper",
                 xanchor="left", yanchor="top",
                 text=f"<b>{value_str}</b>{extra}",
                 font=dict(size=17, color="#0F2D52"), showarrow=False),
            dict(x=0.97, y=0.78, xref="paper", yref="paper",
                 xanchor="right", yanchor="top",
                 text=f"YTD {ytd_str}",
                 font=dict(size=8.5, color="#667085"), showarrow=False),
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

    rows = [strip[i:i + 4] for i in range(0, len(strip), 4)]
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


def _match_bullet_to_article(bullet: str, news_df) -> dict | None:
    """Return the news_df row most relevant to a bullet via word overlap."""
    if news_df is None or news_df.empty:
        return None
    b_words = set(bullet.lower().split())
    best, best_score = None, 0
    for _, r in news_df.iterrows():
        h_words = set((r.get("headline","") or "").lower().split())
        score = len(b_words & h_words)
        if score > best_score:
            best_score, best = score, r
    return best.to_dict() if best is not None and best_score >= 2 else None


def render_news_bullets(writing, news_df):
    """AI bullets with inline source links after each one; source list below."""
    bullets = writing.get("news_bullets") or []

    if bullets:
        for b in bullets:
            match = _match_bullet_to_article(b, news_df)
            if match and match.get("url"):
                src = match.get("source","") or ""
                pub = match.get("published_at","") or ""
                dt_str = ""
                if pub:
                    try: dt_str = pd.Timestamp(pub).strftime("%d %b")
                    except Exception: pass
                meta = " · ".join([x for x in [src, dt_str] if x])
                link = f" <a href='{match['url']}' target='_blank' style='font-size:11px;color:#1E88E5;text-decoration:none;'>↗ {meta}</a>" if meta else f" <a href='{match['url']}' target='_blank' style='font-size:11px;color:#1E88E5;'>↗</a>"
                st.markdown(f"- {b}{link}", unsafe_allow_html=True)
            else:
                st.markdown(f"- {b}")
    else:
        if news_df is None or news_df.empty:
            st.caption("No news available.")
            return
        for _, r in news_df.head(10).iterrows():
            cat = r.get("category","")
            headline = r.get("headline","")
            url = r.get("url","")
            prefix = f"**{cat}** — " if cat and cat != "Other" else ""
            link = f" [↗]({url})" if url else ""
            st.markdown(f"- {prefix}{headline}{link}")

    # Unmatched sources with inline checkboxes for PDF inclusion
    if news_df is not None and not news_df.empty:
        with st.expander("📎 Source articles — tick to include in PDF", expanded=True):
            st.caption("Checked articles appear in the PDF news table. Changes take effect when you click **Update PDF**.")

            selections = {}
            for i, (_, r) in enumerate(news_df.head(NEWS_COUNT).iterrows()):
                headline = r.get("headline","") or ""
                url      = r.get("url","")      or ""
                source   = r.get("source","")   or ""
                pub      = r.get("published_at","") or ""
                dt_str   = ""
                if pub:
                    try: dt_str = pd.Timestamp(pub).strftime("%d %b %H:%M")
                    except Exception: dt_str = str(pub)[:10]
                meta = " · ".join([x for x in [source, dt_str] if x])

                c1, c2 = st.columns([0.05, 0.95])
                with c1:
                    checked = st.checkbox(
                        "", value=True,
                        key=f"pdf_inc_{i}_{headline[:20]}",
                        label_visibility="collapsed",
                    )
                with c2:
                    link_md = f"[{headline}]({url})" if url else headline
                    meta_span = (f'  <span style="color:#9AA8B7;font-size:11px;"> — {meta}</span>'
                                 if meta else "")
                    st.markdown(
                        f"<div style='padding:2px 0;font-size:12px;line-height:1.4;'>"
                        f"{link_md}{meta_span}"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                selections[headline] = checked

            if st.button("🔄 Update PDF with ticked articles", use_container_width=True, type="secondary"):
                keep = [h for h, v in selections.items() if v]
                filtered = news_df[news_df["headline"].isin(keep)].copy()
                new_pdf = build_pdf(
                    "Daily Market Briefing",
                    st.session_state.get("pdf_chart_png"),
                    st.session_state["equities_df"],
                    st.session_state["rates_df"],
                    st.session_state["commodities_df"],
                    st.session_state.get("bonds_df", st.session_state["commodities_df"]),
                    st.session_state["metrics"],
                    st.session_state["writing"],
                    filtered,
                    st.session_state["status"],
                    fx_df=st.session_state.get("fx_df"),
                )
                st.session_state["pdf_bytes"] = new_pdf
                st.success(f"PDF updated with {len(keep)} articles.")


def build_pdf(title, chart_png, equities_df, rates_df, commodities_df, bonds_df,
              metrics, writing, news_df, status, cotd=None, cotd_png=None, fx_df=None,
              research_docs=None, research_themes=None):
    """Professional one-page landscape PDF. Max 2 levels of Table nesting."""
    from reportlab.platypus import HRFlowable
    buffer = BytesIO()
    PW  = 28.1   # usable width cm  (29.7 - 0.8*2 margins)
    doc = SimpleDocTemplate(
        buffer, pagesize=landscape(A4),
        rightMargin=0.8*cm, leftMargin=0.8*cm,
        topMargin=0.6*cm,   bottomMargin=0.6*cm,
    )
    ss = getSampleStyleSheet()

    NAV  = colors.HexColor("#0F2D52")
    BLU  = colors.HexColor("#1E88E5")
    MID  = colors.HexColor("#2C5282")
    LIT  = colors.HexColor("#EBF4FF")
    STR  = colors.HexColor("#F7FAFD")
    RUL  = colors.HexColor("#CBD5E0")
    GRN  = colors.HexColor("#16A34A")
    RED  = colors.HexColor("#DC2626")
    GRY  = colors.HexColor("#718096")
    TXT  = colors.HexColor("#1A202C")
    WHT  = colors.white

    def P(text, fn="Helvetica", sz=6, col=TXT, lead=None, bold=False):
        fn2 = "Helvetica-Bold" if bold else fn
        return Paragraph(text, ParagraphStyle("_",parent=ss["BodyText"],
            fontName=fn2, fontSize=sz, textColor=col, leading=lead or sz*1.25))

    def _t(s, n):
        s = "" if s is None else str(s)
        return s if len(s) <= n else s[:n-1]+"\u2026"

    def _xs(s):
        """XML-escape untrusted text before it goes into Paragraph.
        reportlab parses its input as mini-XML, so a stray &/</> from a
        PDF extraction (P&G, S&P, <5%, >15x) crashes doc.build()."""
        s = "" if s is None else str(s)
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # Reviewer asked to hide "N/A" cells — they erode credibility even when
    # they're a single missing data point. Use an em-dash placeholder that
    # reads as "data not applicable / not available" without the harsh "N/A".
    _NA_CELL = "—"

    def _pct(v):
        if v is None: return _NA_CELL
        try:
            f=float(v)
            return _NA_CELL if f!=f else f"{f:+.2f}%"
        except: return _NA_CELL

    def _num(v):
        if v is None: return _NA_CELL
        try:
            f=float(v)
            return _NA_CELL if f!=f else f"{f:,.2f}"
        except: return _NA_CELL

    def _pc(v):   # colour for pct value
        try:
            f=float(v); return GRN if f>0 else (RED if f<0 else GRY)
        except: return GRY

    story = []

    # ── 1. HEADER ─────────────────────────────────────────────────────────────
    hdr = Table(
        [[P(title, fn="Helvetica-Bold", sz=18, col=WHT, lead=20),
          P(f"{datetime.now().strftime('%A, %d %B %Y')}", sz=7.5, col=WHT, lead=9)]],
        colWidths=[PW*0.72*cm, PW*0.28*cm],
    )
    hdr.setStyle(TableStyle([
        ("BACKGROUND",   (0,0),(-1,-1), NAV),
        ("LEFTPADDING",  (0,0),(-1,-1), 10),
        ("RIGHTPADDING", (0,0),(-1,-1), 10),
        ("TOPPADDING",   (0,0),(-1,-1), 8),
        ("BOTTOMPADDING",(0,0),(-1,-1), 8),
        ("ALIGN",        (1,0),(1,0),   "RIGHT"),
        ("VALIGN",       (0,0),(-1,-1), "MIDDLE"),
        ("LINEBELOW",    (0,0),(-1,-1), 2, BLU),
    ]))
    story += [hdr, Spacer(1, 0.12*cm)]

    # ── 1b. EXECUTIVE SUMMARY ────────────────────────────────────────────────
    # Reviewer's "nice-to-have #1": a 3-bullet exec summary at the top so the
    # client sees the brief's message before drowning in tables. Rendered as
    # a single bordered card: lead paragraph + 3 short bullets.
    _es = (writing or {}).get("executive_summary") or {}
    _es_lead = (_es.get("lead") or "").strip()
    _es_bullets = [b for b in (_es.get("bullets") or []) if isinstance(b, str) and b.strip()][:3]
    if _es_lead or _es_bullets:
        # Reviewer asked for the exec summary to be more prominent.
        # Bumped: section title 5.8→6.6, lead 6.2→7.2, bullets 5.8→6.6 bold.
        es_flows = [
            P("EXECUTIVE SUMMARY", fn="Helvetica-Bold", sz=6.6, col=BLU, lead=8),
            HRFlowable(width=(PW-1.0)*cm, thickness=0.5, color=BLU),
            Spacer(1, 0.05*cm),
        ]
        if _es_lead:
            es_flows.append(P(_xs(_es_lead), fn="Helvetica", sz=7.2, col=TXT, lead=9.2))
        if _es_bullets:
            if _es_lead:
                es_flows.append(Spacer(1, 0.07*cm))
            for b in _es_bullets:
                es_flows.append(
                    P(f"<b>\u25aa</b>&nbsp;&nbsp;{_xs(b)}",
                      fn="Helvetica-Bold", sz=6.6, col=NAV, lead=8.2)
                )
        es_tbl = Table([[es_flows]], colWidths=[PW*cm])
        es_tbl.setStyle(TableStyle([
            ("BACKGROUND",   (0,0),(-1,-1), LIT),
            ("BOX",          (0,0),(-1,-1), 0.7, BLU),
            ("LEFTPADDING",  (0,0),(-1,-1), 10),
            ("RIGHTPADDING", (0,0),(-1,-1), 10),
            ("TOPPADDING",   (0,0),(-1,-1), 7),
            ("BOTTOMPADDING",(0,0),(-1,-1), 7),
            ("VALIGN",       (0,0),(-1,-1), "TOP"),
        ]))
        story += [es_tbl, Spacer(1, 0.14*cm)]

    # ── 2. KPI STRIP — last trading day moves, flat 2-row table ──────────────
    kpis = [
        ("Global Eq 1D",    metrics.get("global_equities_d1")),
        ("Global Bonds 1D", metrics.get("global_bonds_d1")),
        ("USD Bonds 1D",    metrics.get("usd_bonds_d1")),
        ("EUR Bonds 1D",    metrics.get("eur_bonds_d1")),
        ("Gold 1D",         metrics.get("gold_d1")),
        ("Bitcoin 1D",      metrics.get("bitcoin_d1")),
    ]
    lbl_row, val_row, kpi_cmds = [], [], [
        ("BACKGROUND",   (0,0),(-1,-1), LIT),
        ("BOX",          (0,0),(-1,-1), 0.5, RUL),
        ("INNERGRID",    (0,0),(-1,-1), 0.5, WHT),
        ("TOPPADDING",   (0,0),(-1,-1), 4),
        ("BOTTOMPADDING",(0,0),(-1,-1), 4),
        ("LEFTPADDING",  (0,0),(-1,-1), 6),
        ("RIGHTPADDING", (0,0),(-1,-1), 6),
        ("VALIGN",       (0,0),(-1,-1), "MIDDLE"),
    ]
    for i, (lbl, v) in enumerate(kpis):
        vs = _pct(v)
        try: f=float(v); col = GRN if f>0 else (RED if f<0 else NAV)
        except: col = NAV
        arr = "\u25b2 " if col==GRN else ("\u25bc " if col==RED else "")
        lbl_row.append(P(lbl, sz=5.5, col=GRY))
        val_row.append(P(f"{arr}{vs}", fn="Helvetica-Bold", sz=10, col=col, lead=12))
        kpi_cmds.append(("TEXTCOLOR", (i,1),(i,1), col))

    kpi_tbl = Table([lbl_row, val_row], colWidths=[(PW/6)*cm]*6)
    kpi_tbl.setStyle(TableStyle(kpi_cmds))
    story += [kpi_tbl, Spacer(1, 0.14*cm)]

    # ── 3. MAIN ROW: bullets | chart | cotd ───────────────────────────────────
    NAR_W   = PW * 0.32
    CHART_W = PW * 0.44
    COTD_W  = PW - NAR_W - CHART_W

    # Build bullet list as a flat table (2 cols: arrow+text | meta).
    # AI path: writing.news_bullets are already in house style.
    # Fallback path: AI failed — clean raw headlines via deterministic pass
    # (strip "Wall Street's Super Bowl Wednesday:" etc., dedupe, cap at 5)
    # before showing, so we never ship raw media-y text to clients.
    bullets = writing.get("news_bullets") or []
    _ai_bullets_used = bool(bullets)
    if not bullets and not news_df.empty:
        raw_headlines = [r.get("headline", "") for _, r in news_df.head(15).iterrows()]
        bullets = _dedupe_headlines(
            [_clean_headline_for_bullet(h) for h in raw_headlines if h],
            limit=5,
        )

    bul_rows, bul_cmds = [], [
        ("VALIGN",       (0,0),(-1,-1), "TOP"),
        ("LEFTPADDING",  (0,0),(-1,-1), 2),
        ("RIGHTPADDING", (0,0),(-1,-1), 2),
        ("TOPPADDING",   (0,0),(-1,-1), 1.5),
        ("BOTTOMPADDING",(0,0),(-1,-1), 1.5),
        ("ALIGN",        (1,0),(1,-1),  "RIGHT"),
    ]
    for ri, b in enumerate(bullets[:5]):
        match = _match_bullet_to_article(b, news_df) if not news_df.empty else None
        meta  = ""
        if match:
            src = _t(match.get("source","") or "", 22)
            pub = match.get("published_at","") or ""
            try: dt_s = pd.Timestamp(pub).strftime("%d %b") if pub else ""
            except: dt_s = ""
            meta = " \u00b7 ".join([x for x in [src,dt_s] if x])
        bul_rows.append([
            P(f"\u2192 {_xs(b)}", sz=5.6, col=TXT, lead=7),
            P(_xs(meta), fn="Helvetica-Oblique", sz=4.8, col=GRY, lead=6),
        ])
        bg = WHT if ri%2==0 else STR
        bul_cmds.append(("BACKGROUND",(0,ri),(-1,ri), bg))
        if ri < len(bullets)-1:
            bul_cmds.append(("LINEBELOW",(0,ri),(-1,ri), 0.3, RUL))

    bul_tbl = Table(bul_rows or [[P(""),P("")]], colWidths=[(NAR_W-2.2)*cm, 2.2*cm])
    bul_tbl.setStyle(TableStyle(bul_cmds))

    # Narrative cell — "Market Recap" title + 1-sentence AI summary
    nar_title = P("Market Recap", fn="Helvetica-Bold", sz=7.5, col=NAV, lead=9)
    _summ_txt = (writing.get("news_summary","") or writing.get("subheadline","")).strip()
    nar_summ  = P(_xs(_summ_txt[:600]), sz=5.5, col=GRY, lead=7.0) if _summ_txt else None
    # Surface AI status: when the cascade failed and we fell back to cleaned
    # raw headlines, label the section so the reader knows this is edited
    # headline copy, not curated AI commentary.
    _nar_sec_text = (
        "WHAT'S MOVING MARKETS"
        if _ai_bullets_used
        else "WHAT'S MOVING MARKETS — edited headlines (AI summary unavailable)"
    )
    nar_sec   = P(_nar_sec_text, fn="Helvetica-Bold", sz=5.8, col=BLU, lead=7)
    nar_rule  = HRFlowable(width=(NAR_W-0.6)*cm, thickness=0.5, color=RUL)

    # Chart cell — no box border (avoids misalignment when left column is taller)
    if chart_png:
        chart_cell = Image(BytesIO(chart_png), width=CHART_W*cm, height=6.5*cm)
    else:
        chart_cell = P("<i>Chart unavailable</i>", sz=6, col=GRY)

    # CotD cell — chart + reason + compact upcoming events below
    _today_d = datetime.now().date()
    _cutoff_d = _today_d + timedelta(days=31)
    _upcoming = [
        e for e in MACRO_EVENTS
        if _today_d <= datetime.strptime(e["date"], "%Y-%m-%d").date() <= _cutoff_d
    ]

    cotd_flows = []
    if cotd and isinstance(cotd, dict):
        cotd_label  = cotd.get("label", "Notable Move")
        tf          = int(cotd.get("timeframe_days", 60))
        # Smart-truncate at a sentence boundary (450 chars cap, was 350).
        # The naive [:350] cut produced fragments like 'Watch whether this
        # move continues or.' when the AI's third sentence ran past the
        # limit. The new helper finds the last complete sentence end, falls
        # back to the last word, and trims dangling conjunctions before
        # adding terminal punctuation.
        reason_text = _smart_truncate_at_sentence(
            cotd.get("reason", "") or "", 450,
        )
        # Convert any trailing "?" into "." (analytical conclusion, not
        # an open question — the prompt rule already says this, this is
        # just belt-and-suspenders).
        if reason_text and reason_text.endswith("?"):
            reason_text = reason_text[:-1] + "."
        cotd_flows += [
            P("CHART OF THE DAY", fn="Helvetica-Bold", sz=5.8, col=BLU, lead=7),
            Spacer(1, 0.04*cm),
            P(f"<b>{_xs(_t(cotd_label,24))}</b>  \u00b7 {tf} days", sz=7, col=NAV, lead=8.5),
            Spacer(1, 0.04*cm),
        ]
        if cotd_png:
            cotd_flows.append(Image(BytesIO(cotd_png), width=(COTD_W-0.5)*cm, height=2.4*cm))
            cotd_flows.append(Spacer(1, 0.04*cm))
        cotd_flows.append(P(_xs(reason_text), sz=5.0, col=TXT, lead=6.3))
    else:
        cotd_flows.append(P(""))

    # Upcoming events — compact list below CotD
    if _upcoming:
        cotd_flows += [
            Spacer(1, 0.08*cm),
            HRFlowable(width=(COTD_W-0.6)*cm, thickness=0.4, color=RUL),
            Spacer(1, 0.06*cm),
            P("UPCOMING EVENTS — NEXT 30 DAYS", fn="Helvetica-Bold", sz=5.5, col=BLU, lead=7),
            Spacer(1, 0.04*cm),
        ]
        for e in _upcoming[:5]:
            try:
                date_str = datetime.strptime(e["date"], "%Y-%m-%d").strftime("%d %b")
            except Exception:
                date_str = e["date"]
            cotd_flows.append(
                P(f"<b>{_xs(date_str)}</b>  {_xs(str(e.get('event','')))}", sz=5.2, col=TXT, lead=6.5)
            )
            cotd_flows.append(Spacer(1, 0.025*cm))

    # Build narrative flowables
    _nar_flows = [nar_title]
    if nar_summ:
        _nar_flows += [Spacer(1,0.04*cm), nar_summ]
    _nar_flows += [Spacer(1,0.06*cm), nar_sec, Spacer(1,0.03*cm), nar_rule, Spacer(1,0.04*cm), bul_tbl]

    # Single-level outer table: each cell holds a list of flowables
    main_tbl = Table(
        [[_nar_flows, chart_cell, cotd_flows]],
        colWidths=[NAR_W*cm, CHART_W*cm, COTD_W*cm],
    )
    main_tbl.setStyle(TableStyle([
        ("VALIGN",       (0,0),(-1,-1), "TOP"),
        ("BOX",          (0,0),(0,0),   0.4, RUL),
        # no box on chart cell (col 1) — avoids border misalignment when text column is taller
        ("BOX",          (2,0),(2,0),   0.8, BLU),
        ("BACKGROUND",   (0,0),(0,0),   WHT),
        ("BACKGROUND",   (1,0),(1,0),   WHT),
        ("BACKGROUND",   (2,0),(2,0),   LIT),
        ("LEFTPADDING",  (0,0),(-1,-1), 5),
        ("RIGHTPADDING", (0,0),(-1,-1), 5),
        ("TOPPADDING",   (0,0),(-1,-1), 5),
        ("BOTTOMPADDING",(0,0),(-1,-1), 5),
    ]))
    story += [main_tbl, Spacer(1, 0.10*cm)]

    # ── 4. DATA TABLES — max 2 levels deep ────────────────────────────────────
    def _dtbl(df, sec_title):
        """One data table. Level 1 = this table; cells contain Paragraphs only."""
        half = PW / 2
        cw = [half*0.44*cm, half*0.18*cm, half*0.14*cm, half*0.13*cm, half*0.11*cm]
        # Row 0 = section title spanning all cols
        # Row 1 = column headers
        # Row 2+ = data
        rows = [
            [P(_xs(sec_title), fn="Helvetica-Bold", sz=6.5, col=WHT, lead=8),
             P(""), P(""), P(""), P("")],
            [P("Instrument", fn="Helvetica-Bold", sz=5.5, col=WHT, lead=7),
             P("Level",      fn="Helvetica-Bold", sz=5.5, col=WHT, lead=7),
             P("1D",         fn="Helvetica-Bold", sz=5.5, col=WHT, lead=7),
             P("7d",         fn="Helvetica-Bold", sz=5.5, col=WHT, lead=7),
             P("YTD",        fn="Helvetica-Bold", sz=5.5, col=WHT, lead=7)],
        ]
        cmds = [
            ("BACKGROUND",   (0,0),(-1,0),  NAV),
            ("BACKGROUND",   (0,1),(-1,1),  MID),
            ("SPAN",         (0,0),(-1,0)),
            ("TOPPADDING",   (0,0),(-1,0),  2.5),("BOTTOMPADDING",(0,0),(-1,0), 2.5),
            ("TOPPADDING",   (0,1),(-1,-1), 1.5),("BOTTOMPADDING",(0,1),(-1,-1), 1.5),
            ("LEFTPADDING",  (0,0),(-1,-1), 4),("RIGHTPADDING",(0,0),(-1,-1), 4),
            ("ALIGN",        (1,1),(-1,-1), "RIGHT"),
            ("VALIGN",       (0,0),(-1,-1), "MIDDLE"),
            ("LINEBELOW",    (0,0),(-1,0),  1.5, BLU),
            ("BOX",          (0,0),(-1,-1), 0.4, RUL),
            ("LINEBELOW",    (0,1),(-1,-1), 0.3, RUL),
        ]
        # Drop rows where level is None/NaN — these render as "N/A | N/A | N/A | N/A"
        # and erode credibility (reviewer's #1 complaint). Keep rows where level
        # exists but a single return column is missing — partial data is fine.
        def _has_level(row) -> bool:
            v = row.get("level")
            if v is None:
                return False
            try:
                f = float(v)
                return f == f  # NaN check
            except Exception:
                return False
        df = df[df.apply(_has_level, axis=1)] if not df.empty else df

        for ri, (_, row) in enumerate(df.iterrows()):
            d1 = row.get("d1"); wtd = row.get("wtd"); ytd = row.get("ytd")
            bg = WHT if ri%2==0 else STR
            cmds.append(("BACKGROUND", (0,ri+2),(-1,ri+2), bg))
            cmds.append(("TEXTCOLOR",  (2,ri+2),(2,ri+2), _pc(d1)))
            cmds.append(("FONTNAME",   (2,ri+2),(2,ri+2), "Helvetica-Bold"))
            rows.append([
                P(_xs(_t(str(row.get("label","")), 22)), sz=5.5, col=TXT, lead=7),
                P(_num(row.get("level")),            sz=5.5, col=TXT, lead=7),
                P(_pct(d1),  sz=5.5, col=_pc(d1),   lead=7),
                P(_pct(wtd), sz=5.5, col=_pc(wtd),  lead=7),
                P(_pct(ytd), sz=5.5, col=_pc(ytd),  lead=7),
            ])
        t = Table(rows, colWidths=cw, repeatRows=2)
        t.setStyle(TableStyle(cmds))
        return t

    GAP = 0.25*cm
    half_w = (PW/2)*cm

    # ── Split bonds_df: actual bonds (BNDW/BND/IEAG) vs crypto ───────────────
    _bond_labels = {"Global Bonds", "USD Bonds", "EUR Bonds"}
    if bonds_df is not None and not bonds_df.empty:
        actual_bonds_df = bonds_df[bonds_df["label"].isin(_bond_labels)].copy()
        crypto_df       = bonds_df[~bonds_df["label"].isin(_bond_labels)].copy()
    else:
        actual_bonds_df = pd.DataFrame()
        crypto_df       = pd.DataFrame()

    # Rates & Bonds combined
    rates_bonds_df = pd.concat([rates_df, actual_bonds_df], ignore_index=True) if not actual_bonds_df.empty else rates_df

    # Commodities & Crypto combined
    commodities_crypto_df = pd.concat([commodities_df, crypto_df], ignore_index=True) if not crypto_df.empty else commodities_df

    # FX table: EUR/CHF, EUR/USD, USD/CHF, DXY
    _fx_want = ["EUR/CHF", "EUR/USD", "USD/CHF", "DXY (USD Index)"]
    if fx_df is not None and not fx_df.empty:
        fx_pdf_df = fx_df[fx_df["label"].isin(_fx_want)].copy()
    else:
        fx_pdf_df = pd.DataFrame()

    # Row 1: Equities | Rates & Bonds
    data_r1 = Table(
        [[_dtbl(equities_df,"EQUITIES"), P(""), _dtbl(rates_bonds_df,"RATES & BONDS")]],
        colWidths=[half_w, GAP, half_w],
    )
    data_r1.setStyle(TableStyle([
        ("VALIGN",(0,0),(-1,-1),"TOP"),
        ("LEFTPADDING",(0,0),(-1,-1),0),("RIGHTPADDING",(0,0),(-1,-1),0),
        ("TOPPADDING",(0,0),(-1,-1),0),("BOTTOMPADDING",(0,0),(-1,-1),0),
    ]))
    # Row 2: Commodities & Crypto | FX
    if not fx_pdf_df.empty:
        data_r2 = Table(
            [[_dtbl(commodities_crypto_df,"COMMODITIES & CRYPTO"), P(""), _dtbl(fx_pdf_df,"FX")]],
            colWidths=[half_w, GAP, half_w],
        )
    else:
        data_r2 = Table(
            [[_dtbl(commodities_crypto_df,"COMMODITIES & CRYPTO")]],
            colWidths=[PW*cm],
        )
    data_r2.setStyle(TableStyle([
        ("VALIGN",(0,0),(-1,-1),"TOP"),
        ("LEFTPADDING",(0,0),(-1,-1),0),("RIGHTPADDING",(0,0),(-1,-1),0),
        ("TOPPADDING",(0,0),(-1,-1),0),("BOTTOMPADDING",(0,0),(-1,-1),0),
    ]))
    story += [data_r1, Spacer(1, 0.12*cm), data_r2]

    # ── 4b. PORTFOLIO IMPLICATIONS ────────────────────────────────────────────
    # 3-4 cross-asset implication bullets at the macro level. NOT individualized
    # advice — these are general consequences of the day's themes covered by
    # the disclaimer + professional-use footer. Renders as a small bordered
    # block above Research Highlights so page 2 has analytical value, not just
    # tables of bank views. AI-generated; suppressed entirely if no AI output.
    _pi_list = (writing or {}).get("portfolio_implications") or []
    _pi_list = [b for b in _pi_list if isinstance(b, str) and b.strip()][:4]
    if _pi_list:
        pi_flows = [
            P("PORTFOLIO IMPLICATIONS", fn="Helvetica-Bold", sz=5.8, col=BLU, lead=7),
            HRFlowable(width=(PW-1.0)*cm, thickness=0.5, color=BLU),
            Spacer(1, 0.04*cm),
        ]
        for b in _pi_list:
            pi_flows.append(
                P(f"<b>\u25aa</b>&nbsp;&nbsp;{_xs(b)}",
                  sz=5.6, col=TXT, lead=7.0)
            )
        pi_tbl = Table([[pi_flows]], colWidths=[PW*cm])
        pi_tbl.setStyle(TableStyle([
            ("BACKGROUND",   (0,0),(-1,-1), LIT),
            ("BOX",          (0,0),(-1,-1), 0.5, BLU),
            ("LEFTPADDING",  (0,0),(-1,-1), 9),
            ("RIGHTPADDING", (0,0),(-1,-1), 9),
            ("TOPPADDING",   (0,0),(-1,-1), 6),
            ("BOTTOMPADDING",(0,0),(-1,-1), 6),
            ("VALIGN",       (0,0),(-1,-1), "TOP"),
        ]))
        story += [Spacer(1, 0.12*cm), pi_tbl]

    # ── 5. RESEARCH HIGHLIGHTS ────────────────────────────────────────────────
    # Per-bank summary: each uploaded broker gets its own section with 3-5
    # bullets of its own views. Fallback: a compact per-doc line list when
    # Gemini synthesis returns nothing.
    _banks_list = (research_themes or {}).get("banks") if isinstance(research_themes, dict) else None

    # Debug path: if we HAVE research_docs but NO bank summaries came back,
    # show the diagnostic in the PDF itself so the user can see exactly why.
    # Without this we get silent "empty section" with no signal about why.
    if not _banks_list and research_docs:
        try:
            import streamlit as _st
            _dbg = _st.session_state.get("_research_themes_debug") or {}
        except Exception:
            _dbg = {}
        story += [
            Spacer(1, 0.06*cm),
            HRFlowable(width=PW*cm, thickness=0.5, color=RUL),
            Spacer(1, 0.03*cm),
            P("RESEARCH HIGHLIGHTS", fn="Helvetica-Bold", sz=5.8, col=NAV, lead=7),
            Spacer(1, 0.04*cm),
            P(f"<i>No AI summaries produced. Diagnostic:</i>",
              sz=4.8, col=GRY, lead=6),
        ]
        if _dbg:
            banks_rendered = _dbg.get("banks_rendered") or []
            notes = _dbg.get("notes") or []
            story.append(P(
                _xs(f"banks rendered: {banks_rendered or 'none'}"),
                sz=4.6, col=GRY, lead=6,
            ))
            for n in notes[:8]:
                story.append(P(
                    f"\u00a0\u00a0\u2022 {_xs(str(n))[:180]}",
                    sz=4.6, col=GRY, lead=5.8,
                ))
        else:
            story.append(P(
                _xs(f"research_docs count: {len(research_docs)}; "
                    "no debug info — check if build_research_themes was called."),
                sz=4.6, col=GRY, lead=6,
            ))

    if _banks_list:
        # Assign a colour per bank — used in the SOURCE column.
        _bank_palette = [BLU, GRN, RED, NAV, MID]
        _bank_colour: dict[str, object] = {}

        def _colour_for(bank: str):
            if bank not in _bank_colour:
                _bank_colour[bank] = _bank_palette[len(_bank_colour) % len(_bank_palette)]
            return _bank_colour[bank]

        # Reviewer's nice-to-have #3: render Research Highlights as a
        # 3-column table — Source | Key Message | Portfolio Implication —
        # so the section reads like an advisory note, not a summary feed.
        # Falls back to the legacy bullet list if no items came through.
        _has_items = any(
            isinstance(entry.get("items"), list) and entry.get("items")
            for entry in _banks_list
        )

        story += [
            Spacer(1, 0.06*cm),
            HRFlowable(width=PW*cm, thickness=0.5, color=RUL),
            Spacer(1, 0.03*cm),
            P("RESEARCH HIGHLIGHTS",
              fn="Helvetica-Bold", sz=5.8, col=NAV, lead=7),
            Spacer(1, 0.04*cm),
        ]

        if _has_items:
            # 2-column table — Source | Key Message.
            # The Portfolio Implication column was dropped 2026-04-27: with no
            # client-portfolio context the AI was reusing a small set of stock
            # phrases ("supports defensive sector tilt", "points to continued
            # duration risk") across unrelated points, eroding credibility.
            # Bank research is already opinionated; the Key Message carries
            # the bank's own view. The brief reports; it does not advise.
            #
            # The bank source label is repeated on EVERY row (not just the
            # first row of a bank's group) so that page breaks don't leave
            # rows orphaned without an attribution.
            rh_cw = [PW*0.18*cm, PW*0.82*cm]
            rh_rows = [[
                P("Source",      fn="Helvetica-Bold", sz=5.5, col=WHT, lead=7),
                P("Key Message", fn="Helvetica-Bold", sz=5.5, col=WHT, lead=7),
            ]]
            rh_cmds = [
                ("BACKGROUND",   (0,0),(-1,0),  NAV),
                ("TEXTCOLOR",    (0,0),(-1,0),  WHT),
                ("LINEBELOW",    (0,0),(-1,0),  1.0, BLU),
                ("VALIGN",       (0,0),(-1,-1), "TOP"),
                ("LEFTPADDING",  (0,0),(-1,-1), 4),
                ("RIGHTPADDING", (0,0),(-1,-1), 4),
                ("TOPPADDING",   (0,0),(-1,-1), 2.5),
                ("BOTTOMPADDING",(0,0),(-1,-1), 2.5),
                ("BOX",          (0,0),(-1,-1), 0.3, RUL),
            ]
            # Reviewer asked for a curated 5-7 row table, not a dump of every
            # bullet from every bank. Cap items per bank to 2 (down from 4)
            # and total table rows to 7. The prompt already orders bullets
            # by importance, so the first 2 are the strongest views.
            MAX_ITEMS_PER_BANK = 2
            MAX_TOTAL_ROWS     = 7
            ri = 1
            rows_added = 0
            for entry in _banks_list[:6]:
                if rows_added >= MAX_TOTAL_ROWS:
                    break
                bank = str(entry.get("bank") or "")
                items = entry.get("items") or []
                if not bank or not items:
                    bullets = entry.get("bullets") or []
                    if bank and bullets:
                        items = [{"key_message": b} for b in bullets if isinstance(b, str)]
                    else:
                        continue
                bcol = _colour_for(bank)
                for it in items[:MAX_ITEMS_PER_BANK]:
                    if rows_added >= MAX_TOTAL_ROWS:
                        break
                    km = (it.get("key_message") or "").strip()
                    if not km:
                        continue
                    rh_rows.append([
                        P(f'<font color="{bcol.hexval()}"><b>{_xs(bank).upper()}</b></font>',
                          sz=5.4, col=NAV, lead=6.6),
                        P(_xs(km), sz=4.9, col=TXT, lead=6.3),
                    ])
                    bg = WHT if (ri % 2 == 1) else STR
                    rh_cmds.append(("BACKGROUND", (0,ri),(-1,ri), bg))
                    ri += 1
                    rows_added += 1

            if len(rh_rows) > 1:
                rh_tbl = Table(rh_rows, colWidths=rh_cw, repeatRows=1)
                rh_tbl.setStyle(TableStyle(rh_cmds))
                story.append(rh_tbl)
        else:
            # Legacy path: bank-headed bullet list (used when no items returned at all).
            for entry in _banks_list[:6]:
                bank = str(entry.get("bank") or "")
                bullets = entry.get("bullets") or []
                if not bank or not bullets:
                    continue
                bcol = _colour_for(bank)
                story.append(P(
                    f'<font color="{bcol.hexval()}"><b>{_xs(bank).upper()}</b></font>',
                    sz=5.2, col=NAV, lead=6.5,
                ))
                for bullet in bullets[:5]:
                    if not isinstance(bullet, str):
                        continue
                    story.append(P(
                        f"\u00a0\u00a0\u2022 {_xs(bullet)}",
                        sz=4.9, col=TXT, lead=6.3,
                    ))
                story.append(Spacer(1, 0.05*cm))

    elif False:  # Mechanical fallback disabled per user preference:
                 # "I prefer to see nothing than to see this mechanical."
                 # If Gemini returns nothing, the Research Highlights
                 # section is simply omitted.
        def _infer_bank_fb(fname: str) -> str:
            fn = fname.lower()
            if "morning call" in fn:                         return "BoS"
            if "barclays"     in fn:                         return "Barclays"
            if "daily europe" in fn:                         return "UBS"
            if "equity_coverage" in fn or "universe" in fn:  return "UBS Universe"
            if "dmo"          in fn or "ocbc" in fn:         return "OCBC"
            if "goldman"      in fn or fn.startswith("gs_"): return "Goldman"
            if "jpmorgan"     in fn or fn.startswith("jpm"): return "JPMorgan"
            if "morgan stanley" in fn or fn.startswith("ms_"): return "Morgan Stanley"
            return fname.split(".")[0][:24]

        bank_bullets_fb: dict[str, list[str]] = {}
        for fname, rdoc in research_docs.items():
            try:
                bank  = _infer_bank_fb(fname)
                dtype = rdoc.get("_doc_type", "generic_research")
                bullets = bank_bullets_fb.setdefault(bank, [])
                if dtype == "equity_coverage":
                    stocks = rdoc.get("stocks") or []
                    buys  = [s["name"] for s in stocks if s.get("rating") == "Buy"][:6]
                    sells = [s["name"] for s in stocks if s.get("rating") == "Sell"][:4]
                    if buys:
                        bullets.append(f"Buys: {', '.join(buys)}")
                    if sells:
                        bullets.append(f"Sells: {', '.join(sells)}")
                elif dtype == "morning_call":
                    for vp in (rdoc.get("equity_viewpoints") or [])[:3]:
                        bullets.append(str(vp))
                    rec = rdoc.get("recommendation_changes") or {}
                    for upg in (rec.get("upgrades") or [])[:2]:
                        bullets.append(
                            f"Upgrade: {upg.get('name','')} "
                            f"{upg.get('rating_old','')}\u2192{upg.get('rating_new','')}"
                        )
                    for dwn in (rec.get("downgrades") or [])[:2]:
                        bullets.append(
                            f"Downgrade: {dwn.get('name','')} "
                            f"{dwn.get('rating_old','')}\u2192{dwn.get('rating_new','')}"
                        )
                    if not bullets:
                        text = (rdoc.get("text") or "").replace("\n", " ").strip()
                        if text:
                            bullets.append(_t(text, 220))
                else:
                    text = (rdoc.get("text") or "").replace("\n", " ").strip()
                    if text:
                        bullets.append(_t(text, 220))
                if not bullets:
                    bank_bullets_fb.pop(bank, None)
            except Exception:
                continue

        if bank_bullets_fb:
            _palette_fb = [BLU, GRN, RED, NAV, MID]
            story += [
                Spacer(1, 0.06*cm),
                HRFlowable(width=PW*cm, thickness=0.5, color=RUL),
                Spacer(1, 0.03*cm),
                P("RESEARCH HIGHLIGHTS",
                  fn="Helvetica-Bold", sz=5.8, col=NAV, lead=7),
                Spacer(1, 0.04*cm),
            ]
            for i, (bank, bullets) in enumerate(list(bank_bullets_fb.items())[:6]):
                bcol = _palette_fb[i % len(_palette_fb)]
                story.append(P(
                    f'<font color="{bcol.hexval()}"><b>{_xs(bank).upper()}</b></font>',
                    sz=5.2, col=NAV, lead=6.5,
                ))
                for bullet in bullets[:4]:
                    story.append(P(
                        f"\u00a0\u00a0\u2022 {_xs(bullet)}",
                        sz=4.9, col=TXT, lead=6.3,
                    ))
                story.append(Spacer(1, 0.05*cm))

    # ── 6. DATA TIMESTAMP + SOURCES ───────────────────────────────────────────
    # Reviewer's must-fix #3: every client-facing brief needs an explicit
    # "as-of" timestamp and a sources note, otherwise stale or partial data
    # has no anchor.
    try:
        _ts = now_zurich().strftime("%d %B %Y, %H:%M %Z")
    except Exception:
        _ts = datetime.now().strftime("%d %B %Y, %H:%M")
    footer_meta = (
        "<b>Prepared for professional/informational use. Not intended as "
        "individualized investment advice.</b> "
        f"<b>Market data as of:</b> {_xs(_ts)}. "
        "<b>Sources:</b> market data providers (Yahoo Finance, FRED), "
        "public news sources, and internal research summaries from "
        "uploaded broker documents."
    )

    # ── 7. DISCLAIMER ─────────────────────────────────────────────────────────
    disc = ("Disclaimer: This briefing is for informational purposes only and does not "
            "constitute investment advice or a recommendation to buy or sell any financial "
            "instrument. Information is believed reliable but accuracy cannot be guaranteed. "
            "Past performance is not indicative of future results. Market data may be delayed. "
            "Always consult a qualified financial adviser before making investment decisions.")
    story += [
        Spacer(1, 0.04*cm),
        HRFlowable(width=PW*cm, thickness=0.4, color=RUL),
        Spacer(1, 0.02*cm),
        P(footer_meta, sz=4.4, col=GRY, lead=5.4),
        Spacer(1, 0.02*cm),
        P(disc, sz=4.2, col=GRY, lead=5.2),
    ]

    try:
        doc.build(story)
    except Exception as e:
        # Last-ditch: surface the real error as a one-page PDF so the app
        # doesn't crash and we can see what went wrong. The UI doesn't show
        # the underlying exception text; embedding it here makes it visible.
        try:
            buffer.seek(0); buffer.truncate(0)
            doc2 = SimpleDocTemplate(buffer, pagesize=A4,
                leftMargin=1.0*cm, rightMargin=1.0*cm,
                topMargin=0.8*cm, bottomMargin=0.8*cm)
            msg = f"PDF render failed: {type(e).__name__}: {str(e)[:400]}"
            doc2.build([Paragraph(_xs(msg),
                ParagraphStyle("_err", fontName="Helvetica", fontSize=10))])
        except Exception:
            pass
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


def build_base_state(include_crypto_flag, use_gemini_flag):
    snapshot, history, chart_allowed_keys = build_bundle()

    if not include_crypto_flag:
        snapshot = snapshot[snapshot["group"] != "alternatives"].reset_index(drop=True)

    equities_df    = snapshot[snapshot["group"] == "equities"][["label", "description", "level", "d1", "wtd", "mtd", "ytd"]]
    rates_df       = snapshot[snapshot["group"] == "rates"][["label", "description", "level", "d1", "wtd", "mtd", "ytd"]]
    commodities_df = snapshot[snapshot["group"] == "commodities"][["label", "description", "level", "d1", "wtd", "mtd", "ytd"]]
    bonds_df       = snapshot[snapshot["group"].isin(["bonds", "alternatives"])][["label", "description", "level", "d1", "wtd", "mtd", "ytd"]]
    fx_df          = snapshot[snapshot["group"] == "fx"][["label", "description", "level", "d1", "wtd", "mtd", "ytd"]]

    news_df, news_status = load_news()
    research_docs    = st.session_state.get("research_docs", {})
    research_context = get_research_context(research_docs)
    writing, gemini_status = build_writing(news_df, snapshot, use_gemini_flag, research_context)

    # Cross-bank research synthesis — Gemini produces themed bullets with
    # bank attribution (e.g. "Oil: UBS says… OCBC says…"). Empty dict on
    # failure / Gemini off / no research.
    research_themes = build_research_themes(research_docs, use_gemini_flag)

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
        "global_equities_d1":  get_metric("msci_world", "d1"),
        "global_bonds_ytd":    get_metric("global_bonds", "ytd"),
        "global_bonds_d1":     get_metric("global_bonds", "d1"),
        "usd_bonds_ytd":       get_metric("usd_bonds", "ytd"),
        "usd_bonds_d1":        get_metric("usd_bonds", "d1"),
        "eur_bonds_ytd":       get_metric("eur_bonds", "ytd"),
        "eur_bonds_d1":        get_metric("eur_bonds", "d1"),
        "gold_ytd":            get_metric("gold", "ytd"),
        "gold_d1":             get_metric("gold", "d1"),
        "bitcoin_ytd":         get_metric("bitcoin", "ytd"),
        "bitcoin_d1":          get_metric("bitcoin", "d1"),
    }

    return {
        "snapshot": snapshot,
        "history": history,
        "equities_df": equities_df,
        "rates_df": rates_df,
        "commodities_df": commodities_df,
        "bonds_df": bonds_df,
        "fx_df": fx_df,
        "news_df": news_df,
        "writing": writing,
        "status": status,
        "metrics": metrics,
        "chart_allowed_keys": chart_allowed_keys,
        "include_crypto_flag": include_crypto_flag,
        "research_themes": research_themes,
    }


def _fig_to_png(fig, width, height, scale=1.5):
    """Shared kaleido export helper. Returns bytes or None.
    Handles both kaleido 0.2.x (pio.kaleido.scope.mathjax) and kaleido 1.x
    (scope attribute removed) — Streamlit Cloud resolves the version non-
    deterministically and either can be installed."""
    try:
        import plotly.io as pio
        # Best-effort: disable mathjax on kaleido 0.2.x only. On 1.x the
        # attribute doesn't exist and we silently skip.
        try:
            pio.kaleido.scope.mathjax = None
        except Exception:
            pass
        return pio.to_image(fig, format="png", scale=scale, width=width, height=height)
    except Exception:
        try:
            return fig.to_image(format="png", scale=1, width=width, height=height)
        except Exception:
            return None


def add_render_outputs(base_state, chart_window="YTD"):
    # history + chart_allowed_keys are stripped from serialized snapshots
    # (too large for JSON/Gist). Re-hydrate from a fresh build_bundle() call
    # when they're missing so we never KeyError out of a saved snapshot.
    if "history" not in base_state or "chart_allowed_keys" not in base_state:
        try:
            _snap, fresh_history, fresh_chart_keys = build_bundle()
            base_state.setdefault("history", fresh_history)
            base_state.setdefault("chart_allowed_keys", fresh_chart_keys)
        except Exception:
            base_state.setdefault("history", pd.DataFrame(
                columns=["date", "key", "label", "group", "value", "source_type"]))
            base_state.setdefault("chart_allowed_keys", [])
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
            pdf_chart_png = _fig_to_png(pdf_fig, width=940, height=400)

    # Chart of the Day — compute before build_pdf so it can appear in PDF
    cotd = pick_chart_of_day(history, base_state.get("news_df"))

    # Generate cotd chart PNG if we have a valid cotd and history
    cotd_png = None
    if cotd and isinstance(cotd, dict) and cotd.get("key"):
        try:
            key = cotd["key"]
            tf  = int(cotd.get("timeframe_days", 60))
            g   = history[history["key"] == key].sort_values("date")
            cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=tf)
            g   = g[g["date"] >= cutoff]
            if not g.empty:
                cotd_fig = go.Figure()
                cotd_fig.add_trace(go.Scatter(
                    x=g["date"], y=g["value"], mode="lines",
                    line=dict(width=2, color=PRIMARY), fill="tozeroy",
                    fillcolor="rgba(16,59,115,0.07)",
                ))
                cotd_fig.update_layout(
                    height=200, margin=dict(l=10, r=10, t=10, b=25),
                    plot_bgcolor="white", paper_bgcolor="white", showlegend=False,
                    xaxis=dict(showgrid=False, tickformat="%d %b", tickfont=dict(size=7)),
                    yaxis=dict(showgrid=True, gridcolor="#F0F4F8", tickfont=dict(size=7)),
                )
                cotd_png = _fig_to_png(cotd_fig, width=420, height=210)
        except Exception:
            cotd_png = None

    pdf_bytes = build_pdf(
        "Daily Market Briefing",
        pdf_chart_png,
        base_state["equities_df"],
        base_state["rates_df"],
        base_state["commodities_df"],
        base_state.get("bonds_df", base_state.get("commodities_df")),
        base_state["metrics"],
        base_state["writing"],
        base_state["news_df"],
        base_state["status"],
        cotd=cotd,
        cotd_png=cotd_png,
        fx_df=base_state.get("fx_df"),
        research_docs=st.session_state.get("research_docs"),
        research_themes=base_state.get("research_themes"),
    )

    state = dict(base_state)
    state["fig"]           = fig
    state["pdf_bytes"]     = pdf_bytes
    state["pdf_chart_png"] = pdf_chart_png
    state["chart_of_day"]  = cotd
    return state


# ── Fixed constants (not user-selectable) ────────────────────────────────────
include_crypto    = True         # always include crypto
chart_mode        = "Expanded"   # always use extended chart
use_gemini_writing = True        # always attempt Gemini (falls back if key missing)

generate = False

with st.sidebar:
    st.markdown("**Mode**")
    mode = st.radio("", ["Live", "Morning snapshot"], index=1, label_visibility="collapsed")
    st.caption("Snapshot freezes at 08:00 Zurich.")

    st.markdown("**Chart window**")
    chart_window = st.radio("", ["YTD", "3 months", "6 months", "1 year"], index=0, label_visibility="collapsed")

    st.markdown("**Options**")
    show_definitions = st.checkbox("Show definitions", value=False)
    auto_refresh     = st.checkbox("Auto-refresh (live)", value=False)
    if auto_refresh:
        refresh_seconds = st.selectbox("Every (s)", [30, 60, 120, 300], index=1, label_visibility="visible")
        if mode == "Live":
            st_autorefresh(interval=refresh_seconds * 1000, key="live_refresh")

    if st.button("🔄 Refresh", use_container_width=True):
        st.rerun()

    st.markdown("---")
    st.markdown("**📚 Research Library**")

    # Auto-pull PDFs from Google Drive (SNIPER/Research_Processed + Inbox).
    # This runs once per session; the 🔄 button below forces a re-sync.
    loaded, failed = autoload_research_from_drive()
    if _drive_pdf_loader is not None:
        drive_count = len(st.session_state.get("research_docs", {}))
        failed_docs = st.session_state.get("_drive_research_failed", {})
        if loaded or failed:
            parts = []
            if loaded:
                parts.append(f"☁️ Auto-loaded {loaded} PDF(s) from Drive")
            if failed:
                parts.append(f"⚠️ {failed} failed to parse")
            st.caption(" · ".join(parts))
        else:
            st.caption(f"☁️ Drive sync: {drive_count} PDF(s) in library.")
        if failed_docs:
            for fname, err in list(failed_docs.items())[:5]:
                st.caption(f"⚠️ {fname[:35]} — {str(err)[:60]}")
        # Diagnostic: show research-themes grounding debug if a Brief was
        # generated in this session. Helps spot why cross-bank view is empty.
        _rt_dbg = st.session_state.get("_research_themes_debug")
        if _rt_dbg:
            seen = _rt_dbg.get("seen", 0)
            kept = _rt_dbg.get("kept", 0)
            if seen:
                st.caption(f"🧪 Research themes: {kept}/{seen} bullets passed grounding check")
                for drop in (_rt_dbg.get("drops") or [])[:5]:
                    st.caption(f"   ✗ {drop[:90]}")
            elif _drive_pdf_loader is not None:
                st.caption("🧪 Research themes: Gemini returned no themed output (fallback to per-doc)")
        if st.button("🔄 Re-sync from Drive", use_container_width=True, key="drive_resync"):
            autoload_research_from_drive(force_refresh=True)
            st.rerun()
    else:
        st.caption("⚠️ Drive sync unavailable — install google-api-python-client / google-auth.")

    st.caption("Or drop PDFs manually (Morning Call, Equity Universe, Fixed Income, reports…):")
    uploaded_files = st.file_uploader(
        "Drop PDFs here", type=["pdf"],
        accept_multiple_files=True,
        key="research_uploads",
    )
    if uploaded_files:
        # Session-only uploads. Drive upload is disabled — service accounts
        # can't write to personal My Drive folders (Google policy). Add PDFs
        # to Google Drive manually if you want them to persist across
        # sessions / be picked up by the autopilot.
        if "research_docs" not in st.session_state:
            st.session_state["research_docs"] = {}
        for f in uploaded_files:
            if f.name not in st.session_state["research_docs"]:
                pdf_bytes = f.read()
                doc = auto_detect_and_parse(pdf_bytes, f.name)
                st.session_state["research_docs"][f.name] = doc

    # Show the current library (whether loaded from Drive, uploaded, or both).
    docs = st.session_state.get("research_docs", {})
    if docs:
        for fname, doc in docs.items():
            dtype = doc.get("_doc_type", "generic")
            icon = {"morning_call": "🏦", "equity_coverage": "📊",
                    "fixed_income_coverage": "📈", "preferred_fi": "⭐",
                    "monthly_guide": "📘"}.get(dtype, "📄")
            err = " ⚠️" if doc.get("error") else " ✅"
            st.caption(f"{icon} {fname[:35]}{err}")
        if st.button("🗑 Clear library", use_container_width=True):
            st.session_state["research_docs"] = {}
            # Let the Drive autoload re-populate on the next run if desired.
            st.session_state["_drive_research_loaded"] = False
            st.rerun()

    st.markdown("---")
    generate = st.button("▶ Generate Brief", type="primary", use_container_width=True)

if generate:
    znow = now_zurich()
    today_str = znow.date().isoformat()

    if mode == "Live":
        base_state = build_base_state(include_crypto, use_gemini_writing)
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
                base_state = build_base_state(include_crypto, use_gemini_writing)
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
                    base_state = build_base_state(include_crypto, use_gemini_writing)
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
    ai_detail = status.get("gemini_reason", "")
    ai_label  = ai_detail if status["gemini_used"] else f"OFF ({ai_detail[:55]})"
    st.caption(
        f"{mode_note}   |   {g_col} AI {ai_label}  "
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
        st.markdown("**📅 Upcoming Events**")
        today = pd.Timestamp.today().normalize()
        upcoming = [e for e in MACRO_EVENTS if pd.Timestamp(e["date"]) >= today][:6]
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
        st.markdown(
            "<div style='background:#FFF8E1;border:1px solid #FFD54F;border-radius:8px;"
            "padding:8px 10px;font-size:11px;color:#5D4037;'>"
            "⚠️ <b>Disclaimer:</b> This briefing is for informational purposes only and does not "
            "constitute investment advice, a solicitation, or a recommendation to buy or sell any "
            "financial instrument. Past performance is not indicative of future results. Always "
            "consult a qualified financial adviser before making investment decisions."
            "</div>",
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 3b. Research Library (Morning Call, Equity Universe, etc.) ───────────
    render_research_library()
    if st.session_state.get("research_docs"):
        today_str = pd.Timestamp.today().date().isoformat()
        # Save snapshot for tracking
        save_research_snapshot(st.session_state["research_docs"], today_str)
        # Show changes vs previous day
        changes = diff_research_snapshots(st.session_state["research_docs"], today_str)
        if changes:
            with st.expander(f"📋 Research Changes vs Previous Day ({len(changes)} change{'s' if len(changes)!=1 else ''})", expanded=True):
                for ch in changes:
                    ctype = ch.get("type","")
                    name  = ch.get("name","")
                    old   = ch.get("old","")
                    new   = ch.get("new","")
                    src   = ch.get("src","")[:25]
                    if ctype in ("upgrade",):
                        st.markdown(f"🟢 **UPGRADE** {name}: {old} → **{new}** *(from {src})*")
                    elif ctype in ("downgrade",):
                        st.markdown(f"🔴 **DOWNGRADE** {name}: {old} → **{new}** *(from {src})*")
                    elif ctype == "rating":
                        icon = "🟢" if new in ("Buy",) else ("🔴" if new in ("Sell",) else "🟡")
                        st.markdown(f"{icon} **RATING CHANGE** {name}: {old} → **{new}** *(from {src})*")
                    elif ctype == "fv":
                        st.markdown(f"📌 **FV CHANGE** {name}: {old} → **{new}** *(from {src})*")
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
        tabs = st.tabs(["Equities", "Rates", "Commodities", "Bonds & Crypto", "Definitions", "History"])
        with tabs[0]:
            st.dataframe(compact_table(st.session_state["equities_df"]),   use_container_width=True, height=300)
        with tabs[1]:
            st.dataframe(compact_table(st.session_state["rates_df"]),      use_container_width=True, height=180)
        with tabs[2]:
            st.dataframe(compact_table(st.session_state["commodities_df"]),use_container_width=True, height=260)
        with tabs[3]:
            st.dataframe(compact_table(st.session_state.get("bonds_df", st.session_state["commodities_df"])), use_container_width=True, height=260)
        with tabs[4]:
            import streamlit as _st
            dfs_def = []
            for key in ["equities_df", "rates_df", "commodities_df"]:
                df_src = st.session_state.get(key)
                if df_src is not None:
                    dfs_def.append(definitions_table(df_src))
            if dfs_def:
                combined = pd.concat(dfs_def, ignore_index=True).drop_duplicates()
                st.dataframe(
                    combined,
                    use_container_width=True,
                    height=520,
                    column_config={
                        "label":       st.column_config.TextColumn("Instrument", width="small"),
                        "description": st.column_config.TextColumn("Description & What a Move Means", width="large"),
                    },
                )
            else:
                st.info("Generate the brief first.")
        with tabs[5]:
            st.dataframe(st.session_state["history"], use_container_width=True, height=480)

    # ── 7. PDF download ───────────────────────────────────────────────────────
    st.markdown("---")

    st.download_button(
        "⬇  Download PDF newsletter",
        st.session_state["pdf_bytes"],
        file_name=f"daily_brief_{pd.Timestamp.today().date()}.pdf",
        mime="application/pdf",
        use_container_width=True,
    )
