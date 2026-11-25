"""Personal (CHF-investor) additions to the NXMAN Daily Brief.

Dashboard-only panels — nothing here feeds the one-page PDF, so the PDF
layout is unchanged.

  * fetch_bund_yield / fetch_swiss_yield — live official yields
    (Deutsche Bundesbank, Swiss National Bank) replacing the manual
    MANUAL_BUND_10Y / MANUAL_CH_10Y numbers.
  * render_what_matters_today — short top-of-page block: CHF line + 3 bullets.
  * render_chf_lens          — key assets: local-currency vs CHF returns.
  * render_bond_panel        — yields (bp) and bond-ETF prices (%).
  * load_cyprus_news / render_cyprus_news — Cyprus business headlines.

Every fetcher fails soft: a failed source shows as "unavailable", it never
breaks the page. All numbers come from the fetched series — nothing is
hard-coded or AI-generated here.
"""
from __future__ import annotations

import io
import re
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import streamlit as st

_UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) NXMAN-Brief/1.0"}

# ── Official yield sources ───────────────────────────────────────────────────
# Bundesbank: daily yield of the *current* Federal security of each tenor.
BUNDESBANK_SERIES = {
    "2y":  "D.REN.EUR.A610.000000WT0202.A",
    "5y":  "D.REN.EUR.A620.000000WT0505.A",
    "10y": "D.REN.EUR.A630.000000WT1010.A",
    "30y": "D.REN.EUR.A640.000000WT3030.A",
}
# SNB data portal: Swiss Confederation bond yields by maturity.
# rendoblid (daily) froze on 2025-07-31. The SNB now publishes the same daily
# yields in cube rendeiduebd (D0=CHF, D1=<tenor>), but only once a month, so
# the latest value can be up to ~5 weeks old — the panel shows its date.
SNB_CUBES = ["rendeiduebd", "rendoblid"]


def _parse_sdmx_csv(text: str) -> pd.Series:
    """Parse Bundesbank output: SDMX-CSV (TIME_PERIOD/OBS_VALUE) or the
    'download' CSV (date in col 0, value in col 1, metadata rows on top)."""
    try:
        df = pd.read_csv(io.StringIO(text))
        if "TIME_PERIOD" in df.columns and "OBS_VALUE" in df.columns:
            s = pd.Series(pd.to_numeric(df["OBS_VALUE"], errors="coerce").values,
                          index=pd.to_datetime(df["TIME_PERIOD"], errors="coerce"))
            return s.dropna()[lambda x: x.index.notna()].sort_index()
    except Exception:
        pass
    rows = []
    for line in text.splitlines():
        sep = ";" if ";" in line else ","  # ';' files use decimal commas
        parts = [p.strip().strip('"') for p in line.split(sep, 2)]
        if len(parts) < 2 or not re.match(r"^\d{4}-\d{2}-\d{2}$", parts[0]):
            continue
        try:
            rows.append((pd.Timestamp(parts[0]), float(parts[1].replace(",", "."))))
        except ValueError:
            continue
    if not rows:
        return pd.Series(dtype=float)
    d, v = zip(*rows)
    return pd.Series(v, index=pd.DatetimeIndex(d)).sort_index()


@st.cache_data(ttl=3600)
def fetch_bund_yield(tenor: str = "10y") -> pd.Series:
    """Daily German Federal bond yield (% p.a.) from the Bundesbank API."""
    key = BUNDESBANK_SERIES[tenor]
    start = (pd.Timestamp.today() - pd.Timedelta(days=400)).strftime("%Y-%m-%d")
    urls = [
        f"https://api.statistiken.bundesbank.de/rest/data/BBSSY/{key}?format=csvdata&startPeriod={start}",
        f"https://api.statistiken.bundesbank.de/rest/download/BBSSY/{key}?format=csv&lang=en",
    ]
    last_err = None
    for u in urls:
        try:
            r = requests.get(u, headers=_UA, timeout=25)
            r.raise_for_status()
            s = _parse_sdmx_csv(r.text)
            s = s[s.index >= pd.Timestamp(start)]
            if len(s) >= 20:
                return s
        except Exception as e:  # try next format
            last_err = e
    raise ValueError(f"Bundesbank {tenor} unavailable ({last_err})")


@st.cache_data(ttl=3600)
def fetch_swiss_yield(tenor: str = "10y") -> tuple[pd.Series, str]:
    """Swiss Confederation yield (% p.a.) from the SNB data portal.
    Returns (series, note). Note is 'monthly release' because the SNB
    publishes the daily values once a month."""
    n = tenor.lower().rstrip("y")
    codes = {f"{n}J", f"{n}J0"}  # e.g. 10J (rendeiduebd) or 10J0 (rendoblid)
    for cube in SNB_CUBES:
        try:
            r = requests.get(f"https://data.snb.ch/api/cube/{cube}/data/csv/en",
                             headers=_UA, timeout=30)
            r.raise_for_status()
            rows, hdr = [], None
            for line in r.text.splitlines():
                p = [x.strip().strip('"') for x in line.split(";")]
                if hdr is None:
                    if p and p[0] == "Date" and "Value" in p:
                        hdr = p
                    continue
                if len(p) != len(hdr):
                    continue
                rec = dict(zip(hdr, p))
                dims = [rec[c] for c in hdr if c not in ("Date", "Value")]
                if not codes.intersection(dims) or rec.get("Value") in ("", None):
                    continue
                if len(dims) > 1 and "CHF" not in dims:
                    continue
                try:
                    rows.append((pd.Timestamp(rec["Date"]), float(rec["Value"])))
                except ValueError:
                    continue
            if not rows:
                continue
            d, v = zip(*rows)
            s = pd.Series(v, index=pd.DatetimeIndex(d)).sort_index()
            s = s[~s.index.duplicated(keep="last")]
            s = s[s.index >= pd.Timestamp.today() - pd.Timedelta(days=400)]
            if len(s) >= 20:
                return s, "monthly release"
        except Exception:
            continue
    raise ValueError(f"SNB {tenor} unavailable")


# ── Generic helpers ──────────────────────────────────────────────────────────
def _on_or_before(s: pd.Series, when) -> float | None:
    e = s[s.index <= pd.Timestamp(when)]
    return None if e.empty else float(e.iloc[-1])


def _series_from_history(history: pd.DataFrame, key: str) -> pd.Series | None:
    if history is None or history.empty:
        return None
    g = history[history["key"] == key].sort_values("date")
    if g.empty:
        return None
    return pd.Series(g["value"].astype(float).values,
                     index=pd.to_datetime(g["date"])).dropna()


def _periods():
    t = pd.Timestamp.today().normalize()
    return {"7d": t - pd.Timedelta(days=7),
            "1m": t - pd.DateOffset(months=1),
            "ytd": pd.Timestamp(t.year - 1, 12, 31)}


def _pct(a, b):
    return None if a is None or b in (None, 0) else (a / b - 1) * 100


def _fmt(v, kind="pct"):
    if v is None or pd.isna(v):
        return "–"
    return f"{v:+.0f} bp" if kind == "bp" else f"{v:+.2f}%"


def _colour(v, up_good=True):
    if v is None or pd.isna(v) or abs(v) < 1e-9:
        return "#64748B"
    return "#16A34A" if (v > 0) == up_good else "#DC2626"


# ── CHF lens ─────────────────────────────────────────────────────────────────
# (key, label, currency of the series). Series are priced in this currency.
CHF_LENS_ASSETS = [
    ("smi",          "SMI",                "CHF"),
    ("msci_world",   "MSCI World",         "USD"),
    ("sp500",        "S&P 500",            "USD"),
    ("nasdaq100",    "Nasdaq 100",         "USD"),
    ("stoxx600",     "Stoxx Europe 600",   "EUR"),
    ("global_bonds", "Global Bonds (ETF)", "USD"),
    ("usd_bonds",    "USD Bonds (ETF)",    "USD"),
    ("eur_bonds",    "Intl/EUR Bonds (ETF)", "USD"),
    ("gold",         "Gold",               "USD"),
    ("wti",          "WTI Oil",            "USD"),
    ("bitcoin",      "Bitcoin",            "USD"),
]


def _snap_row(snapshot: pd.DataFrame, key: str):
    if snapshot is None or snapshot.empty:
        return None
    r = snapshot[snapshot["key"] == key]
    return None if r.empty else r.iloc[0]


def _num(v):
    try:
        return None if v is None or pd.isna(v) else float(v)
    except (TypeError, ValueError):
        return None


def _to_chf(local, fx):
    """Compound a local-currency return with the FX return (both in %)."""
    if local is None or fx is None:
        return None
    return ((1 + local / 100) * (1 + fx / 100) - 1) * 100


def build_chf_lens(snapshot: pd.DataFrame) -> pd.DataFrame:
    """Local-currency vs CHF returns, built from the SAME snapshot the rest of
    the brief shows (frozen at 08:00 in morning-snapshot mode), so the numbers
    always agree with the cards and tables. CHF return = (1+local)(1+FX)-1."""
    fx_rows = {"USD": _snap_row(snapshot, "usdchf"), "EUR": _snap_row(snapshot, "eurchf")}
    rows = []
    for key, label, ccy in CHF_LENS_ASSETS:
        r = _snap_row(snapshot, key)
        if r is None or _num(r.get("level")) is None:
            continue
        f = None if ccy == "CHF" else fx_rows.get(ccy)
        if ccy != "CHF" and f is None:
            continue
        row = {"Asset": label, "Ccy": ccy}
        for col, name in (("d1", "1D"), ("wtd", "WTD"), ("ytd", "YTD")):
            loc = _num(r.get(col))
            fxv = 0.0 if ccy == "CHF" else _num(f.get(col))
            row[f"{name} local"] = loc
            row[f"{name} chf"] = _to_chf(loc, fxv)
        if row["YTD local"] is not None and row["YTD chf"] is not None:
            row["FX effect YTD"] = row["YTD chf"] - row["YTD local"]
        rows.append(row)
    return pd.DataFrame(rows)


def _fx_line(snapshot: pd.DataFrame) -> list[dict]:
    out = []
    for key, label in (("eurchf", "EUR/CHF"), ("usdchf", "USD/CHF"), ("eurusd", "EUR/USD")):
        r = _snap_row(snapshot, key)
        if r is None or _num(r.get("level")) is None:
            continue
        out.append({"label": label, "level": _num(r["level"]), "d1": _num(r.get("d1")),
                    "wtd": _num(r.get("wtd")), "ytd": _num(r.get("ytd"))})
    return out


def render_chf_lens(snapshot: pd.DataFrame):
    st.markdown("#### 🇨🇭 CHF investor lens")
    fx = _fx_line(snapshot)
    if fx:
        cols = st.columns(len(fx))
        for c, r in zip(cols, fx):
            c.metric(r["label"], f"{r['level']:.4f}",
                     f"{r['d1']:+.2f}% 1D" if r["d1"] is not None else None)
            c.caption(f"WTD {_fmt(r['wtd'])} · YTD {_fmt(r['ytd'])}")
        st.caption("EUR/CHF or USD/CHF falling = the franc is strengthening, which "
                   "reduces the CHF value of EUR / USD holdings.")
    df = build_chf_lens(snapshot)
    if df.empty:
        st.caption("CHF lens unavailable — FX history missing from this snapshot.")
        return
    show = df[["Asset", "Ccy", "1D local", "1D chf", "WTD local", "WTD chf",
               "YTD local", "YTD chf", "FX effect YTD"]]
    num_cols = [c for c in show.columns if c not in ("Asset", "Ccy")]

    def _c(v):
        return f"color: {_colour(v)}" if isinstance(v, (int, float)) else ""

    styled = (show.style.format({c: (lambda v: _fmt(v)) for c in num_cols})
              .map(_c, subset=num_cols))
    st.dataframe(styled, use_container_width=True, hide_index=True)
    st.caption("Price returns. 'chf' = the asset's move as experienced by a CHF "
               "investor (unhedged): local price × USD/CHF or EUR/CHF. "
               "'FX effect' = what the currency added (+) or took away (–) YTD.")


# ── Bonds panel ──────────────────────────────────────────────────────────────
# Bond ETF prices — liquid US-listed proxies (yfinance). (label, tickers)
BOND_ETFS = [
    ("US Treasuries 1–3Y",      ["SHY"]),
    ("US Treasuries 7–10Y",     ["IEF"]),
    ("US Treasuries 20Y+",      ["TLT"]),
    ("US Investment Grade Corp", ["LQD"]),
    ("US High Yield",           ["HYG", "JNK"]),
    ("EM USD Sovereign",        ["EMB"]),
    ("Global Agg (USD)",        ["BNDW", "AGGG.L"]),
    ("Intl Agg (USD-hedged)",   ["BNDX"]),
    ("Swiss Govt Bonds (CHF)",  ["CSBGC0.SW", "CSBGC3.SW", "CSBGC7.SW"]),
]


@st.cache_data(ttl=900)
def _yf_close(tickers: tuple) -> tuple[pd.Series, str]:
    import yfinance as yf
    for t in tickers:
        try:
            df = yf.download(t, period="15mo", interval="1d", progress=False,
                             auto_adjust=False, threads=False)
            if df is None or df.empty:
                continue
            s = df["Close"]
            if isinstance(s, pd.DataFrame):
                s = s.iloc[:, 0]
            s = s.dropna()
            if len(s) >= 20 and s.tail(10).round(6).nunique() >= 3:
                return s, t
        except Exception:
            continue
    raise ValueError(f"no data for {tickers}")


@st.cache_data(ttl=900)
def _fred(series_id: str, api_key: str) -> pd.Series:
    r = requests.get("https://api.stlouisfed.org/fred/series/observations",
                     params={"series_id": series_id, "api_key": api_key,
                             "file_type": "json",
                             "observation_start": (pd.Timestamp.today() - pd.Timedelta(days=400)).strftime("%Y-%m-%d")},
                     timeout=25)
    r.raise_for_status()
    obs = [o for o in r.json().get("observations", []) if o.get("value") not in (".", "", None)]
    return pd.Series([float(o["value"]) for o in obs],
                     index=pd.to_datetime([o["date"] for o in obs]))


def build_bond_panel(history: pd.DataFrame, fred_key: str | None):
    """Returns (yields_df, etf_df). Yield changes in bp, ETF changes in %."""
    per = _periods()
    ycurves = []  # (region, tenor, loader)

    def _us(sid, yf_t=None):
        def f():
            if fred_key:
                try:
                    return _fred(sid, fred_key), "FRED"
                except Exception:
                    pass
            if yf_t:
                s, _ = _yf_close((yf_t,))
                return s / 10.0 if yf_t in ("^TNX", "^FVX", "^TYX") else s, "Yahoo"
            raise ValueError(sid)
        return f

    ycurves += [("United States", "2Y",  _us("DGS2")),
                ("United States", "5Y",  _us("DGS5", "^FVX")),
                ("United States", "10Y", _us("DGS10", "^TNX")),
                ("United States", "30Y", _us("DGS30", "^TYX"))]
    for t in ("2y", "5y", "10y", "30y"):
        ycurves.append(("Germany", t.upper(), (lambda t=t: (fetch_bund_yield(t), "Bundesbank"))))
    for t in ("2y", "5y", "10y", "30y"):
        def f(t=t):
            s, _note = fetch_swiss_yield(t)
            return s, "SNB (monthly release)"
        ycurves.append(("Switzerland", t.upper(), f))

    yrows = []
    for region, tenor, loader in ycurves:
        try:
            s, src = loader()
            s = s.dropna()
            last = float(s.iloc[-1])
            prev = float(s.iloc[-2]) if len(s) >= 2 else None

            def bp(ref):
                return None if ref is None else (last - ref) * 100
            yrows.append({"Market": region, "Tenor": tenor, "Yield %": last,
                          "1D": bp(prev), "7D": bp(_on_or_before(s, per["7d"])),
                          "1M": bp(_on_or_before(s, per["1m"])),
                          "YTD": bp(_on_or_before(s, per["ytd"])),
                          "As of": s.index[-1].strftime("%d %b"), "Source": src})
        except Exception:
            yrows.append({"Market": region, "Tenor": tenor, "Yield %": None,
                          "1D": None, "7D": None, "1M": None, "YTD": None,
                          "As of": "", "Source": "unavailable"})
    ydf = pd.DataFrame(yrows)

    erows = []
    for label, tickers in BOND_ETFS:
        try:
            s, used = _yf_close(tuple(tickers))
            last = float(s.iloc[-1])
            erows.append({"Bond segment": label, "Ticker": used, "Price": last,
                          "1D": _pct(last, float(s.iloc[-2])),
                          "7D": _pct(last, _on_or_before(s, per["7d"])),
                          "1M": _pct(last, _on_or_before(s, per["1m"])),
                          "YTD": _pct(last, _on_or_before(s, per["ytd"])),
                          "As of": s.index[-1].strftime("%d %b")})
        except Exception:
            continue  # skip silently — no N/A rows
    return ydf, pd.DataFrame(erows)


def render_bond_panel(history: pd.DataFrame, fred_key: str | None):
    st.markdown("#### 🏦 Bonds — yields and prices")
    with st.spinner("Loading bond data…"):
        ydf, edf = build_bond_panel(history, fred_key)

    st.markdown("**Government bond yields** · changes in basis points "
                "(yield up = bond prices down)")
    pivot_ok = ydf["Yield %"].notna().any()
    if pivot_ok:
        def _yc(v):  # yields up = red (prices fall)
            return f"color: {_colour(v, up_good=False)}" if isinstance(v, (int, float)) else ""
        styled = (ydf.style
                  .format(na_rep="–")
                  .format({"Yield %": lambda v: "–" if pd.isna(v) else f"{v:.2f}",
                           **{c: (lambda v: _fmt(v, 'bp')) for c in ("1D", "7D", "1M", "YTD")}})
                  .map(_yc, subset=["1D", "7D", "1M", "YTD"]))
        st.dataframe(styled, use_container_width=True, hide_index=True,
                     height=min(36 * (len(ydf) + 1) + 4, 480))
        # Simple curve snapshot: 10Y minus 2Y per market.
        spreads = []
        for m in ydf["Market"].unique():
            sub = ydf[ydf["Market"] == m].set_index("Tenor")["Yield %"]
            if pd.notna(sub.get("10Y")) and pd.notna(sub.get("2Y")):
                spreads.append(f"{m} 2s10s {(sub['10Y'] - sub['2Y']) * 100:+.0f} bp")
        if spreads:
            st.caption("Curve: " + " · ".join(spreads) +
                       "  (negative = inverted curve)")
    else:
        st.caption("Yield sources unavailable on this run.")

    if not edf.empty:
        st.markdown("**Bond ETF prices** · price returns")

        def _c(v):
            return f"color: {_colour(v)}" if isinstance(v, (int, float)) else ""
        styled = (edf.style
                  .format({"Price": "{:,.2f}", **{c: (lambda v: _fmt(v)) for c in ("1D", "7D", "1M", "YTD")}})
                  .map(_c, subset=["1D", "7D", "1M", "YTD"]))
        st.dataframe(styled, use_container_width=True, hide_index=True)
    st.caption("Sources: US Treasury yields · FRED (Yahoo fallback); German Bunds · "
               "Deutsche Bundesbank (current benchmark issue); Swiss Confederation · "
               "SNB data portal (published monthly, so up to ~5 weeks behind — see 'As of'); bond ETFs · Yahoo Finance. Rows marked 'unavailable' "
               "failed to load on this run.")


# ── What matters today ───────────────────────────────────────────────────────
def _deterministic_bullets(snapshot: pd.DataFrame) -> list[str]:
    """Fact-only bullets built from the snapshot when no AI summary exists."""
    out = []

    def row(k):
        r = snapshot[snapshot["key"] == k]
        return None if r.empty else r.iloc[0]

    eq = snapshot[(snapshot["group"] == "equities") & snapshot["d1"].notna()]
    if not eq.empty:
        up = int((eq["d1"] > 0).sum())
        best = eq.loc[eq["d1"].idxmax()]
        worst = eq.loc[eq["d1"].idxmin()]
        out.append(f"Equities: {up} of {len(eq)} indices up on the day — best "
                   f"{best['label']} {best['d1']:+.2f}%, weakest {worst['label']} {worst['d1']:+.2f}%.")
    r = row("eurchf")
    if r is not None and pd.notna(r["d1"]) and r["level"] is not None:
        direction = "weaker" if r["d1"] > 0 else "stronger" if r["d1"] < 0 else "unchanged"
        ytd = f", {r['ytd']:+.2f}% YTD" if r["ytd"] is not None and pd.notna(r["ytd"]) else ""
        out.append(f"Franc: EUR/CHF {float(r['level']):.4f} ({r['d1']:+.2f}% 1D{ytd}) "
                   f"— CHF {direction} vs EUR on the day.")
    parts = []
    for k, lbl in (("us10y", "US 10Y"), ("bund10y", "Bund 10Y"), ("ch10y", "CH 10Y")):
        r = row(k)
        if r is not None and r["level"] is not None and pd.notna(r["level"]):
            parts.append(f"{lbl} {float(r['level']):.2f}%" + (" (SNB, monthly data)" if k == "ch10y" else ""))
    if parts:
        out.append("Rates: " + " · ".join(parts) + ".")
    return out[:3]


def render_what_matters_today(writing: dict, snapshot: pd.DataFrame, history: pd.DataFrame):
    fx = _fx_line(snapshot)
    fx_html = " &nbsp;·&nbsp; ".join(
        f"<b>{r['label']}</b> {r['level']:.4f} "
        f"<span style='color:{_colour(r['d1'])};font-weight:700;'>{_fmt(r['d1'])}</span>"
        for r in fx[:2])

    es = (writing or {}).get("executive_summary") or {}
    lead = (es.get("lead") or "").strip()
    bullets = [b for b in (es.get("bullets") or []) if isinstance(b, str) and b.strip()][:3]
    tag = "AI summary — verify before acting"
    if not bullets:
        bullets = _deterministic_bullets(snapshot)
        lead = ""
        tag = "computed from today's data"

    items = "".join(f"<li style='margin:2px 0;'>{b}</li>" for b in bullets)
    st.markdown(
        "<div style='background:#F8FAFC;border:1px solid #CBD5E1;border-left:5px solid #0F2D52;"
        "border-radius:10px;padding:10px 14px;margin:4px 0 12px;'>"
        "<div style='display:flex;justify-content:space-between;flex-wrap:wrap;gap:6px;'>"
        "<span style='font-size:13px;font-weight:800;color:#0F2D52;letter-spacing:.3px;'>"
        "WHAT MATTERS TODAY</span>"
        f"<span style='font-size:12.5px;color:#0F2D52;'>{fx_html}</span></div>"
        + (f"<div style='font-size:13px;color:#1E293B;margin-top:6px;'>{lead}</div>" if lead else "")
        + f"<ul style='font-size:13px;color:#1E293B;margin:6px 0 2px 18px;padding:0;'>{items}</ul>"
        f"<div style='font-size:10.5px;color:#94A3B8;'>{tag}</div></div>",
        unsafe_allow_html=True,
    )


# ── Cyprus news ──────────────────────────────────────────────────────────────
# Direct publisher feeds first; Google News site-filtered feeds as fallback
# (they keep working when a publisher blocks non-browser requests).
CYPRUS_FEEDS = [
    ("Cyprus Mail",   "https://cyprus-mail.com/category/business-finance/feed/"),
    ("in-cyprus",     "https://in-cyprus.philenews.com/feed/"),
    ("Cyprus Mail",   "https://news.google.com/rss/search?q=when:2d+site:cyprus-mail.com+(business+OR+economy+OR+bank+OR+property+OR+tax)&hl=en-US&gl=US&ceid=US:en"),
    ("in-cyprus",     "https://news.google.com/rss/search?q=when:2d+site:in-cyprus.philenews.com&hl=en-US&gl=US&ceid=US:en"),
]


@st.cache_data(ttl=1800)
def load_cyprus_news(max_items: int = 8, lookback_hours: int = 48) -> tuple[pd.DataFrame, list[str]]:
    import feedparser
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    rows, ok_sources = [], []
    for source, url in CYPRUS_FEEDS:
        try:
            r = requests.get(url, headers=_UA, timeout=15)
            r.raise_for_status()
            feed = feedparser.parse(r.content)
            n = 0
            for e in feed.entries[:25]:
                ts = None
                if getattr(e, "published_parsed", None):
                    ts = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
                    if ts < cutoff:
                        continue
                title = (e.get("title") or "").strip()
                if "news.google.com" in url:
                    title = re.sub(r"\s+-\s+[^-]+$", "", title)  # drop " - Cyprus Mail"
                if len(title) < 15:
                    continue
                rows.append({"headline": title, "source": source,
                             "published": ts, "url": e.get("link") or ""})
                n += 1
            if n:
                ok_sources.append(source + (" (via Google News)" if "news.google.com" in url else ""))
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(), ok_sources
    df = pd.DataFrame(rows)
    df["key"] = df["headline"].str.lower().str.replace(r"[^a-z0-9 ]", "", regex=True).str[:70]
    df = (df.sort_values("published", ascending=False, na_position="last")
            .drop_duplicates("key").drop(columns="key").head(max_items))
    return df, ok_sources


def render_cyprus_news():
    st.markdown("**🇨🇾 Cyprus — business & economy**")
    df, used = load_cyprus_news()
    if df.empty:
        st.caption("No Cyprus headlines in the last 48h (or feeds unreachable).")
        return
    for _, r in df.iterrows():
        when = (pd.Timestamp(r["published"]).tz_convert("Europe/Zurich").strftime("%d %b %H:%M")
                if pd.notna(r["published"]) else "")
        st.markdown(
            f"<div style='font-size:12.5px;padding:3px 0;border-bottom:1px solid #F0F4F8;'>"
            f"<a href='{r['url']}' target='_blank' style='color:#0F2D52;text-decoration:none;'>"
            f"{r['headline']}</a> <span style='color:#94A3B8;font-size:10.5px;'>"
            f"· {r['source']} · {when}</span></div>",
            unsafe_allow_html=True,
        )
    st.caption("Sources: " + ", ".join(dict.fromkeys(used)))
