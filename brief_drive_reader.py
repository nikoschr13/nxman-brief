"""
brief_drive_reader.py — drop this into the nxman-brief repo.

Provides `load_research_df()` which returns the SNIPER research.csv as a
pandas DataFrame by reading it from the Google Drive SNIPER folder (owned
by Nikos, SA = Editor).

Config sources, in priority order:

1. Streamlit secrets (when running on Streamlit Cloud):
     st.secrets["gcp_service_account"] = <full SA JSON as dict>
     st.secrets["sniper_drive"]["sniper_folder_id"] = "..."
2. Env vars (handy for CI / containers):
     SNIPER_DRIVE_SA_JSON      — full JSON as a string
     SNIPER_DRIVE_FOLDER_ID    — SNIPER folder ID
3. Local disk (dev on Nikos's Mac):
     ~/.config/sniper/service_account.json
     ~/SNIPER/drive_config.json

If no config resolves, `load_research_df()` returns an empty DataFrame and
logs once — Brief should degrade gracefully, not crash.

Usage:
    from brief_drive_reader import load_research_df
    df = load_research_df()       # columns: date, house, action, name_raw, ticker, ...
"""

from __future__ import annotations

import io
import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Canonical research.csv filename in Drive. Kept as a constant so the
# autopilot can upload by name and Brief can search by the same name.
RESEARCH_CSV_NAME = "research.csv"

LOCAL_SA_PATH = Path.home() / ".config" / "sniper" / "service_account.json"
LOCAL_CFG_PATH = Path.home() / "SNIPER" / "drive_config.json"


# --------------------------------------------------------------------------- config


def _load_sa_info() -> dict[str, Any] | None:
    """Return the SA JSON dict, or None if no source has one."""
    # 1. Streamlit secrets
    try:
        import streamlit as st  # noqa: F401 — optional import
        if "gcp_service_account" in st.secrets:
            # st.secrets values are dict-like but not plain dicts; normalise.
            return dict(st.secrets["gcp_service_account"])
    except Exception:
        pass

    # 2. Env var
    raw = os.environ.get("SNIPER_DRIVE_SA_JSON")
    if raw:
        try:
            return json.loads(raw)
        except Exception as e:
            logger.warning("SNIPER_DRIVE_SA_JSON is not valid JSON: %s", e)

    # 3. Local disk
    if LOCAL_SA_PATH.exists():
        try:
            return json.loads(LOCAL_SA_PATH.read_text())
        except Exception as e:
            logger.warning("Could not read %s: %s", LOCAL_SA_PATH, e)

    return None


def _load_sniper_folder_id() -> str | None:
    """Return the SNIPER folder ID, or None if no source has one."""
    # 1. Streamlit secrets
    try:
        import streamlit as st  # noqa: F401
        sec = st.secrets.get("sniper_drive") if hasattr(st.secrets, "get") else None
        if sec:
            fid = sec.get("sniper_folder_id") if hasattr(sec, "get") else sec["sniper_folder_id"]
            if fid:
                return fid
    except Exception:
        pass

    # 2. Env var
    fid = os.environ.get("SNIPER_DRIVE_FOLDER_ID")
    if fid:
        return fid

    # 3. Local disk
    if LOCAL_CFG_PATH.exists():
        try:
            cfg = json.loads(LOCAL_CFG_PATH.read_text())
            return (cfg.get("folder_ids") or {}).get("sniper")
        except Exception as e:
            logger.warning("Could not read %s: %s", LOCAL_CFG_PATH, e)

    return None


# --------------------------------------------------------------------------- drive


@lru_cache(maxsize=1)
def _drive_client():
    """Build + cache the Drive client for the life of the process."""
    sa_info = _load_sa_info()
    if sa_info is None:
        return None

    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
    except ImportError as e:
        logger.error("google-api-python-client not installed: %s", e)
        return None

    creds = Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _find_research_csv_id(drive, folder_id: str) -> str | None:
    """Look up the research.csv file ID inside the SNIPER folder."""
    try:
        resp = drive.files().list(
            q=(
                f"'{folder_id}' in parents and trashed = false "
                f"and name = '{RESEARCH_CSV_NAME}'"
            ),
            fields="files(id,name,modifiedTime)",
            pageSize=5,
        ).execute()
    except Exception as e:
        logger.error("Drive list failed: %s", e)
        return None

    files = resp.get("files", [])
    if not files:
        logger.warning("No %s found in SNIPER Drive folder %s", RESEARCH_CSV_NAME, folder_id)
        return None
    return files[0]["id"]


def _download_bytes(drive, file_id: str) -> bytes | None:
    try:
        from googleapiclient.http import MediaIoBaseDownload
    except ImportError as e:
        logger.error("googleapiclient missing: %s", e)
        return None

    buf = io.BytesIO()
    try:
        req = drive.files().get_media(fileId=file_id)
        dl = MediaIoBaseDownload(buf, req)
        done = False
        while not done:
            _, done = dl.next_chunk()
    except Exception as e:
        logger.error("Drive download failed: %s", e)
        return None
    return buf.getvalue()


# --------------------------------------------------------------------------- public


def load_research_df() -> pd.DataFrame:
    """Return the SNIPER research.csv as a DataFrame, or an empty frame on any
    misconfiguration / failure. Always safe to call from Brief UI code."""
    drive = _drive_client()
    folder_id = _load_sniper_folder_id()
    if drive is None or not folder_id:
        logger.info("Drive research CSV not available — check SA + folder-ID config.")
        return pd.DataFrame()

    file_id = _find_research_csv_id(drive, folder_id)
    if not file_id:
        return pd.DataFrame()

    raw = _download_bytes(drive, file_id)
    if not raw:
        return pd.DataFrame()

    try:
        return pd.read_csv(io.BytesIO(raw))
    except Exception as e:
        logger.error("Could not parse research.csv: %s", e)
        return pd.DataFrame()


# Streamlit convenience wrapper — caches the DataFrame for N seconds so the
# Brief UI doesn't hit Drive on every interaction. Import only if running
# under Streamlit; otherwise fall back to a plain passthrough.
try:  # pragma: no cover — optional
    import streamlit as _st

    @_st.cache_data(ttl=300)
    def load_research_df_cached() -> pd.DataFrame:
        return load_research_df()

except Exception:
    def load_research_df_cached() -> pd.DataFrame:  # type: ignore[no-redef]
        return load_research_df()


if __name__ == "__main__":
    # CLI: dump the current Drive research.csv to stdout as TSV (for eyeballing).
    df = load_research_df()
    if df.empty:
        print("(empty — no data or config missing)")
    else:
        print(df.to_csv(sep="\t", index=False))
