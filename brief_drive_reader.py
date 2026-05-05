"""
brief_drive_reader.py — drop this into the nxman-brief repo.

Two entry points for the Brief:

* `load_research_df()` / `load_research_df_cached()`
      → the SNIPER research.csv as a pandas DataFrame

* `load_research_pdfs_dict()` / `load_research_pdfs_dict_cached()`
      → {filename: pdf_bytes} for every PDF inside the SNIPER research tree
        on Drive (Research_Processed by default, optionally + Research_Inbox).
        Feed each one to the Brief's existing auto_detect_and_parse() and
        you get the Research Library populated for free — no manual upload.

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

If no config resolves, every loader returns an empty structure and logs once
— Brief should degrade gracefully, never crash.

Usage:
    from brief_drive_reader import load_research_df, load_research_pdfs_dict
    df    = load_research_df()               # columns: date, house, action, ...
    pdfs  = load_research_pdfs_dict()        # {'Morning Call_22 April 2026.pdf': b'...'}
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
THREE_WAY_LOG_NAME = "sniper_3way_log.csv"

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

    # Full drive scope (read + write) — the Brief now also UPLOADS new PDFs
    # from its sidebar into Research_Inbox. SA is Editor on the SNIPER tree.
    creds = Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/drive"],
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


def _find_3way_log_meta(drive, folder_id: str) -> tuple[str | None, str]:
    """Return (file_id, modified_time_iso) for the 3-way log on Drive.

    The modifiedTime tells the Brief when the file was last synced — i.e.
    when the SNIPER autopilot last ran and captured prices from yfinance.
    Surfacing that in the UI lets users tell at a glance whether they're
    looking at fresh post-open data or yesterday's close.
    """
    try:
        resp = drive.files().list(
            q=(
                f"'{folder_id}' in parents and trashed = false "
                f"and name = '{THREE_WAY_LOG_NAME}'"
            ),
            fields="files(id,name,modifiedTime)",
            pageSize=5,
        ).execute()
    except Exception as e:
        logger.error("Drive list failed for %s: %s", THREE_WAY_LOG_NAME, e)
        return None, ""
    files = resp.get("files", [])
    if not files:
        logger.info("No %s found in SNIPER Drive folder %s", THREE_WAY_LOG_NAME, folder_id)
        return None, ""
    return files[0]["id"], files[0].get("modifiedTime", "") or ""


# Backward-compat shim — older callers expect just a file ID.
def _find_3way_log_id(drive, folder_id: str) -> str | None:
    file_id, _ = _find_3way_log_meta(drive, folder_id)
    return file_id


def load_3way_log() -> tuple[pd.DataFrame, str]:
    """Return (DataFrame, modifiedTime ISO string) for the 3-way log.

    modifiedTime is Drive's record of when the file was last touched —
    i.e. when the SNIPER autopilot last ran. Empty string when the file
    isn't reachable. Brief UI shows this so the user can tell whether
    prices are fresh or stale.

    Empty DataFrame on any misconfiguration / failure.
    """
    drive = _drive_client()
    folder_id = _load_sniper_folder_id()
    if drive is None or not folder_id:
        return pd.DataFrame(), ""

    file_id, mtime = _find_3way_log_meta(drive, folder_id)
    if not file_id:
        return pd.DataFrame(), ""

    raw = _download_bytes(drive, file_id)
    if not raw:
        return pd.DataFrame(), mtime

    try:
        return pd.read_csv(io.BytesIO(raw)), mtime
    except Exception as e:
        logger.error("Could not parse %s: %s", THREE_WAY_LOG_NAME, e)
        return pd.DataFrame(), mtime


# --------------------------------------------------------------------------- PDF loader


FOLDER_MIME = "application/vnd.google-apps.folder"
PDF_MIME = "application/pdf"


def _find_subfolder(drive, parent_id: str, name: str) -> str | None:
    """Return the folder ID for a child folder by name, or None if not found."""
    # name may legitimately contain apostrophes; escape them for the Drive query.
    safe_name = name.replace("'", "\\'")
    try:
        resp = drive.files().list(
            q=(
                f"'{parent_id}' in parents and trashed = false "
                f"and mimeType = '{FOLDER_MIME}' "
                f"and name = '{safe_name}'"
            ),
            fields="files(id,name)",
            pageSize=5,
        ).execute()
    except Exception as e:
        logger.error("Drive subfolder lookup failed for %s: %s", name, e)
        return None
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def _list_pdfs_in_folder(drive, folder_id: str, page_size: int = 200) -> list[dict]:
    """Return [{id, name, modifiedTime}, ...] for every PDF in `folder_id`."""
    try:
        resp = drive.files().list(
            q=(
                f"'{folder_id}' in parents and trashed = false "
                f"and mimeType = '{PDF_MIME}'"
            ),
            fields="files(id,name,modifiedTime)",
            orderBy="modifiedTime desc",
            pageSize=page_size,
        ).execute()
    except Exception as e:
        logger.error("Drive PDF list failed in %s: %s", folder_id, e)
        return []
    return resp.get("files", [])


def load_research_pdfs_dict(
    include_inbox: bool = True,
    include_processed: bool = False,
    max_pdfs: int = 20,
) -> dict[str, bytes]:
    """Download PDFs from the SNIPER research tree on Drive.

    Default behaviour (2026-04 policy): INBOX ONLY. Processed is treated as
    an archive and does not appear in the Brief library. Pass
    include_processed=True explicitly if you need the archive too.

    Args:
        include_inbox: pull PDFs currently in Research_Inbox. Default True.
        include_processed: also pull PDFs from Research_Processed. Default
            False — the archive stays out of the library so the Brief only
            shows today's / this-cycle's broker drops.
        max_pdfs: cap the number of PDFs returned to keep Brief startup
            fast. Newest-first by modifiedTime.

    Returns:
        {filename: pdf_bytes}. Empty dict on any misconfiguration / failure.
    """
    drive = _drive_client()
    sniper_id = _load_sniper_folder_id()
    if drive is None or not sniper_id:
        logger.info("Drive PDF library not available — check SA + folder-ID config.")
        return {}

    inbox_id = _find_subfolder(drive, sniper_id, "Research_Inbox") if include_inbox else None
    processed_id = _find_subfolder(drive, sniper_id, "Research_Processed") if include_processed else None

    files: list[dict] = []
    if inbox_id:
        files.extend(_list_pdfs_in_folder(drive, inbox_id))
    if processed_id:
        files.extend(_list_pdfs_in_folder(drive, processed_id))
    if not files and not inbox_id and not processed_id:
        # No subfolders requested at all — try the SNIPER root as a last resort.
        files = _list_pdfs_in_folder(drive, sniper_id)

    if not files:
        return {}

    # Dedupe by filename.
    by_name: dict[str, dict] = {}
    for f in files:
        by_name[f["name"]] = f

    ordered = sorted(
        by_name.values(),
        key=lambda f: f.get("modifiedTime", ""),
        reverse=True,
    )[:max_pdfs]

    out: dict[str, bytes] = {}
    for f in ordered:
        raw = _download_bytes(drive, f["id"])
        if raw:
            out[f["name"]] = raw
    return out


def load_research_pdfs_with_meta(
    include_inbox: bool = True,
    include_processed: bool = False,
    max_pdfs: int = 20,
) -> dict[str, dict]:
    """Same as load_research_pdfs_dict but also returns Drive's modifiedTime.

    Added 2026-05-04 so the Brief can filter "today's drops" by the actual
    upload time (Drive modifiedTime) rather than a date parsed out of the
    filename — broker PDFs are dated by the publishing house's cover-page,
    not by when the user dropped them into the inbox, so filename-date
    filtering wrongly drops PDFs the user just uploaded.

    Returns:
        {filename: {"bytes": pdf_bytes, "modified_time": ISO_string}}
        Empty dict on misconfig / failure (same as the bytes-only sibling).
    """
    drive = _drive_client()
    sniper_id = _load_sniper_folder_id()
    if drive is None or not sniper_id:
        logger.info("Drive PDF library not available — check SA + folder-ID config.")
        return {}

    inbox_id = _find_subfolder(drive, sniper_id, "Research_Inbox") if include_inbox else None
    processed_id = _find_subfolder(drive, sniper_id, "Research_Processed") if include_processed else None

    files: list[dict] = []
    if inbox_id:
        files.extend(_list_pdfs_in_folder(drive, inbox_id))
    if processed_id:
        files.extend(_list_pdfs_in_folder(drive, processed_id))
    if not files and not inbox_id and not processed_id:
        files = _list_pdfs_in_folder(drive, sniper_id)
    if not files:
        return {}

    by_name: dict[str, dict] = {}
    for f in files:
        by_name[f["name"]] = f

    ordered = sorted(
        by_name.values(),
        key=lambda f: f.get("modifiedTime", ""),
        reverse=True,
    )[:max_pdfs]

    out: dict[str, dict] = {}
    for f in ordered:
        raw = _download_bytes(drive, f["id"])
        if raw:
            out[f["name"]] = {
                "bytes": raw,
                "modified_time": f.get("modifiedTime", ""),
            }
    return out


# --------------------------------------------------------------------------- writer


def upload_pdf_to_research_inbox(filename: str, pdf_bytes: bytes) -> str | None:
    """Upload a PDF into the SNIPER Research_Inbox folder on Drive.

    Used by the Brief sidebar: when a user drops a PDF into the uploader,
    the same bytes are pushed to Drive so the autopilot picks them up on
    the next sweep. Returns the new Drive file ID on success, or None on
    any failure (misconfig, network, 403, etc.) — Brief should degrade
    gracefully, never crash.

    Args:
        filename: display name for the new Drive file (e.g.
            "Morning Call_24 April 2026.pdf").
        pdf_bytes: the PDF content as bytes.

    Returns:
        The Drive file ID string, or None on any failure.
    """
    drive = _drive_client()
    sniper_id = _load_sniper_folder_id()
    if drive is None or not sniper_id:
        logger.info("Drive upload skipped — SA / folder-ID config missing.")
        return None

    inbox_id = _find_subfolder(drive, sniper_id, "Research_Inbox")
    if not inbox_id:
        logger.warning("Research_Inbox folder not found under SNIPER root; cannot upload.")
        return None

    try:
        from googleapiclient.http import MediaInMemoryUpload
    except ImportError as e:
        logger.error("googleapiclient missing: %s", e)
        return None

    # resumable=True handles big PDFs + transient rate-limit retries
    # (google-api-python-client retries 429/5xx on resumable uploads).
    media = MediaInMemoryUpload(pdf_bytes, mimetype="application/pdf", resumable=True)
    metadata = {"name": filename, "parents": [inbox_id]}

    # Simple retry loop with backoff for non-retryable-by-library failures
    import time as _time
    last_err: str = ""
    for attempt in range(3):
        try:
            created = drive.files().create(
                body=metadata,
                media_body=media,
                fields="id,name,createdTime",
            ).execute()
            return created.get("id")
        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)[:200]}"
            logger.warning("Drive upload attempt %d failed for %s: %s",
                           attempt + 1, filename, last_err)
            if attempt < 2:
                _time.sleep(2 ** attempt)  # 1s, 2s

    logger.error("Drive upload failed for %s after 3 attempts: %s", filename, last_err)
    # Stash the last error on the fn so the caller can surface it in the UI.
    upload_pdf_to_research_inbox.last_error = last_err  # type: ignore[attr-defined]
    return None


# --------------------------------------------------------------------------- streamlit caches


# Streamlit convenience wrappers — cache results for N seconds so the Brief UI
# doesn't hit Drive on every interaction. Import only if running under
# Streamlit; otherwise fall back to plain passthroughs.
try:  # pragma: no cover — optional
    import streamlit as _st

    @_st.cache_data(ttl=300)
    def load_research_df_cached() -> pd.DataFrame:
        return load_research_df()

    @_st.cache_data(ttl=300)
    def load_3way_log_cached() -> tuple[pd.DataFrame, str]:
        return load_3way_log()

    @_st.cache_data(ttl=300)
    def load_research_pdfs_dict_cached(
        include_inbox: bool = True,
        include_processed: bool = False,
        max_pdfs: int = 20,
    ) -> dict[str, bytes]:
        return load_research_pdfs_dict(
            include_inbox=include_inbox,
            include_processed=include_processed,
            max_pdfs=max_pdfs,
        )

    @_st.cache_data(ttl=300)
    def load_research_pdfs_with_meta_cached(
        include_inbox: bool = True,
        include_processed: bool = False,
        max_pdfs: int = 20,
    ) -> dict[str, dict]:
        return load_research_pdfs_with_meta(
            include_inbox=include_inbox,
            include_processed=include_processed,
            max_pdfs=max_pdfs,
        )

except Exception:
    def load_research_df_cached() -> pd.DataFrame:  # type: ignore[no-redef]
        return load_research_df()

    def load_3way_log_cached() -> tuple[pd.DataFrame, str]:  # type: ignore[no-redef]
        return load_3way_log()

    def load_research_pdfs_dict_cached(  # type: ignore[no-redef]
        include_inbox: bool = True,
        include_processed: bool = False,
        max_pdfs: int = 20,
    ) -> dict[str, bytes]:
        return load_research_pdfs_dict(
            include_inbox=include_inbox,
            include_processed=include_processed,
            max_pdfs=max_pdfs,
        )

    def load_research_pdfs_with_meta_cached(  # type: ignore[no-redef]
        include_inbox: bool = True,
        include_processed: bool = False,
        max_pdfs: int = 20,
    ) -> dict[str, dict]:
        return load_research_pdfs_with_meta(
            include_inbox=include_inbox,
            include_processed=include_processed,
            max_pdfs=max_pdfs,
        )


if __name__ == "__main__":
    # CLI: list what's available in Drive for the Brief.
    df = load_research_df()
    if df.empty:
        print("research.csv: (empty — no data or config missing)")
    else:
        print(f"research.csv: {len(df)} rows")

    pdfs = load_research_pdfs_dict()
    print(f"PDFs found: {len(pdfs)}")
    for name, blob in pdfs.items():
        print(f"  {name}  ({len(blob)} bytes)")