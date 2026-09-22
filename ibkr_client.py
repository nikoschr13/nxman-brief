"""Small read-only client for the Interactive Brokers Web API.

Only portfolio and account endpoints are exposed here.  There are deliberately
no order, allocation, transfer, or trading methods in this module.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests


class IBKRAPIError(RuntimeError):
    """A user-safe IBKR connection error."""


@dataclass(frozen=True)
class IBKRConfig:
    base_url: str = ""
    access_token: str = ""
    session_cookie: str = ""
    account_id: str = ""
    verify_ssl: bool = True
    timeout_seconds: int = 20

    @property
    def configured(self) -> bool:
        return bool(self.base_url.strip())


class IBKRClient:
    """Read-only wrapper around IBKR Web API portfolio endpoints."""

    def __init__(self, config: IBKRConfig):
        self.config = config
        self.base_url = config.base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if config.access_token:
            self.session.headers["Authorization"] = f"Bearer {config.access_token}"
        if config.session_cookie:
            self.session.headers["Cookie"] = config.session_cookie

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        if not self.config.configured:
            raise IBKRAPIError("IBKR is not configured.")
        try:
            response = self.session.get(
                f"{self.base_url}/{path.lstrip('/')}",
                params=params,
                timeout=self.config.timeout_seconds,
                verify=self.config.verify_ssl,
            )
        except requests.Timeout as exc:
            raise IBKRAPIError("IBKR timed out. The authenticated session may be unavailable.") from exc
        except requests.RequestException as exc:
            raise IBKRAPIError("Could not reach the configured IBKR endpoint.") from exc

        if response.status_code in {401, 403}:
            raise IBKRAPIError("IBKR authentication expired or lacks portfolio permission.")
        if response.status_code == 404:
            raise IBKRAPIError("The configured IBKR endpoint or account was not found.")
        if not response.ok:
            raise IBKRAPIError(f"IBKR returned HTTP {response.status_code}.")
        try:
            return response.json()
        except ValueError as exc:
            raise IBKRAPIError("IBKR returned an invalid response.") from exc

    def accounts(self) -> list[dict[str, Any]]:
        payload = self._get("portfolio/accounts")
        return payload if isinstance(payload, list) else []

    def positions(self, account_id: str, max_pages: int = 20) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for page in range(max_pages):
            payload = self._get(f"portfolio/{account_id}/positions/{page}")
            page_rows = payload if isinstance(payload, list) else []
            if not page_rows:
                break
            rows.extend(page_rows)
        return rows

    def summary(self, account_id: str) -> dict[str, Any]:
        payload = self._get(f"portfolio/{account_id}/summary")
        return payload if isinstance(payload, dict) else {}

    def ledger(self, account_id: str) -> dict[str, Any]:
        payload = self._get(f"portfolio/{account_id}/ledger")
        return payload if isinstance(payload, dict) else {}

    def market_snapshot(self, conids: list[int], fields: str = "31,55,82,84,86") -> list[dict[str, Any]]:
        """Read entitled quotes; requires an active IBKR brokerage session."""
        if not conids:
            return []
        params = {"conids": ",".join(str(c) for c in conids), "fields": fields}
        payload = self._get(
            "iserver/marketdata/snapshot",
            params=params,
        )
        # IBKR treats the first call for new contracts as a pre-flight that
        # starts the streams and can return only conids.  A follow-up request
        # retrieves any fields that are already available.
        if isinstance(payload, list) and payload and not any("31" in row for row in payload if isinstance(row, dict)):
            payload = self._get(
                "iserver/marketdata/snapshot",
                params={"conids": params["conids"]},
            )
        return payload if isinstance(payload, list) else []

    def portfolio(self, account_id: str = "") -> dict[str, Any]:
        accounts = self.accounts()
        available_ids = [
            str(row.get("accountId") or row.get("id") or "").strip()
            for row in accounts
            if isinstance(row, dict)
        ]
        chosen = (account_id or self.config.account_id or "").strip()
        if not chosen and available_ids:
            chosen = available_ids[0]
        if not chosen:
            raise IBKRAPIError("IBKR returned no accessible portfolio account.")
        if available_ids and chosen not in available_ids:
            raise IBKRAPIError("The configured IBKR account is not available to this session.")

        return {
            "account_id": chosen,
            "accounts": accounts,
            "positions": self.positions(chosen),
            "summary": self.summary(chosen),
            "ledger": self.ledger(chosen),
            "as_of": datetime.now(timezone.utc).isoformat(),
        }


def mask_account_id(account_id: str) -> str:
    text = str(account_id or "")
    if len(text) <= 4:
        return "••••"
    return f"••••{text[-4:]}"
