from __future__ import annotations

from typing import Any

from realbricks.connectors.http import get_json


class FreddieMacClient:
    """Starter client for Freddie Mac developer APIs.

    Endpoint paths vary by product and entitlement. Keep endpoint names in config.
    """

    def __init__(self, bearer_token: str, base_url: str = "https://api.freddiemac.com"):
        if not bearer_token:
            raise ValueError("Freddie Mac bearer token is required")
        self.bearer_token = bearer_token
        self.base_url = base_url.rstrip("/")

    def get_rates_snapshot(self, endpoint_path: str) -> dict[str, Any]:
        url = f"{self.base_url}/{endpoint_path.lstrip('/')}"
        return get_json(url, headers={"Authorization": f"Bearer {self.bearer_token}"})

