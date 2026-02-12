from __future__ import annotations

from typing import Any

from realbricks.connectors.http import get_json


class RepliersClient:
    def __init__(self, api_key: str, base_url: str = "https://api.repliers.io"):
        if not api_key:
            raise ValueError("Repliers API key is required")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def search_listings(self, city: str | None = None, postal_code: str | None = None, limit: int = 25) -> dict[str, Any]:
        params = []
        if city:
            params.append(f"city={city}")
        if postal_code:
            params.append(f"postalCode={postal_code}")
        params.append(f"resultsPerPage={limit}")
        query = "&".join(params)
        url = f"{self.base_url}/listings?{query}"
        return get_json(url, headers={"REPLIERS-API-KEY": self.api_key})

