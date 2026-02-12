from __future__ import annotations

import json
from typing import Any
from urllib import request, error


def get_json(url: str, headers: dict[str, str] | None = None, timeout: int = 30) -> dict[str, Any]:
    req = request.Request(url, headers=headers or {}, method="GET")
    try:
        with request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        raise RuntimeError(f"GET {url} failed with status {exc.code}") from exc


def post_json(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str] | None = None,
    timeout: int = 30,
) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req_headers = {"Content-Type": "application/json", **(headers or {})}
    req = request.Request(url, data=body, headers=req_headers, method="POST")
    try:
        with request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        raise RuntimeError(f"POST {url} failed with status {exc.code}") from exc

