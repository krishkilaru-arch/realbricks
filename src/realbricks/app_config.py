from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AppConfig:
    kpi_table: str
    ranking_table: str
    state_table: str
    page_size_default: int
    page_size_max: int
    allowed_users: list[str]
    allowed_email_domains: list[str]
    enable_actions: bool
    app_title: str


def _require_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"Invalid app config: `{key}` must be a non-empty string.")
    return value.strip()


def _require_int(payload: dict[str, Any], key: str, min_value: int) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or value < min_value:
        raise RuntimeError(f"Invalid app config: `{key}` must be an integer >= {min_value}.")
    return value


def _optional_str_list(payload: dict[str, Any], key: str) -> list[str]:
    value = payload.get(key, [])
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(x, str) for x in value):
        raise RuntimeError(f"Invalid app config: `{key}` must be a list of strings.")
    return [x.strip().lower() for x in value if x.strip()]


def load_app_config(path: str | Path) -> AppConfig:
    p = Path(path)
    if not p.exists():
        raise RuntimeError(
            f"Missing app config at `{p}`. Create it from `conf/app_config.template.json`."
        )

    payload = json.loads(p.read_text(encoding="utf-8"))
    page_size_default = _require_int(payload, "page_size_default", 1)
    page_size_max = _require_int(payload, "page_size_max", 1)
    if page_size_default > page_size_max:
        raise RuntimeError("Invalid app config: `page_size_default` cannot exceed `page_size_max`.")

    enable_actions = payload.get("enable_actions", True)
    if not isinstance(enable_actions, bool):
        raise RuntimeError("Invalid app config: `enable_actions` must be a boolean.")

    return AppConfig(
        kpi_table=_require_str(payload, "kpi_table"),
        ranking_table=_require_str(payload, "ranking_table"),
        state_table=_require_str(payload, "state_table"),
        page_size_default=page_size_default,
        page_size_max=page_size_max,
        allowed_users=_optional_str_list(payload, "allowed_users"),
        allowed_email_domains=_optional_str_list(payload, "allowed_email_domains"),
        enable_actions=enable_actions,
        app_title=_require_str(payload, "app_title"),
    )

