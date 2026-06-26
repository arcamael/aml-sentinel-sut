"""World-Check (Sanctions) mock — `mocks/world_check` (doc 01 §7)."""

from __future__ import annotations

import os

from mocks.provider_mock import create_app

app = create_app(
    provider_id="world_check",
    list_type="sanctions",
    watchlist_path=os.environ.get("AML_WATCHLIST_FILE", "data/watchlists/sanctions.jsonl"),
    list_version=os.environ.get("AML_LIST_VERSION", "v1"),
)
