"""ComplyAdvantage (Adverse Media) mock — `mocks/comply_advantage` (doc 01 §7)."""

from __future__ import annotations

import os

from mocks.provider_mock import create_app

app = create_app(
    provider_id="comply_advantage",
    list_type="adverse_media",
    watchlist_path=os.environ.get("AML_WATCHLIST_FILE", "data/watchlists/adverse_media.jsonl"),
    list_version=os.environ.get("AML_LIST_VERSION", "v1"),
)
