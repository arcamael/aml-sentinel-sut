"""Dow Jones (PEP) mock — `mocks/dow_jones` (doc 01 §7)."""

from __future__ import annotations

import os

from mocks.provider_mock import create_app

app = create_app(
    provider_id="dow_jones",
    list_type="pep",
    watchlist_path=os.environ.get("AML_WATCHLIST_FILE", "data/watchlists/pep.jsonl"),
    list_version=os.environ.get("AML_LIST_VERSION", "v1"),
)
