"""Identifier helpers.

``trace_id`` is a UUIDv7 (doc 01 §6) — time-ordered, so it sorts roughly by
creation. ``uuid.uuid7()`` only exists in the stdlib from Python 3.14, but the
service images and CI run Python 3.12, so we implement RFC 9562 §5.7 directly to
stay portable across every supported interpreter.
"""

from __future__ import annotations

import secrets
import time
import uuid


def uuid7() -> uuid.UUID:
    """Generate a UUIDv7 (48-bit ms timestamp + version/variant + random)."""
    unix_ts_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    value = (
        (unix_ts_ms << 80)
        | (0x7 << 76)  # version 7
        | (rand_a << 64)
        | (0b10 << 62)  # variant (RFC 4122)
        | rand_b
    )
    return uuid.UUID(int=value)


def new_trace_id() -> str:
    """A fresh UUIDv7 ``trace_id`` as a string."""
    return str(uuid7())
