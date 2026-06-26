"""A small per-provider circuit breaker (Phase 4).

CLOSED → (``threshold`` consecutive failures) → OPEN → (after ``cooldown``)
→ HALF_OPEN → (success) → CLOSED, or (failure) → OPEN again. While OPEN the
gateway skips the provider entirely and returns a degraded result immediately,
shedding load from a sick dependency.

The clock is injectable so tests (and the reconciliation ``Clock`` in Phase 7)
can drive state transitions deterministically without sleeping.
"""

from __future__ import annotations

import time
from collections.abc import Callable

CLOSED = "closed"
OPEN = "open"
HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        *,
        threshold: int = 3,
        cooldown_s: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._threshold = threshold
        self._cooldown = cooldown_s
        self._clock = clock
        self._failures = 0
        self._opened_at: float | None = None
        self._state = CLOSED

    @property
    def state(self) -> str:
        # Lazily transition OPEN → HALF_OPEN once the cooldown has elapsed.
        if self._state == OPEN and self._opened_at is not None:
            if self._clock() - self._opened_at >= self._cooldown:
                self._state = HALF_OPEN
        return self._state

    def allow(self) -> bool:
        """Whether a call may proceed right now."""
        return self.state in (CLOSED, HALF_OPEN)

    def record_success(self) -> None:
        self._failures = 0
        self._opened_at = None
        self._state = CLOSED

    def record_failure(self) -> None:
        # A failure in HALF_OPEN re-opens immediately; otherwise count up.
        if self.state == HALF_OPEN:
            self._trip()
            return
        self._failures += 1
        if self._failures >= self._threshold:
            self._trip()

    def _trip(self) -> None:
        self._state = OPEN
        self._opened_at = self._clock()
