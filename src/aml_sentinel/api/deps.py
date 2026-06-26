"""FastAPI dependencies: DB session per request and a process-wide producer."""

from __future__ import annotations

from collections.abc import Iterator

from aml_sentinel.db.base import SessionLocal
from aml_sentinel.events import EventProducer

# One producer per process (thread-safe; cheap to share).
_producer: EventProducer | None = None


def get_db() -> Iterator:
    """Yield a DB session, closing it when the request finishes."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def get_producer() -> EventProducer:
    """Return the lazily-initialised shared Kafka producer."""
    global _producer
    if _producer is None:
        _producer = EventProducer()
    return _producer


def close_producer() -> None:
    global _producer
    if _producer is not None:
        _producer.close()
        _producer = None
