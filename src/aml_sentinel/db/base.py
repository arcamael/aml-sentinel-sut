"""SQLAlchemy engine, session factory, and declarative base.

The declarative ``Base`` is the single ``MetaData`` source that Alembic
autogenerate and the test harness both bind to.
"""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from aml_sentinel.config import settings


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


# ``future=True`` is the default in SQLAlchemy 2.0; kept explicit for clarity.
engine = create_engine(settings.database_url, pool_pre_ping=True)

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
