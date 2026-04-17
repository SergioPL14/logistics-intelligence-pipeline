"""Database engine factory for the logistics data warehouse."""
from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def build_engine(
    *,
    user: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: str | None = None,
    database: str | None = None,
) -> Engine:
    """Build a SQLAlchemy engine from explicit args or environment variables."""
    user = user or os.environ["POSTGRES_USER"]
    password = password or os.environ["POSTGRES_PASSWORD"]
    host = host or os.environ.get("POSTGRES_HOST", "localhost")
    port = port or os.environ.get("POSTGRES_PORT", "5432")
    database = database or os.environ["POSTGRES_DB"]

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url, pool_pre_ping=True)
