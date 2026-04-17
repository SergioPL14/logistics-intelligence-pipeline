"""Generic DataFrame-to-SQL loader for the logistics data warehouse."""
from __future__ import annotations

import logging
from typing import Literal

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def _ensure_schema(engine: Engine, schema: str) -> None:
    """Create the schema if it does not exist (Postgres only)."""
    if schema and schema not in inspect(engine).get_schema_names():
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))


def load_dataframe(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    *,
    schema: str | None = None,
    if_exists: Literal["replace", "append"] = "replace",
) -> int:
    """Write a DataFrame to a database table.

    Returns the number of rows written.
    """
    if schema:
        _ensure_schema(engine, schema)

    rows = df.to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
    )
    written = rows if rows is not None else len(df)
    logger.info(
        "Loaded %d rows into %s.%s (if_exists=%s)",
        written,
        schema or "public",
        table_name,
        if_exists,
    )
    return written
