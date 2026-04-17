"""Unit tests for the DataFrame loader using SQLite in-memory."""
from __future__ import annotations

from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine, inspect, text

from src.load.loader import _ensure_schema, load_dataframe


def _engine():
    return create_engine("sqlite:///:memory:")


def test_load_dataframe_creates_table_and_writes_rows() -> None:
    engine = _engine()
    df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
    written = load_dataframe(df, "test_table", engine)
    assert written == 2
    assert "test_table" in inspect(engine).get_table_names()
    engine.dispose()


def test_load_dataframe_replace_overwrites() -> None:
    engine = _engine()
    df1 = pd.DataFrame({"x": [1, 2, 3]})
    df2 = pd.DataFrame({"x": [10]})
    load_dataframe(df1, "t", engine, if_exists="replace")
    load_dataframe(df2, "t", engine, if_exists="replace")
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM t")).scalar()
    assert rows == 1
    engine.dispose()


def test_load_dataframe_append_adds_rows() -> None:
    engine = _engine()
    df = pd.DataFrame({"x": [1]})
    load_dataframe(df, "t", engine, if_exists="replace")
    load_dataframe(df, "t", engine, if_exists="append")
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM t")).scalar()
    assert rows == 2
    engine.dispose()


def test_load_dataframe_returns_row_count() -> None:
    engine = _engine()
    df = pd.DataFrame({"a": range(5)})
    assert load_dataframe(df, "five", engine) == 5
    engine.dispose()


def test_load_dataframe_with_schema_calls_ensure_schema() -> None:
    engine = _engine()
    df = pd.DataFrame({"x": [1]})
    with patch("src.load.loader._ensure_schema") as mock_ensure:
        written = load_dataframe(df, "t", engine, schema="main")
        mock_ensure.assert_called_once_with(engine, "main")
    assert written == 1
    engine.dispose()


def test_ensure_schema_skips_existing() -> None:
    engine = _engine()
    _ensure_schema(engine, "main")
    engine.dispose()


def test_ensure_schema_creates_missing() -> None:
    engine = _engine()
    with patch("src.load.loader.inspect") as mock_insp:
        mock_insp.return_value.get_schema_names.return_value = []
        with patch("src.load.loader.text") as mock_text:
            _ensure_schema(engine, "new_schema")
            mock_text.assert_called_once()
    engine.dispose()
