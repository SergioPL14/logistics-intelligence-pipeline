"""Unit tests for the engine factory."""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from sqlalchemy.engine import Engine

from src.load.db import build_engine


def test_build_engine_from_explicit_args() -> None:
    engine = build_engine(
        user="u", password="p", host="localhost", port="5432", database="testdb"
    )
    assert isinstance(engine, Engine)
    assert "u:***@localhost:5432/testdb" in str(engine.url)
    engine.dispose()


def test_build_engine_from_env_vars() -> None:
    env = {
        "POSTGRES_USER": "eu",
        "POSTGRES_PASSWORD": "ep",
        "POSTGRES_HOST": "envhost",
        "POSTGRES_PORT": "9999",
        "POSTGRES_DB": "envdb",
    }
    with patch.dict(os.environ, env, clear=False):
        engine = build_engine()
    assert "eu:***@envhost:9999/envdb" in str(engine.url)
    engine.dispose()


def test_build_engine_defaults_host_and_port() -> None:
    env = {
        "POSTGRES_USER": "u",
        "POSTGRES_PASSWORD": "p",
        "POSTGRES_DB": "db",
    }
    cleaned = {k: v for k, v in os.environ.items() if k not in ("POSTGRES_HOST", "POSTGRES_PORT")}
    with patch.dict(os.environ, {**cleaned, **env}, clear=True):
        engine = build_engine()
    assert "localhost:5432" in str(engine.url)
    engine.dispose()


def test_build_engine_missing_required_var_raises() -> None:
    with patch.dict(os.environ, {}, clear=True), pytest.raises(KeyError):
        build_engine()
