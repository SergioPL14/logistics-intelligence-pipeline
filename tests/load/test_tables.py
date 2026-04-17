"""Smoke test for table definitions — verifies import and basic structure."""
from __future__ import annotations

from src.load.tables import (
    bronze_diesel,
    bronze_meta,
    bronze_orders,
    bronze_weather,
    gold_delivery_risk,
    gold_meta,
    silver_diesel,
    silver_meta,
    silver_orders,
    silver_weather,
)


def test_bronze_schema() -> None:
    assert bronze_meta.schema == "bronze"
    assert bronze_orders.name == "orders"
    assert bronze_diesel.name == "diesel_prices"
    assert bronze_weather.name == "weather_snapshots"


def test_silver_schema() -> None:
    assert silver_meta.schema == "silver"
    assert silver_orders.name == "orders"
    assert silver_diesel.name == "diesel_prices"
    assert silver_weather.name == "weather_latest"


def test_gold_schema() -> None:
    assert gold_meta.schema == "gold"
    assert gold_delivery_risk.name == "delivery_risk"


def test_gold_columns() -> None:
    col_names = [c.name for c in gold_delivery_risk.columns]
    assert "order_id" in col_names
    assert "score" in col_names
    assert "risk_band" in col_names
