"""SQLAlchemy table definitions for the logistics data warehouse."""
from __future__ import annotations

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    MetaData,
    String,
    Table,
)

bronze_meta = MetaData(schema="bronze")
silver_meta = MetaData(schema="silver")
gold_meta = MetaData(schema="gold")

# ── Bronze ──────────────────────────────────────────────────────────────────

bronze_orders = Table(
    "orders",
    bronze_meta,
    Column("order_id", String, primary_key=True),
    Column("customer_zip_code", String),
    Column("customer_city", String),
    Column("customer_state", String(2)),
    Column("order_purchase_timestamp", DateTime),
    Column("order_weight_g", Float, nullable=True),
)

bronze_diesel = Table(
    "diesel_prices",
    bronze_meta,
    Column("period", DateTime),
    Column("region", String),
    Column("price_usd_per_gallon", Float),
    Column("series_id", String),
)

bronze_weather = Table(
    "weather_snapshots",
    bronze_meta,
    Column("zip_code", String),
    Column("observed_at", DateTime(timezone=True)),
    Column("temp_c", Float),
    Column("wind_speed_ms", Float),
    Column("precipitation_mm", Float),
    Column("condition", String),
)

# ── Silver ──────────────────────────────────────────────────────────────────

silver_orders = Table(
    "orders",
    silver_meta,
    Column("order_id", String, primary_key=True),
    Column("customer_zip_code", String),
    Column("customer_city", String),
    Column("customer_state", String(2)),
    Column("order_purchase_timestamp", DateTime),
    Column("order_weight_g", Float),
)

silver_diesel = Table(
    "diesel_prices",
    silver_meta,
    Column("period", DateTime),
    Column("price_usd_per_gallon", Float),
)

silver_weather = Table(
    "weather_latest",
    silver_meta,
    Column("zip_code", String, primary_key=True),
    Column("observed_at", DateTime(timezone=True)),
    Column("temp_c", Float),
    Column("wind_speed_ms", Float),
    Column("precipitation_mm", Float),
    Column("condition", String),
)

# ── Gold ────────────────────────────────────────────────────────────────────

gold_delivery_risk = Table(
    "delivery_risk",
    gold_meta,
    Column("order_id", String, primary_key=True),
    Column("score", Float),
    Column("risk_band", String),
    Column("diesel_price_usd_per_gallon", Float),
    Column("weather_condition", String),
    Column("computed_at", DateTime(timezone=True)),
)
