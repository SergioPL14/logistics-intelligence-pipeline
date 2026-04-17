"""Logistics Intelligence Pipeline — Airflow DAG.

Orchestrates the full ETL: extract (bronze) → transform (silver/gold) → load (Postgres).
"""
from __future__ import annotations

import logging
from datetime import timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "logistics",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

EXTRACT_TASK_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="logistics_pipeline",
    default_args=DEFAULT_ARGS,
    description="Extract → Transform → Load delivery risk scores",
    schedule=None,
    catchup=False,
    tags=["logistics", "etl"],
)
def logistics_pipeline():
    """Build bronze → silver → gold delivery risk scores and load to Postgres."""

    @task(**EXTRACT_TASK_ARGS)
    def extract_orders_task() -> str:
        """Extract Olist orders into bronze parquet + csv."""
        from src.extract.extract_orders import extract_orders

        df = extract_orders()
        logger.info("Extracted %d orders to bronze", len(df))
        return "ok"

    @task(**EXTRACT_TASK_ARGS)
    def extract_diesel_task() -> str:
        """Extract EIA diesel prices into bronze parquet + csv."""
        from src.extract.extract_diesel import extract_diesel

        df = extract_diesel()
        logger.info("Extracted %d diesel price rows to bronze", len(df))
        return "ok"

    @task(**EXTRACT_TASK_ARGS)
    def extract_weather_task() -> str:
        """Extract OpenWeather snapshots for top-N order locations into bronze."""
        from src.extract.extract_weather import extract_weather, top_n_order_locations

        locations = top_n_order_locations()
        df = extract_weather(locations)
        logger.info("Extracted %d weather snapshots to bronze", len(df))
        return "ok"

    @task
    def transform_silver_orders_task() -> str:
        """Bronze → silver orders: drop missing weights, cast timestamps."""
        from src.transform.silver_orders import transform_orders

        df = transform_orders()
        logger.info("Silver orders: %d rows", len(df))
        return "ok"

    @task
    def transform_silver_diesel_task() -> str:
        """Bronze → silver diesel: cast period to datetime, sort ascending."""
        from src.transform.silver_diesel import transform_diesel

        df = transform_diesel()
        logger.info("Silver diesel: %d rows", len(df))
        return "ok"

    @task
    def transform_silver_weather_task() -> str:
        """Bronze → silver weather: latest observation per zip code."""
        from src.transform.silver_weather import transform_weather

        df = transform_weather()
        logger.info("Silver weather: %d rows", len(df))
        return "ok"

    @task
    def build_gold_task() -> str:
        """Join silver inputs and compute delivery risk scores."""
        from src.transform.gold_delivery_risk import build_gold
        from src.transform.silver_diesel import transform_diesel
        from src.transform.silver_orders import transform_orders
        from src.transform.silver_weather import transform_weather

        orders = transform_orders(output_dir=None)
        diesel = transform_diesel(output_dir=None)
        weather = transform_weather(output_dir=None)
        gold = build_gold(orders, diesel, weather)
        logger.info("Gold delivery_risk: %d rows", len(gold))
        return "ok"

    @task
    def load_gold_task() -> str:
        """Load gold delivery_risk to Postgres."""
        import pandas as pd

        from src.load.db import build_engine
        from src.load.loader import load_dataframe

        gold_path = __import__("pathlib").Path(__file__).resolve().parents[1] / "data" / "gold"
        df = pd.read_parquet(gold_path / "delivery_risk.parquet")
        engine = build_engine()
        written = load_dataframe(df, "delivery_risk", engine, schema="gold")
        engine.dispose()
        logger.info("Loaded %d rows to gold.delivery_risk", written)
        return "ok"

    # ── Dependencies ────────────────────────────────────────────────────────

    # Phase 1: Extract (parallel)
    orders_done = extract_orders_task()
    diesel_done = extract_diesel_task()
    weather_done = extract_weather_task()

    # Phase 2: Transform silver (parallel, but each waits for its extract)
    silver_orders_done = transform_silver_orders_task()
    silver_diesel_done = transform_silver_diesel_task()
    silver_weather_done = transform_silver_weather_task()

    orders_done >> silver_orders_done
    diesel_done >> silver_diesel_done
    weather_done >> silver_weather_done

    # Phase 3: Gold (waits for all silver)
    gold_done = build_gold_task()
    [silver_orders_done, silver_diesel_done, silver_weather_done] >> gold_done

    # Phase 4: Load (waits for gold)
    gold_done >> load_gold_task()


logistics_pipeline()
