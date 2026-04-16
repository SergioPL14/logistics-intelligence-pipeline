"""Silver → gold join + risk score computation."""
from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.transform.risk_score import compute_risk

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_GOLD_DIR = PROJECT_ROOT / "data" / "gold"
DEFAULT_GOLD_STEM = "delivery_risk"

GOLD_COLUMNS = [
    "order_id",
    "score",
    "risk_band",
    "diesel_price_usd_per_gallon",
    "weather_condition",
    "computed_at",
]


def build_gold(
    orders: pd.DataFrame,
    diesel: pd.DataFrame,
    weather_latest: pd.DataFrame,
    output_dir: Path | None = DEFAULT_GOLD_DIR,
    output_stem: str = DEFAULT_GOLD_STEM,
) -> pd.DataFrame:
    """Join silver inputs and compute the delivery risk score per order.

    Joins:
      orders x diesel via merge_asof on order_purchase_timestamp <= period
      result x weather_latest via inner join on customer_zip_code = zip_code
    Orders without a matching weather snapshot are dropped (we only have
    weather for the top-N most-frequent zips).
    """
    orders = orders.sort_values("order_purchase_timestamp").reset_index(drop=True)
    diesel = diesel.sort_values("period").reset_index(drop=True)

    joined = pd.merge_asof(
        orders,
        diesel[["period", "price_usd_per_gallon"]],
        left_on="order_purchase_timestamp",
        right_on="period",
        direction="backward",
    ).dropna(subset=["price_usd_per_gallon"])

    joined = joined.merge(
        weather_latest[["zip_code", "condition", "wind_speed_ms", "precipitation_mm"]],
        left_on="customer_zip_code",
        right_on="zip_code",
        how="inner",
    )

    now = datetime.now(UTC)
    rows = [
        compute_risk(
            order_id=r.order_id,
            order_weight_g=r.order_weight_g,
            diesel_price_usd_per_gallon=r.price_usd_per_gallon,
            weather_condition=r.condition,
            wind_speed_ms=r.wind_speed_ms,
            precipitation_mm=r.precipitation_mm,
            computed_at=now,
        ).model_dump()
        for r in joined.itertuples(index=False)
    ]
    df = pd.DataFrame(rows, columns=GOLD_COLUMNS)
    logger.info("Gold delivery_risk: %d rows", len(df))

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_dir / f"{output_stem}.parquet", index=False)
        df.to_csv(output_dir / f"{output_stem}.csv", index=False)

    return df
