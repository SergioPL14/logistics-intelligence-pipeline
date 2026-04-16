"""Bronze → silver transform for orders."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "orders.parquet"
DEFAULT_SILVER_DIR = PROJECT_ROOT / "data" / "silver"
DEFAULT_SILVER_STEM = "orders"

OUTPUT_COLUMNS = [
    "order_id",
    "customer_zip_code",
    "customer_city",
    "customer_state",
    "order_purchase_timestamp",
    "order_weight_g",
]


def transform_orders(
    bronze_path: Path = DEFAULT_BRONZE_PATH,
    output_dir: Path | None = DEFAULT_SILVER_DIR,
    output_stem: str = DEFAULT_SILVER_STEM,
) -> pd.DataFrame:
    """Drop orders missing weight, ensure timestamp dtype, and persist as parquet+csv."""
    df = pd.read_parquet(bronze_path)
    before = len(df)
    df = df.dropna(subset=["order_weight_g"]).copy()
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"], utc=False)
    df = df[OUTPUT_COLUMNS].sort_values("order_purchase_timestamp").reset_index(drop=True)
    logger.info("Silver orders: %d -> %d rows (dropped %d for missing weight)",
                before, len(df), before - len(df))

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_dir / f"{output_stem}.parquet", index=False)
        df.to_csv(output_dir / f"{output_stem}.csv", index=False)

    return df
