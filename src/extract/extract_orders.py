"""Extract Olist e-commerce orders into a unified bronze dataset.

Joins:
  orders        -> customers      (customer_zip_code_prefix)
  order_items   -> products       (product_weight_g, summed per order)

Output columns: order_id, customer_zip_code, order_purchase_timestamp, order_weight_g
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_DIR = PROJECT_ROOT / "resources" / "olist_datasets"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "bronze"
DEFAULT_OUTPUT_STEM = "orders"

OUTPUT_COLUMNS = [
    "order_id",
    "customer_zip_code",
    "order_purchase_timestamp",
    "order_weight_g",
]


def _read_csv(source_dir: Path, name: str, usecols: list[str]) -> pd.DataFrame:
    path = source_dir / name
    if not path.exists():
        raise FileNotFoundError(f"Missing Olist file: {path}")
    return pd.read_csv(path, usecols=usecols)


def extract_orders(
    source_dir: Path = DEFAULT_SOURCE_DIR,
    output_dir: Path | None = DEFAULT_OUTPUT_DIR,
    output_stem: str = DEFAULT_OUTPUT_STEM,
) -> pd.DataFrame:
    """Build the unified orders dataframe and optionally persist as parquet + csv."""
    logger.info("Reading Olist sources from %s", source_dir)

    orders = _read_csv(
        source_dir,
        "olist_orders_dataset.csv",
        ["order_id", "customer_id", "order_purchase_timestamp"],
    )
    customers = _read_csv(
        source_dir,
        "olist_customers_dataset.csv",
        ["customer_id", "customer_zip_code_prefix"],
    )
    items = _read_csv(
        source_dir,
        "olist_order_items_dataset.csv",
        ["order_id", "product_id"],
    )
    products = _read_csv(
        source_dir,
        "olist_products_dataset.csv",
        ["product_id", "product_weight_g"],
    )

    orders["order_purchase_timestamp"] = pd.to_datetime(
        orders["order_purchase_timestamp"], errors="coerce"
    )

    weight_per_order = (
        items.merge(products, on="product_id", how="left")
        .groupby("order_id", as_index=False)["product_weight_g"]
        .sum(min_count=1)
        .rename(columns={"product_weight_g": "order_weight_g"})
    )

    df = (
        orders.merge(customers, on="customer_id", how="left")
        .merge(weight_per_order, on="order_id", how="left")
        .rename(columns={"customer_zip_code_prefix": "customer_zip_code"})
    )

    df["customer_zip_code"] = (
        df["customer_zip_code"]
        .astype("Int64")
        .astype(str)
        .str.zfill(5)
        .where(df["customer_zip_code"].notna(), None)
    )

    result = df[OUTPUT_COLUMNS].copy()

    before = len(result)
    result = result.dropna(subset=["order_id", "customer_zip_code", "order_purchase_timestamp"])
    logger.info("Dropped %d rows with missing keys", before - len(result))

    logger.info("Extracted %d order rows", len(result))

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = output_dir / f"{output_stem}.parquet"
        csv_path = output_dir / f"{output_stem}.csv"
        result.to_parquet(parquet_path, index=False)
        result.to_csv(csv_path, index=False)
        logger.info("Wrote %s and %s", parquet_path, csv_path)

    return result


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    df = extract_orders()
    logger.info("Sample:\n%s", df.head(10).to_string(index=False))
    logger.info("Total rows: %d", len(df))
