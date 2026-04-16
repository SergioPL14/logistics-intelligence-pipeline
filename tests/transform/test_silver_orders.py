"""Unit tests for the orders bronze->silver transform."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.transform.silver_orders import OUTPUT_COLUMNS, transform_orders


@pytest.fixture
def bronze_orders(tmp_path: Path) -> Path:
    df = pd.DataFrame(
        {
            "order_id": ["b", "a", "c"],
            "customer_zip_code": ["01001", "02002", "03003"],
            "customer_city": ["sao paulo", "rio", "belo"],
            "customer_state": ["SP", "RJ", "MG"],
            "order_purchase_timestamp": [
                "2026-01-02 10:00:00",
                "2026-01-01 09:00:00",
                "2026-01-03 11:00:00",
            ],
            "order_weight_g": [100.0, None, 300.0],
            "extra_col": ["x", "y", "z"],
        }
    )
    path = tmp_path / "orders.parquet"
    df.to_parquet(path, index=False)
    return path


def test_transform_orders_drops_missing_weight_sorts_and_writes(
    bronze_orders: Path, tmp_path: Path
) -> None:
    out_dir = tmp_path / "silver"
    df = transform_orders(bronze_path=bronze_orders, output_dir=out_dir, output_stem="orders")

    assert list(df.columns) == OUTPUT_COLUMNS
    assert list(df["order_id"]) == ["b", "c"]
    assert pd.api.types.is_datetime64_any_dtype(df["order_purchase_timestamp"])
    assert (out_dir / "orders.parquet").exists()
    assert (out_dir / "orders.csv").exists()


def test_transform_orders_skips_write_when_output_dir_none(bronze_orders: Path) -> None:
    df = transform_orders(bronze_path=bronze_orders, output_dir=None)
    assert len(df) == 2
