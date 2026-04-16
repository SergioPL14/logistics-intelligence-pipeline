"""Unit tests for src.extract.extract_orders."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.extract.extract_orders import OUTPUT_COLUMNS, extract_orders


@pytest.fixture
def olist_source_dir(tmp_path: Path) -> Path:
    """Build a minimal Olist-shaped CSV set covering join + edge cases.

    Scenarios encoded:
      * O1 — two line items → weights summed (100 + 250 = 350).
      * O2 — single item with NULL product weight → NaN preserved.
      * O3 — customer zip prefix shorter than 5 digits → zero-padded.
      * O4 — customer_id has no match in customers → row dropped.
      * O5 — null purchase timestamp → row dropped.
    """
    src = tmp_path / "olist"
    src.mkdir()

    pd.DataFrame(
        {
            "order_id": ["O1", "O2", "O3", "O4", "O5"],
            "customer_id": ["C1", "C2", "C3", "C_MISSING", "C5"],
            "order_status": ["delivered"] * 5,
            "order_purchase_timestamp": [
                "2018-01-01 10:00:00",
                "2018-01-02 11:00:00",
                "2018-01-03 12:00:00",
                "2018-01-04 13:00:00",
                "",
            ],
            "order_approved_at": [""] * 5,
            "order_delivered_carrier_date": [""] * 5,
            "order_delivered_customer_date": [""] * 5,
            "order_estimated_delivery_date": [""] * 5,
        }
    ).to_csv(src / "olist_orders_dataset.csv", index=False)

    pd.DataFrame(
        {
            "customer_id": ["C1", "C2", "C3", "C5"],
            "customer_unique_id": ["U1", "U2", "U3", "U5"],
            "customer_zip_code_prefix": [12345, 99999, 1310, 54321],
            "customer_city": ["sao paulo", "rio de janeiro", "campinas", "santos"],
            "customer_state": ["SP", "RJ", "SP", "SP"],
        }
    ).to_csv(src / "olist_customers_dataset.csv", index=False)

    pd.DataFrame(
        {
            "order_id": ["O1", "O1", "O2", "O3", "O4", "O5"],
            "order_item_id": [1, 2, 1, 1, 1, 1],
            "product_id": ["P1", "P2", "P3", "P1", "P1", "P1"],
            "seller_id": ["S"] * 6,
            "shipping_limit_date": [""] * 6,
            "price": [10.0] * 6,
            "freight_value": [1.0] * 6,
        }
    ).to_csv(src / "olist_order_items_dataset.csv", index=False)

    pd.DataFrame(
        {
            "product_id": ["P1", "P2", "P3"],
            "product_category_name": ["a", "b", "c"],
            "product_name_lenght": [1, 1, 1],
            "product_description_lenght": [1, 1, 1],
            "product_photos_qty": [1, 1, 1],
            "product_weight_g": [100.0, 250.0, None],
            "product_length_cm": [1, 1, 1],
            "product_height_cm": [1, 1, 1],
            "product_width_cm": [1, 1, 1],
        }
    ).to_csv(src / "olist_products_dataset.csv", index=False)

    return src


def test_extract_returns_expected_columns(olist_source_dir: Path) -> None:
    df = extract_orders(source_dir=olist_source_dir, output_dir=None)
    assert list(df.columns) == OUTPUT_COLUMNS


def test_extract_drops_rows_with_missing_keys(olist_source_dir: Path) -> None:
    df = extract_orders(source_dir=olist_source_dir, output_dir=None)
    # O4 dropped (no matching customer → null zip), O5 dropped (null timestamp).
    assert set(df["order_id"]) == {"O1", "O2", "O3"}


def test_extract_sums_weight_across_items(olist_source_dir: Path) -> None:
    df = extract_orders(source_dir=olist_source_dir, output_dir=None)
    weight = df.loc[df["order_id"] == "O1", "order_weight_g"].iloc[0]
    assert weight == 350.0


def test_extract_preserves_null_weight(olist_source_dir: Path) -> None:
    df = extract_orders(source_dir=olist_source_dir, output_dir=None)
    weight = df.loc[df["order_id"] == "O2", "order_weight_g"].iloc[0]
    assert pd.isna(weight)


def test_extract_pads_zip_code(olist_source_dir: Path) -> None:
    df = extract_orders(source_dir=olist_source_dir, output_dir=None)
    zip_o3 = df.loc[df["order_id"] == "O3", "customer_zip_code"].iloc[0]
    assert zip_o3 == "01310"


def test_extract_writes_parquet_and_csv(olist_source_dir: Path, tmp_path: Path) -> None:
    out = tmp_path / "out"
    df = extract_orders(source_dir=olist_source_dir, output_dir=out, output_stem="orders")

    parquet_path = out / "orders.parquet"
    csv_path = out / "orders.csv"
    assert parquet_path.exists()
    assert csv_path.exists()

    from_parquet = pd.read_parquet(parquet_path)
    from_csv = pd.read_csv(csv_path, dtype={"customer_zip_code": str})

    assert len(from_parquet) == len(df) == len(from_csv)
    assert set(from_parquet["order_id"]) == set(df["order_id"]) == set(from_csv["order_id"])


def test_extract_with_output_dir_none_writes_nothing(
    olist_source_dir: Path, tmp_path: Path
) -> None:
    out = tmp_path / "should_not_exist"
    df = extract_orders(source_dir=olist_source_dir, output_dir=None)
    assert not out.exists()
    assert not df.empty


def test_extract_raises_on_missing_source(tmp_path: Path) -> None:
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    with pytest.raises(FileNotFoundError):
        extract_orders(source_dir=empty_dir, output_dir=None)
