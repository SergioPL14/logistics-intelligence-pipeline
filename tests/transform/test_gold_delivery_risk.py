"""End-to-end tests for the gold delivery_risk join + scoring."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.transform.gold_delivery_risk import GOLD_COLUMNS, build_gold


def _orders() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": ["o1", "o2", "o3"],
            "customer_zip_code": ["01001", "02002", "99999"],  # 99999 has no weather
            "customer_city": ["a", "b", "c"],
            "customer_state": ["SP", "RJ", "ZZ"],
            "order_purchase_timestamp": pd.to_datetime(
                ["2026-01-10", "2026-01-20", "2026-01-25"]
            ),
            "order_weight_g": [500.0, 15_000.0, 1_000.0],
        }
    )


def _diesel() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "period": pd.to_datetime(["2026-01-01", "2026-01-15"]),
            "price_usd_per_gallon": [3.5, 4.5],
        }
    )


def _weather() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "zip_code": ["01001", "02002"],
            "condition": ["Clear", "Rain"],
            "wind_speed_ms": [1.0, 5.0],
            "precipitation_mm": [0.0, 2.0],
        }
    )


def test_build_gold_joins_scores_and_writes(tmp_path: Path) -> None:
    out_dir = tmp_path / "gold"
    df = build_gold(_orders(), _diesel(), _weather(), output_dir=out_dir, output_stem="risk")

    assert list(df.columns) == GOLD_COLUMNS
    # o3 dropped (no weather match)
    assert set(df["order_id"]) == {"o1", "o2"}
    # Diesel join correctness: o1 (Jan 10) -> 3.5; o2 (Jan 20) -> 4.5
    o1 = df[df["order_id"] == "o1"].iloc[0]
    o2 = df[df["order_id"] == "o2"].iloc[0]
    assert o1["diesel_price_usd_per_gallon"] == 3.5
    assert o2["diesel_price_usd_per_gallon"] == 4.5
    assert 0.0 <= o1["score"] <= 1.0
    assert o2["score"] > o1["score"]  # heavier + rain + higher diesel
    assert (out_dir / "risk.parquet").exists()
    assert (out_dir / "risk.csv").exists()


def test_build_gold_no_write(tmp_path: Path) -> None:
    df = build_gold(_orders(), _diesel(), _weather(), output_dir=None)
    assert len(df) == 2
