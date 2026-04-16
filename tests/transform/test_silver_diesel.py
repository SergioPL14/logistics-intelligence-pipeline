"""Unit tests for the diesel bronze->silver transform."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.transform.silver_diesel import transform_diesel


def _write_bronze(tmp_path: Path) -> Path:
    df = pd.DataFrame(
        {
            "period": ["2026-01-15", "2026-01-01", "2026-01-08"],
            "region": ["US", "US", "US"],
            "price_usd_per_gallon": [3.5, 3.3, 3.4],
            "series_id": ["EMD"] * 3,
        }
    )
    path = tmp_path / "diesel.parquet"
    df.to_parquet(path, index=False)
    return path


def test_transform_diesel_sorts_and_writes(tmp_path: Path) -> None:
    bronze = _write_bronze(tmp_path)
    out_dir = tmp_path / "silver"
    df = transform_diesel(bronze_path=bronze, output_dir=out_dir, output_stem="diesel")

    assert pd.api.types.is_datetime64_any_dtype(df["period"])
    assert list(df["period"]) == sorted(df["period"])
    assert (out_dir / "diesel.parquet").exists()
    assert (out_dir / "diesel.csv").exists()


def test_transform_diesel_no_write(tmp_path: Path) -> None:
    bronze = _write_bronze(tmp_path)
    df = transform_diesel(bronze_path=bronze, output_dir=None)
    assert len(df) == 3
