"""Unit tests for the weather bronze->silver transform."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.transform.silver_weather import transform_weather


def _write_bronze(tmp_path: Path) -> Path:
    df = pd.DataFrame(
        {
            "zip_code": ["01001", "01001", "02002"],
            "observed_at": [
                "2026-01-01T10:00:00Z",
                "2026-01-02T10:00:00Z",  # latest for 01001
                "2026-01-01T10:00:00Z",
            ],
            "temp_c": [20.0, 21.0, 22.0],
            "wind_speed_ms": [1.0, 2.0, 3.0],
            "precipitation_mm": [0.0, 0.5, 0.0],
            "condition": ["Clear", "Rain", "Clouds"],
        }
    )
    path = tmp_path / "weather.parquet"
    df.to_parquet(path, index=False)
    return path


def test_transform_weather_keeps_latest_per_zip(tmp_path: Path) -> None:
    bronze = _write_bronze(tmp_path)
    out_dir = tmp_path / "silver"
    df = transform_weather(bronze_path=bronze, output_dir=out_dir, output_stem="weather")

    assert len(df) == 2
    row_01001 = df[df["zip_code"] == "01001"].iloc[0]
    assert row_01001["condition"] == "Rain"
    assert (out_dir / "weather.parquet").exists()
    assert (out_dir / "weather.csv").exists()


def test_transform_weather_no_write(tmp_path: Path) -> None:
    bronze = _write_bronze(tmp_path)
    df = transform_weather(bronze_path=bronze, output_dir=None)
    assert len(df) == 2
