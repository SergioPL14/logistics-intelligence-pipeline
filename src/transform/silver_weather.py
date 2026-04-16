"""Bronze → silver transform for weather snapshots."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "weather_snapshots.parquet"
DEFAULT_SILVER_DIR = PROJECT_ROOT / "data" / "silver"
DEFAULT_SILVER_STEM = "weather_latest"


def transform_weather(
    bronze_path: Path = DEFAULT_BRONZE_PATH,
    output_dir: Path | None = DEFAULT_SILVER_DIR,
    output_stem: str = DEFAULT_SILVER_STEM,
) -> pd.DataFrame:
    """Reduce snapshots to the latest observation per zip code."""
    df = pd.read_parquet(bronze_path)
    df["observed_at"] = pd.to_datetime(df["observed_at"], utc=True)
    df = (
        df.sort_values("observed_at")
        .drop_duplicates(subset="zip_code", keep="last")
        .reset_index(drop=True)
    )
    logger.info("Silver weather (latest per zip): %d rows", len(df))

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_dir / f"{output_stem}.parquet", index=False)
        df.to_csv(output_dir / f"{output_stem}.csv", index=False)

    return df
