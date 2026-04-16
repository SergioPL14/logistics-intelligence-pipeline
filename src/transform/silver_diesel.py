"""Bronze → silver transform for diesel prices."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "diesel_prices.parquet"
DEFAULT_SILVER_DIR = PROJECT_ROOT / "data" / "silver"
DEFAULT_SILVER_STEM = "diesel_prices"


def transform_diesel(
    bronze_path: Path = DEFAULT_BRONZE_PATH,
    output_dir: Path | None = DEFAULT_SILVER_DIR,
    output_stem: str = DEFAULT_SILVER_STEM,
) -> pd.DataFrame:
    """Cast period to datetime and sort ascending — required for merge_asof in gold."""
    df = pd.read_parquet(bronze_path)
    df["period"] = pd.to_datetime(df["period"])
    df = df.sort_values("period").reset_index(drop=True)
    logger.info("Silver diesel: %d rows", len(df))

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_dir / f"{output_stem}.parquet", index=False)
        df.to_csv(output_dir / f"{output_stem}.csv", index=False)

    return df
