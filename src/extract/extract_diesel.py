"""Extract weekly U.S. retail diesel prices from the EIA v2 API.

Series: ``EMD_EPD2D_PTE_NUS_DPG`` — weekly U.S. No.2 diesel retail price,
all sellers, USD per gallon.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from src.models import DieselPrice

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "bronze"
DEFAULT_OUTPUT_STEM = "diesel_prices"

EIA_BASE_URL = "https://api.eia.gov/v2/petroleum/pri/gnd/data/"
DEFAULT_SERIES_ID = "EMD_EPD2D_PTE_NUS_DPG"
DEFAULT_REGION = "NUS"
REQUEST_TIMEOUT_SECONDS = 30
MAX_ROWS = 5000

OUTPUT_COLUMNS = ["period", "region", "price_usd_per_gallon", "series_id"]


def _get_api_key() -> str:
    load_dotenv()
    key = os.environ.get("EIA_API_KEY")
    if not key:
        raise RuntimeError("EIA_API_KEY environment variable is not set")
    return key


def _fetch_raw(
    api_key: str,
    series_id: str,
    start_date: str | None,
) -> list[dict[str, object]]:
    """Call the EIA v2 endpoint and return the ``response.data`` list."""
    params: list[tuple[str, str | int]] = [
        ("frequency", "weekly"),
        ("data[0]", "value"),
        ("facets[series][]", series_id),
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "desc"),
        ("length", MAX_ROWS),
        ("api_key", api_key),
    ]
    if start_date is not None:
        params.append(("start", start_date))

    logger.info("Requesting EIA series %s (start=%s)", series_id, start_date)
    response = requests.get(EIA_BASE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    body = response.json()
    data = body.get("response", {}).get("data", [])
    logger.info("EIA returned %d rows", len(data))
    return data


def _to_models(raw_rows: list[dict[str, object]], series_id: str) -> list[DieselPrice]:
    """Validate raw rows into DieselPrice instances (per-row contract enforcement)."""
    models: list[DieselPrice] = []
    for row in raw_rows:
        models.append(
            DieselPrice(
                period=str(row["period"]),  # type: ignore[arg-type]
                region=DEFAULT_REGION,
                price_usd_per_gallon=float(row["value"]),  # type: ignore[arg-type]
                series_id=series_id,
            )
        )
    return models


def extract_diesel(
    start_date: str | None = None,
    series_id: str = DEFAULT_SERIES_ID,
    output_dir: Path | None = DEFAULT_OUTPUT_DIR,
    output_stem: str = DEFAULT_OUTPUT_STEM,
) -> pd.DataFrame:
    """Fetch weekly diesel prices from EIA and persist as parquet + csv.

    Args:
        start_date: Optional ``YYYY-MM-DD`` lower bound on the period.
        series_id: EIA series identifier; defaults to weekly U.S. retail diesel.
        output_dir: Directory to write bronze artifacts to. Pass ``None`` to skip.
        output_stem: Filename stem for the output files (no extension).

    Returns:
        DataFrame with columns ``period``, ``region``, ``price_usd_per_gallon``,
        ``series_id``, sorted by ``period`` ascending.

    Raises:
        RuntimeError: If ``EIA_API_KEY`` is unset.
        requests.HTTPError: If the EIA API returns a non-2xx response.
        pydantic.ValidationError: If any row violates the DieselPrice contract.
    """
    api_key = _get_api_key()
    raw = _fetch_raw(api_key, series_id, start_date)
    models = _to_models(raw, series_id)

    df = pd.DataFrame([m.model_dump() for m in models], columns=OUTPUT_COLUMNS)
    df = df.sort_values("period").reset_index(drop=True)
    logger.info("Built diesel price dataframe with %d rows", len(df))

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = output_dir / f"{output_stem}.parquet"
        csv_path = output_dir / f"{output_stem}.csv"
        df.to_parquet(parquet_path, index=False)
        df.to_csv(csv_path, index=False)
        logger.info("Wrote %s and %s", parquet_path, csv_path)

    return df


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    df = extract_diesel()
    logger.info("Sample:\n%s", df.tail(10).to_string(index=False))
    logger.info("Total rows: %d", len(df))
