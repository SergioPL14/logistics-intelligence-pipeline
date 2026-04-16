"""Extract current weather snapshots from OpenWeather for a set of locations.

Uses the free "current weather" endpoint, not historical. Each pipeline run
captures a point-in-time observation per location; downstream transforms join
on zip + latest-snapshot. See README "Design Notes" for the rationale.

Geocoding uses OpenWeather's `/geo/1.0/direct` (city + state + country) instead
of `/geo/1.0/zip`, because Olist stores only the 5-digit prefix of an 8-digit
Brazilian CEP, which the zip endpoint cannot resolve.
"""
from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from src.models import WeatherSnapshot

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ORDERS_PATH = PROJECT_ROOT / "data" / "bronze" / "orders.parquet"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "bronze"
DEFAULT_OUTPUT_STEM = "weather_snapshots"
DEFAULT_COUNTRY = "BR"
DEFAULT_TOP_N_LOCATIONS = 20

GEOCODE_URL = "https://api.openweathermap.org/geo/1.0/direct"
WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
REQUEST_TIMEOUT_SECONDS = 30

OUTPUT_COLUMNS = [
    "zip_code",
    "observed_at",
    "temp_c",
    "wind_speed_ms",
    "precipitation_mm",
    "condition",
]

Location = tuple[str, str, str]  # (zip_code, city, state)


def _get_api_key() -> str:
    load_dotenv()
    key = os.environ.get("OPENWEATHER_API_KEY")
    if not key:
        raise RuntimeError("OPENWEATHER_API_KEY environment variable is not set")
    return key


def top_n_order_locations(
    n: int = DEFAULT_TOP_N_LOCATIONS,
    orders_path: Path = DEFAULT_ORDERS_PATH,
) -> list[Location]:
    """Return the N most frequent (zip, city, state) triples from the orders bronze."""
    if not orders_path.exists():
        raise FileNotFoundError(f"Orders bronze file not found: {orders_path}")
    df = pd.read_parquet(
        orders_path,
        columns=["customer_zip_code", "customer_city", "customer_state"],
    )
    counts = (
        df.groupby(["customer_zip_code", "customer_city", "customer_state"])
        .size()
        .sort_values(ascending=False)
        .head(n)
    )
    locations: list[Location] = [
        (zip_code, city, state) for (zip_code, city, state), _ in counts.items()
    ]
    logger.info("Selected top %d locations from %s", len(locations), orders_path)
    return locations


def _geocode_city(
    api_key: str,
    city: str,
    state: str,
    country: str,
) -> tuple[float, float] | None:
    """Resolve city/state/country to (lat, lon). Returns None if not found."""
    response = requests.get(
        GEOCODE_URL,
        params={
            "q": f"{city},{state},{country}",
            "limit": 1,
            "appid": api_key,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    body = response.json()
    if not body:
        logger.warning("Geocoding returned no results for %s,%s,%s", city, state, country)
        return None
    return float(body[0]["lat"]), float(body[0]["lon"])


def _fetch_weather(api_key: str, lat: float, lon: float) -> dict[str, object]:
    response = requests.get(
        WEATHER_URL,
        params={"lat": lat, "lon": lon, "units": "metric", "appid": api_key},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def _to_snapshot(zip_code: str, payload: dict[str, object]) -> WeatherSnapshot:
    main = payload.get("main", {}) or {}
    wind = payload.get("wind", {}) or {}
    rain = payload.get("rain", {}) or {}
    snow = payload.get("snow", {}) or {}
    weather_list = payload.get("weather", []) or []
    condition = weather_list[0].get("main", "Unknown") if weather_list else "Unknown"

    precipitation = float(rain.get("1h", 0.0)) + float(snow.get("1h", 0.0))  # type: ignore[union-attr]

    return WeatherSnapshot(
        zip_code=zip_code,
        observed_at=datetime.now(UTC),
        temp_c=float(main["temp"]),  # type: ignore[index]
        wind_speed_ms=float(wind.get("speed", 0.0)),  # type: ignore[union-attr]
        precipitation_mm=precipitation,
        condition=str(condition),
    )


def extract_weather(
    locations: Iterable[Location],
    country: str = DEFAULT_COUNTRY,
    output_dir: Path | None = DEFAULT_OUTPUT_DIR,
    output_stem: str = DEFAULT_OUTPUT_STEM,
) -> pd.DataFrame:
    """Fetch current weather snapshots for each location and persist as parquet + csv.

    Geocoding is cached per ``(city, state)`` tuple within a single run, so multiple
    zip codes in the same city only trigger one geocode call.

    Args:
        locations: Iterable of ``(zip_code, city, state)`` triples.
        country: ISO country code passed to OpenWeather geocoding (default ``BR``).
        output_dir: Directory to write bronze artifacts. Pass ``None`` to skip.
        output_stem: Filename stem (no extension).

    Returns:
        DataFrame with columns matching the WeatherSnapshot contract.

    Raises:
        RuntimeError: If ``OPENWEATHER_API_KEY`` is unset.
        requests.HTTPError: If a non-2xx response is returned by either endpoint.
        pydantic.ValidationError: If a built snapshot violates the WeatherSnapshot contract.
    """
    api_key = _get_api_key()
    snapshots: list[WeatherSnapshot] = []
    geocode_cache: dict[tuple[str, str], tuple[float, float] | None] = {}

    for zip_code, city, state in locations:
        cache_key = (city, state)
        if cache_key not in geocode_cache:
            geocode_cache[cache_key] = _geocode_city(api_key, city, state, country)
        coords = geocode_cache[cache_key]
        if coords is None:
            continue
        lat, lon = coords
        payload = _fetch_weather(api_key, lat, lon)
        snapshots.append(_to_snapshot(zip_code, payload))

    df = pd.DataFrame([s.model_dump() for s in snapshots], columns=OUTPUT_COLUMNS)
    logger.info("Built weather snapshot dataframe with %d rows", len(df))

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
    locations = top_n_order_locations()
    df = extract_weather(locations)
    logger.info("Sample:\n%s", df.head(10).to_string(index=False))
    logger.info("Total rows: %d", len(df))
