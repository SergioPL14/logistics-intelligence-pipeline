"""Unit tests for src.extract.extract_weather (OpenWeather API mocked)."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests
from pydantic import ValidationError

from src.extract.extract_weather import (
    GEOCODE_URL,
    OUTPUT_COLUMNS,
    extract_weather,
    top_n_order_locations,
)


def _resp(payload: object, status: int = 200) -> MagicMock:
    r = MagicMock(spec=requests.Response)
    r.status_code = status
    r.json.return_value = payload
    if status >= 400:
        r.raise_for_status.side_effect = requests.HTTPError(f"{status}")
    else:
        r.raise_for_status.return_value = None
    return r


GEO_OK = [{"name": "Sao Paulo", "lat": -23.5, "lon": -46.6, "country": "BR"}]
GEO_EMPTY: list[dict] = []
WEATHER_OK = {
    "main": {"temp": 22.5},
    "wind": {"speed": 3.2},
    "rain": {"1h": 0.4},
    "weather": [{"main": "Rain"}],
}


def _route(url: str, params: dict | None = None, **_: object) -> MagicMock:
    if url == GEOCODE_URL:
        q = (params or {}).get("q", "")
        if q.startswith("Unknownville"):
            return _resp(GEO_EMPTY)
        return _resp(GEO_OK)
    return _resp(WEATHER_OK)


@pytest.fixture
def ow_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENWEATHER_API_KEY", "test-key")


def test_extract_weather_happy_path(ow_key: None) -> None:
    locations = [("01310", "sao paulo", "SP"), ("12345", "campinas", "SP")]
    with patch("src.extract.extract_weather.requests.get", side_effect=_route):
        df = extract_weather(locations, output_dir=None)

    assert list(df.columns) == OUTPUT_COLUMNS
    assert len(df) == 2
    assert df["temp_c"].iloc[0] == 22.5
    assert df["wind_speed_ms"].iloc[0] == 3.2
    assert df["precipitation_mm"].iloc[0] == 0.4
    assert df["condition"].iloc[0] == "Rain"


def test_extract_weather_skips_unresolvable_city(ow_key: None) -> None:
    locations = [
        ("01310", "sao paulo", "SP"),
        ("99999", "Unknownville", "ZZ"),
        ("12345", "campinas", "SP"),
    ]
    with patch("src.extract.extract_weather.requests.get", side_effect=_route):
        df = extract_weather(locations, output_dir=None)
    assert len(df) == 2  # Unknownville dropped


def test_extract_weather_caches_geocode_per_city(ow_key: None) -> None:
    # Two zips in the same (city, state) → only one geocode call.
    locations = [("01310", "sao paulo", "SP"), ("01311", "sao paulo", "SP")]
    with patch("src.extract.extract_weather.requests.get", side_effect=_route) as mock_get:
        extract_weather(locations, output_dir=None)
    geo_calls = [c for c in mock_get.call_args_list if c.args[0] == GEOCODE_URL]
    assert len(geo_calls) == 1


def test_extract_weather_writes_files(ow_key: None, tmp_path: Path) -> None:
    out = tmp_path / "bronze"
    with patch("src.extract.extract_weather.requests.get", side_effect=_route):
        extract_weather([("01310", "sao paulo", "SP")], output_dir=out, output_stem="weather")
    assert (out / "weather.parquet").exists()
    assert (out / "weather.csv").exists()


def test_extract_weather_missing_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENWEATHER_API_KEY", raising=False)
    monkeypatch.setattr("src.extract.extract_weather.load_dotenv", lambda: None)
    with pytest.raises(RuntimeError, match="OPENWEATHER_API_KEY"):
        extract_weather([("01310", "sao paulo", "SP")], output_dir=None)


def test_extract_weather_http_error_bubbles(ow_key: None) -> None:
    def boom(url: str, params: dict | None = None, **_: object) -> MagicMock:
        if url == GEOCODE_URL:
            return _resp(GEO_OK)
        return _resp({}, status=500)

    with patch("src.extract.extract_weather.requests.get", side_effect=boom), \
         pytest.raises(requests.HTTPError):
        extract_weather([("01310", "sao paulo", "SP")], output_dir=None)


def test_extract_weather_geocode_http_error_bubbles(ow_key: None) -> None:
    def boom(url: str, **_: object) -> MagicMock:
        return _resp({}, status=500)

    with patch("src.extract.extract_weather.requests.get", side_effect=boom), \
         pytest.raises(requests.HTTPError):
        extract_weather([("01310", "sao paulo", "SP")], output_dir=None)


def test_extract_weather_validation_failure(ow_key: None) -> None:
    bad_weather = {
        "main": {"temp": 22.5},
        "wind": {"speed": -1.0},
        "weather": [{"main": "Clear"}],
    }

    def bad(url: str, **_: object) -> MagicMock:
        if url == GEOCODE_URL:
            return _resp(GEO_OK)
        return _resp(bad_weather)

    with patch("src.extract.extract_weather.requests.get", side_effect=bad), \
         pytest.raises(ValidationError):
        extract_weather([("01310", "sao paulo", "SP")], output_dir=None)


def test_extract_weather_handles_missing_optional_fields(ow_key: None) -> None:
    minimal = {"main": {"temp": 18.0}, "wind": {}, "weather": []}

    def route(url: str, **_: object) -> MagicMock:
        if url == GEOCODE_URL:
            return _resp(GEO_OK)
        return _resp(minimal)

    with patch("src.extract.extract_weather.requests.get", side_effect=route):
        df = extract_weather([("01310", "sao paulo", "SP")], output_dir=None)

    assert df["wind_speed_ms"].iloc[0] == 0.0
    assert df["precipitation_mm"].iloc[0] == 0.0
    assert df["condition"].iloc[0] == "Unknown"


def test_top_n_order_locations(tmp_path: Path) -> None:
    orders = pd.DataFrame(
        {
            "order_id": list("ABCDEFG"),
            "customer_zip_code": ["01310", "01310", "01310", "12345", "12345", "99999", "55555"],
            "customer_city": ["sp", "sp", "sp", "campinas", "campinas", "rio", "santos"],
            "customer_state": ["SP", "SP", "SP", "SP", "SP", "RJ", "SP"],
            "order_purchase_timestamp": pd.Timestamp("2018-01-01"),
            "order_weight_g": 100.0,
        }
    )
    path = tmp_path / "orders.parquet"
    orders.to_parquet(path, index=False)

    top3 = top_n_order_locations(n=3, orders_path=path)
    assert len(top3) == 3
    assert top3[0] == ("01310", "sp", "SP")
    assert top3[1] == ("12345", "campinas", "SP")


def test_top_n_order_locations_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        top_n_order_locations(orders_path=tmp_path / "nope.parquet")
