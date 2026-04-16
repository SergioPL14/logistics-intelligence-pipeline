"""Unit tests for the WeatherSnapshot contract."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.models import WeatherSnapshot


def _now() -> datetime:
    return datetime(2026, 4, 16, 12, 0, tzinfo=UTC)


def test_weather_snapshot_happy_path() -> None:
    snap = WeatherSnapshot(
        zip_code="01310",
        observed_at=_now(),
        temp_c=22.5,
        wind_speed_ms=3.2,
        precipitation_mm=0.0,
        condition="Clear",
    )
    assert snap.zip_code == "01310"
    assert snap.condition == "Clear"


def test_weather_snapshot_normalizes_zip() -> None:
    snap = WeatherSnapshot(
        zip_code="1310",
        observed_at=_now(),
        temp_c=10.0,
        wind_speed_ms=0.0,
        precipitation_mm=0.0,
        condition="Rain",
    )
    assert snap.zip_code == "01310"


@pytest.mark.parametrize(
    ("field", "value"),
    [("wind_speed_ms", -1.0), ("precipitation_mm", -0.5)],
)
def test_weather_snapshot_rejects_negative_metrics(field: str, value: float) -> None:
    payload: dict[str, object] = {
        "zip_code": "12345",
        "observed_at": _now(),
        "temp_c": 0.0,
        "wind_speed_ms": 0.0,
        "precipitation_mm": 0.0,
        "condition": "Clear",
    }
    payload[field] = value
    with pytest.raises(ValidationError):
        WeatherSnapshot(**payload)  # type: ignore[arg-type]


def test_weather_snapshot_rejects_invalid_zip() -> None:
    with pytest.raises(ValidationError):
        WeatherSnapshot(
            zip_code="bad",
            observed_at=_now(),
            temp_c=0.0,
            wind_speed_ms=0.0,
            precipitation_mm=0.0,
            condition="Clear",
        )


def test_weather_snapshot_rejects_none_zip() -> None:
    with pytest.raises(ValidationError):
        WeatherSnapshot(
            zip_code=None,  # type: ignore[arg-type]
            observed_at=_now(),
            temp_c=0.0,
            wind_speed_ms=0.0,
            precipitation_mm=0.0,
            condition="Clear",
        )
