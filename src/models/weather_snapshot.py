"""Weather snapshot contract — bronze-layer output of the OpenWeather extractor."""
from __future__ import annotations

from datetime import datetime

from pydantic import Field, field_validator

from src.models._base import LogisticsModel
from src.models._validators import validate_zip_code


class WeatherSnapshot(LogisticsModel):
    """Point-in-time weather observation for a given zip code.

    Designed to join against ``Order.customer_zip_code`` in the silver layer.

    Attributes:
        zip_code: 5-character zero-padded zip prefix (matches Order schema).
        observed_at: UTC timestamp of the observation.
        temp_c: Air temperature in degrees Celsius.
        wind_speed_ms: Wind speed in meters per second (≥0).
        precipitation_mm: Precipitation accumulation in millimeters (≥0).
        condition: OpenWeather "main" descriptor (Rain, Snow, Clear, ...).
    """

    zip_code: str
    observed_at: datetime
    temp_c: float
    wind_speed_ms: float = Field(ge=0)
    precipitation_mm: float = Field(ge=0)
    condition: str = Field(min_length=1)

    @field_validator("zip_code", mode="before")
    @classmethod
    def _normalize_zip(cls, value: object) -> str:
        if value is None:
            raise ValueError("zip_code must not be None")
        return validate_zip_code(str(value))
