"""Order contract — bronze-layer output of the Olist orders extractor."""
from __future__ import annotations

from datetime import datetime

from pydantic import Field, field_validator

from src.models._base import LogisticsModel
from src.models._validators import validate_zip_code


class Order(LogisticsModel):
    """A single e-commerce order, joined with customer zip and total weight.

    Mirrors the schema produced by ``src.extract.extract_orders.extract_orders``.

    Attributes:
        order_id: Olist order UUID (non-empty).
        customer_zip_code: 5-character zero-padded US-style zip prefix.
        order_purchase_timestamp: When the customer placed the order.
        order_weight_g: Sum of product weights across line items, in grams.
            Nullable because some Olist products lack a weight value.
    """

    order_id: str = Field(min_length=1)
    customer_zip_code: str
    order_purchase_timestamp: datetime
    order_weight_g: float | None = Field(default=None, ge=0)

    @field_validator("customer_zip_code", mode="before")
    @classmethod
    def _normalize_zip(cls, value: object) -> str:
        if value is None:
            raise ValueError("customer_zip_code must not be None")
        return validate_zip_code(str(value))
