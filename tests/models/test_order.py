"""Unit tests for the Order contract."""
from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from src.models import Order


def _ts() -> datetime:
    return datetime(2018, 1, 15, 10, 30, 0)


def test_order_happy_path() -> None:
    order = Order(
        order_id="abc-123",
        customer_zip_code="01310",
        order_purchase_timestamp=_ts(),
        order_weight_g=750.5,
    )
    assert order.order_id == "abc-123"
    assert order.customer_zip_code == "01310"
    assert order.order_weight_g == 750.5


def test_order_normalizes_zip_with_missing_leading_zero() -> None:
    order = Order(
        order_id="abc",
        customer_zip_code="1310",
        order_purchase_timestamp=_ts(),
    )
    assert order.customer_zip_code == "01310"


def test_order_weight_is_optional() -> None:
    order = Order(
        order_id="abc",
        customer_zip_code="12345",
        order_purchase_timestamp=_ts(),
    )
    assert order.order_weight_g is None


def test_order_rejects_empty_id() -> None:
    with pytest.raises(ValidationError):
        Order(
            order_id="",
            customer_zip_code="12345",
            order_purchase_timestamp=_ts(),
        )


def test_order_rejects_negative_weight() -> None:
    with pytest.raises(ValidationError):
        Order(
            order_id="abc",
            customer_zip_code="12345",
            order_purchase_timestamp=_ts(),
            order_weight_g=-1.0,
        )


def test_order_rejects_invalid_zip() -> None:
    with pytest.raises(ValidationError):
        Order(
            order_id="abc",
            customer_zip_code="abcde",
            order_purchase_timestamp=_ts(),
        )


def test_order_rejects_none_zip() -> None:
    with pytest.raises(ValidationError):
        Order(
            order_id="abc",
            customer_zip_code=None,  # type: ignore[arg-type]
            order_purchase_timestamp=_ts(),
        )


def test_order_rejects_extra_fields() -> None:
    with pytest.raises(ValidationError):
        Order(
            order_id="abc",
            customer_zip_code="12345",
            order_purchase_timestamp=_ts(),
            unexpected_field="boom",  # type: ignore[call-arg]
        )


def test_order_is_frozen() -> None:
    order = Order(
        order_id="abc",
        customer_zip_code="12345",
        order_purchase_timestamp=_ts(),
    )
    with pytest.raises(ValidationError):
        order.order_id = "mutated"  # type: ignore[misc]
