"""Unit tests for shared field validators."""
from __future__ import annotations

import pytest

from src.models._validators import validate_zip_code


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1310", "01310"),
        ("01310", "01310"),
        ("1", "00001"),
        ("99999", "99999"),
    ],
)
def test_validate_zip_code_pads_with_leading_zeros(raw: str, expected: str) -> None:
    assert validate_zip_code(raw) == expected


@pytest.mark.parametrize(
    "bad",
    ["", "abcde", "12a45", "123456", "12 34", "12-34"],
)
def test_validate_zip_code_rejects_invalid(bad: str) -> None:
    with pytest.raises(ValueError):
        validate_zip_code(bad)
