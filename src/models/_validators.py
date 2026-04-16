"""Reusable field validators shared across pipeline models."""
from __future__ import annotations

ZIP_CODE_LENGTH = 5


def validate_zip_code(value: str) -> str:
    """Normalize and validate a US/Olist zip code prefix.

    Accepts 1-5 digit strings (Olist stores zips as integer prefixes that
    may be missing leading zeros) and returns a 5-character zero-padded
    digit string.

    Args:
        value: Raw zip-code-like string.

    Returns:
        Zero-padded 5-digit string.

    Raises:
        ValueError: If the value is empty, longer than 5 chars, or contains
            non-digit characters.
    """
    if not value:
        raise ValueError("zip code must not be empty")
    if not value.isdigit():
        raise ValueError(f"zip code must contain only digits, got: {value!r}")
    if len(value) > ZIP_CODE_LENGTH:
        raise ValueError(
            f"zip code must be at most {ZIP_CODE_LENGTH} digits, got: {value!r}"
        )
    return value.zfill(ZIP_CODE_LENGTH)
