"""Validation schemas and helpers for transaction records prior to Kafka publishing."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import math
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator


class TransactionSchema(BaseModel):
    """Flexible transaction schema with explicit support for core transaction fields."""

    model_config = ConfigDict(extra="allow", str_strip_whitespace=True)

    transaction_id: str | None = None
    user_id: str | None = None
    account_id: str | None = None
    merchant_id: str | None = None
    amount: float | int | Decimal | None = None
    timestamp: str | None = None

    @field_validator("transaction_id", "user_id", "account_id", "merchant_id", mode="before")
    @classmethod
    def _normalize_string_identifiers(cls, value: Any) -> str | None:
        """Convert null-like values to None and normalize identifier strings."""
        if _is_null_like(value):
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("amount", mode="before")
    @classmethod
    def _normalize_amount(cls, value: Any) -> float | None:
        """Ensure amount is numeric and non-negative when present."""
        if _is_null_like(value):
            return None
        try:
            amount = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("amount must be numeric") from exc
        if amount < 0:
            raise ValueError("amount must be greater than or equal to 0")
        return amount

    @field_validator("timestamp", mode="before")
    @classmethod
    def _normalize_timestamp(cls, value: Any) -> str | None:
        """Normalize the canonical timestamp field to ISO 8601 format."""
        if _is_null_like(value):
            return None
        parsed = _parse_timestamp(value)
        if parsed is None:
            raise ValueError("timestamp must be parseable")
        return parsed


def validate_transaction_record(
    record: dict[str, Any],
    dataset_columns: set[str] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Validate and normalize a transaction payload while preserving input column names."""
    columns = dataset_columns or set(record.keys())

    try:
        model = TransactionSchema.model_validate(record)
    except ValidationError as exc:
        return None, _format_validation_error(exc)

    model_dump = model.model_dump()
    validated: dict[str, Any] = {}
    for key in record.keys():
        validated[key] = model_dump.get(key)

    timestamp_error = _normalize_timestamp_like_columns(validated)
    if timestamp_error is not None:
        return None, timestamp_error

    rule_error = _validate_core_rules(validated, columns)
    if rule_error is not None:
        return None, rule_error

    return validated, None


def _validate_core_rules(record: dict[str, Any], columns: set[str]) -> str | None:
    """Apply dynamic business-safe validation rules based on available columns."""
    if "transaction_id" in columns and _is_empty_identifier(record.get("transaction_id")):
        return "transaction_id cannot be null or empty"

    if "amount" in columns:
        amount = record.get("amount")
        if amount is None:
            return "amount is required"
        try:
            numeric_amount = float(amount)
        except (TypeError, ValueError):
            return "amount must be numeric"
        if numeric_amount < 0:
            return "amount must be greater than or equal to 0"

    has_user_id = "user_id" in columns
    has_account_id = "account_id" in columns
    if has_user_id or has_account_id:
        user_id = record.get("user_id")
        account_id = record.get("account_id")
        if _is_empty_identifier(user_id) and _is_empty_identifier(account_id):
            return "user_id or account_id is required"

    return None


def _normalize_timestamp_like_columns(record: dict[str, Any]) -> str | None:
    """Normalize all timestamp-like fields to ISO strings and reject unparseable values."""
    for key, value in record.items():
        key_lower = key.lower()
        if "time" not in key_lower and "date" not in key_lower and "timestamp" not in key_lower:
            continue
        if value is None:
            continue

        parsed = _parse_timestamp(value)
        if parsed is None:
            return f"{key} must be parseable"
        record[key] = parsed
    return None


def _parse_timestamp(value: Any) -> str | None:
    """Parse common timestamp types and return ISO 8601 string."""
    if _is_null_like(value):
        return None

    if isinstance(value, (datetime, date, pd.Timestamp)):
        return pd.Timestamp(value).isoformat()

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.isoformat()


def _format_validation_error(exc: ValidationError) -> str:
    """Create a compact validation error message suitable for logs."""
    first_error = exc.errors()[0]
    field_path = ".".join(str(part) for part in first_error.get("loc", [])) or "record"
    message = first_error.get("msg", "validation error")
    return f"{field_path}: {message}"


def _is_null_like(value: Any) -> bool:
    """Detect Python/pandas null-like values consistently."""
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return bool(pd.isna(value))


def _is_empty_identifier(value: Any) -> bool:
    """Detect null or empty string identifiers."""
    if value is None:
        return True
    return str(value).strip() == ""
