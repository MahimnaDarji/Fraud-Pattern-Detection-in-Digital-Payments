"""Common utility functions used across the fraud detection system."""

from collections.abc import Iterator
from typing import Any


def chunk_records(records: list[dict[str, Any]], chunk_size: int) -> Iterator[list[dict[str, Any]]]:
    """Yield records in fixed-size batches for scalable processing."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")
    for index in range(0, len(records), chunk_size):
        yield records[index : index + chunk_size]
