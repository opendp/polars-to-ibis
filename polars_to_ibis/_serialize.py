"""
This is a private module: The API may change.
"""

import json
from typing import Any

import polars as pl

from ._utils import replace


def serialize(lf: pl.LazyFrame):
    serial = json.loads(lf.serialize(format="json"))

    # Cleanup:
    replace(serial, "Count", norm_count_params)

    # Vaidation:
    keys = serial.keys()
    if len(keys) != 1:  # type: ignore
        raise ValueError(f"Expected only a single key, not: {keys}")  # pragma: no cover

    return serial


def norm_count_params(params: dict[str, Any] | list[Any] | str) -> Any:
    if isinstance(params, list):
        return {  # pragma: no cover
            "input": params[0],
            "include_nulls": params[1],
        }
    return params  # pragma: no cover
