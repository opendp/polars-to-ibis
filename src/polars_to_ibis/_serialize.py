"""
This is a private module: The API may change.
"""

import json
import warnings
from typing import Any

import polars as pl

from ._utils import replace


def serialize(lf: pl.LazyFrame):
    with warnings.catch_warnings():
        # JSON serialization is deprecated by Polars.
        # If it's dropped by a future version,
        # we can work with the binary serialization.
        warnings.simplefilter("ignore", category=UserWarning)
        json_serialization = lf.serialize(format="json")
    serial = json.loads(json_serialization)

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
