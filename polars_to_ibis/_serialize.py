"""
This is a private module: The API may change.
"""

import json
from collections.abc import Callable
from typing import Any

import polars as pl


def serialize(lf: pl.LazyFrame):
    serial = json.loads(lf.serialize(format="json"))

    # Cleanup:
    replace(serial, "Count", norm_count_params)

    # Vaidation:
    keys = serial.keys()
    if len(keys) != 1:  # type: ignore
        raise NotImplementedError(  # pragma: no cover
            f"Expected only a single key, not: {keys}"
        )

    return serial


def norm_count_params(params: dict[str, Any] | list[Any] | str) -> Any:
    if isinstance(params, list):
        return {  # pragma: no cover
            "input": params[0],
            "include_nulls": params[1],
        }
    return params  # pragma: no cover


def replace(
    source: dict[str, Any] | list[Any] | str, key: str, function: Callable[[Any], Any]
) -> None:
    """
    >>> source = {"foo": [{"bar": 42}]}
    >>> def sound_excited(old):
    ...     return f"{old}!"
    >>> replace(source, "bar", sound_excited)
    >>> source
    {'foo': [{'bar': '42!'}]}
    """
    if isinstance(source, list):
        for i in source:
            replace(i, key, function)
    elif isinstance(source, dict):
        for k, v in source.items():
            if k == key:
                source[k] = function(v)
            elif isinstance(source[k], (dict, list)):
                replace(source[k], key, function)
