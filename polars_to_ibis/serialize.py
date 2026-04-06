"""
LazyFrame.serialize() does not return stable results between Polars versions,
and may be dropped in the future.
Pulling out the serialization and validation logic keeps the rest of the code simple.
"""

import json
from collections.abc import Callable
from typing import Any

import jsonschema
import polars as pl


class UnexpectedPolarsException(Exception):
    """
    JSON structure is not what we expected.
    """

    pass


class Serialization:
    def __init__(self, lf: pl.LazyFrame):
        self._serial = json.loads(lf.serialize(format="json"))

        def norm_count_params(params: dict[str, Any] | list[Any] | str) -> Any:
            if isinstance(params, list):
                return {  # pragma: no cover
                    "input": params[0],
                    "include_nulls": params[1],
                }
            return params  # pragma: no cover

        replace(self._serial, "Count", norm_count_params)
        self._validate()

    def _validate(self):
        keys = self._serial.keys()
        if len(keys) != 1:  # type: ignore
            raise UnexpectedPolarsException(  # pragma: no cover
                f"Expected only a single key, not: {keys}"
            )

        jsonschema.validate(self._serial, {"type": "object"})  # type: ignore


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
