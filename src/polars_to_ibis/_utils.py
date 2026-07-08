"""
This is a private module: The API may change.
"""

from collections.abc import Callable
from pprint import pformat
from typing import Any


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


def abbreviate(source: dict[str, Any]) -> str:
    replace(source, "DataFrameScan", lambda _: "...")
    return pformat(source)
