"""
This is a private module: The API may change.
"""

from collections.abc import Callable
from pprint import pformat
from typing import Any


def replace_ffi_with_input(
    source: dict[str, Any] | list[Any] | str,
):  # pragma: no cover
    """
    Modifies `source` in place, and returns the FFI parameters.
    """
    # TODO: When we have a test case with multiple FFIs,
    # generalize this to handle multiple, instead of just the first.
    if isinstance(source, list):
        for i in range(len(source)):
            match source[i]:
                case {
                    "Function": {
                        "function": {"FfiPlugin": params},
                        "input": [input_expr],
                    }
                }:
                    source[i] = input_expr
                    return params
                case _:
                    params = replace_ffi_with_input(source[i])
                    if params is not None:
                        return params
    if isinstance(source, dict):
        for v in source.values():
            if isinstance(v, (dict, list)):
                params = replace_ffi_with_input(v)
                if params is not None:
                    return params
    return None


def find(
    source: dict[str, Any] | list[Any] | str,
    key: str,
) -> Any:  # pragma: no cover
    """
    >>> source = {"foo": [{"bar": 42}]}
    >>> find(source, "bar")
    42
    """
    if isinstance(source, list):
        for i in source:
            found = find(i, key)
            if found is not None:
                return found
    elif isinstance(source, dict):
        for k, v in source.items():
            if k == key:
                return v
            elif isinstance(v, (dict, list)):
                found = find(v, key)
                if found is not None:
                    return found


def replace(
    source: dict[str, Any] | list[Any] | str,
    key: str,
    function: Callable[[Any], Any],
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
            elif isinstance(v, (dict, list)):
                replace(v, key, function)


def abbreviate(source: dict[str, Any]) -> str:
    replace(source, "kwargs", lambda _: "...")
    replace(source, "DataFrameScan", lambda _: "...")
    return pformat(source)
