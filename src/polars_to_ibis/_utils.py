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
    >>> source = {
    ...     'Select': {
    ...         'expr': [{
    ...             'Function': {
    ...                 'function': {'FfiPlugin': 'flags-lib-symbol-kwargs'},
    ...                 'input': ['Len']
    ...             }
    ...         }]
    ...     }
    ... }
    >>> replace_ffi_with_input(source)
    'flags-lib-symbol-kwargs'
    >>> source
    {'Select': {'expr': ['Len']}}
    """
    if isinstance(source, list):
        for i in source:
            return replace_ffi_with_input(i)
    elif isinstance(source, dict):
        for k, v in source.items():
            if k == "expr":
                function_payload = v[0].get("Function", {})
                ffi_plugin = function_payload.get("function", {}).get("FfiPlugin")
                ffi_input = function_payload.get("input")
                if ffi_plugin is not None:
                    source[k] = ffi_input
                    return ffi_plugin
            elif isinstance(v, (dict, list)):
                return replace_ffi_with_input(v)
    raise ValueError("Expected dict or list")


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
            return find(i, key)
    elif isinstance(source, dict):
        for k, v in source.items():
            if k == key:
                return v
            elif isinstance(v, (dict, list)):
                return find(v, key)


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
    replace(source, "DataFrameScan", lambda _: "...")
    return pformat(source)
