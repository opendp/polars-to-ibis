"""
This is a private module: The API may change.
"""

from collections.abc import Callable
from pprint import pformat
from typing import Any


def replace_ffi_with_input(
    source: dict[str, Any] | list[Any] | str,
):
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
                ffi_plugin = (
                    v[0].get("Function", {}).get("function", {}).get("FfiPlugin")
                )
                ffi_input = v[0].get("Function", {}).get("input")
                if ffi_plugin is not None:
                    source[k] = ffi_input
                    return ffi_plugin
            elif isinstance(v, (dict, list)):
                return replace_ffi_with_input(v)


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
