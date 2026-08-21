"""
This is a private module: The API may change.
"""

from collections.abc import Callable
from pprint import pformat
from typing import Any


class PluginReplacer:
    """
    Finds all FFI plugins in an expression,
    pulls them out, and replaces them with their inputs,
    and separately returns the parameters for each plugin call.
    """

    def __init__(self, source):
        self._source = source
        self._param_dicts = []

    def replace(self):
        self._sub_replace(self._source)
        if not self._param_dicts:
            raise Exception(f"Did not find FFI in {self._source}")
        return self._param_dicts

    def _find_pattern(self, sub_source):
        # Testing subclass could override this.
        match sub_source:
            case {
                "Function": {
                    "function": {"FfiPlugin": ffi_params},
                    "input": [input_expr],
                }
            }:
                return (ffi_params, input_expr)

    def _sub_replace(self, sub_source):
        if isinstance(sub_source, list):
            for i in range(len(sub_source)):
                params_input_tuple = self._find_pattern(sub_source[i])
                if params_input_tuple:
                    sub_source[i] = params_input_tuple[1]
                    self._param_dicts.append(params_input_tuple[0])
                else:
                    self._sub_replace(sub_source[i])
        if isinstance(sub_source, dict):
            for v in sub_source.values():
                if isinstance(v, (dict, list)):
                    self._sub_replace(v)


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
