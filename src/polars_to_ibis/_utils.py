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

    def _sub_replace(self, sub_source):
        if isinstance(sub_source, list):
            for i in range(len(sub_source)):
                match sub_source[i]:
                    # TODO: pull out this test into a smaller function,
                    # so we can write tests that don't depend on this structure.
                    case {
                        "Function": {
                            "function": {"FfiPlugin": ffi_params},
                            "input": [input_expr],
                        }
                    }:
                        sub_source[i] = input_expr
                        self._param_dicts.append(ffi_params)
                    case _:
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
