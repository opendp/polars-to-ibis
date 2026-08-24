"""
This is a private module: The API may change.
"""

from collections import namedtuple
from collections.abc import Callable
from pprint import pformat
from typing import Any

PluginDetails = namedtuple("PluginDetails", ["params_dict", "input_expr"])


def _find_pattern(sub_source):
    match sub_source:
        case {
            "Function": {
                "function": {"FfiPlugin": params_dict},
                "input": [input_expr],
            }
        }:
            return PluginDetails(params_dict=params_dict, input_expr=input_expr)


class PluginReplacer:
    """
    Finds all FFI plugins in an expression,
    pulls them out, and replaces them with their inputs,
    and separately returns the parameters for each plugin call.
    """

    def __init__(self, source, find_pattern=_find_pattern):
        self._source = source
        self._param_dicts = []
        self._find_pattern = find_pattern

    def replace(self):
        self._sub_replace(self._source)
        if not self._param_dicts:
            raise Exception(f"Did not find FFI in {self._source}")  # pragma: no cover
        return self._param_dicts

    def _sub_replace(self, sub_source):
        if isinstance(sub_source, list):
            for i in range(len(sub_source)):
                plugin_details = self._find_pattern(sub_source[i])
                if plugin_details:
                    sub_source[i] = plugin_details.input_expr
                    self._param_dicts.append(plugin_details.params_dict)
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
