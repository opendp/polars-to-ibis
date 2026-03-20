from collections.abc import Callable
from typing import Any

import polars as pl
import pytest

from polars_to_ibis.serialization import Serialization


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


io_pairs = [
    (
        pl.LazyFrame().count(),
        {
            "Select": {
                "expr": [
                    {
                        "Agg": {
                            "Count": {
                                "input": {"Selector": "Wildcard"},
                                "include_nulls": False,
                            }
                        }
                    }
                ],
                "input": {"DataFrameScan": "..."},
                "options": {
                    "run_parallel": True,
                    "duplicate_check": True,
                    "should_broadcast": True,
                },
            }
        },
        # {
        #     "Select": {
        #         "expr": [{"Agg": {"Count": [{"Selector": "Wildcard"}, False]}}],
        #         "input": {"DataFrameScan": "..."},
        #         "options": {
        #             "run_parallel": True,
        #             "duplicate_check": True,
        #             "should_broadcast": True,
        #         },
        #     }
        # },
    ),
    (
        pl.LazyFrame().sort(by="ints").head(1),
        {
            "Slice": {
                "input": {
                    "Sort": {
                        "input": {"DataFrameScan": "..."},
                        "by_column": [{"Column": "ints"}],
                        "slice": None,
                        "sort_options": {
                            "descending": [False],
                            "nulls_last": [False],
                            "multithreaded": True,
                            "maintain_order": False,
                            "limit": None,
                        },
                    }
                },
                "offset": 0,
                "len": 1,
            }
        },
    ),
]


@pytest.mark.parametrize("lf,expected", io_pairs)
def test_serialization(lf: pl.LazyFrame, expected: dict[str, Any]):
    serial = Serialization(lf)._serial  # type: ignore
    replace(serial, "DataFrameScan", lambda _: "...")
    assert serial == expected
