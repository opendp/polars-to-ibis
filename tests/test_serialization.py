from typing import Any

import polars as pl

from polars_to_ibis.serialization import Serialization


def replace(source: dict[str, Any], key: str, new_value: str) -> None:
    """
    >>> source = {"foo": {"bar": 42}}
    >>> replace(source, "bar", "stub")
    >>> source
    {'foo': {'bar': 'stub'}}
    """
    for k in source.keys():
        if k == key:
            source[k] = new_value
        elif isinstance(source[k], dict):
            replace(source[k], key, new_value)


def test_serialization():
    lf = pl.LazyFrame().sort(by="ints").head(1)
    serial = Serialization(lf)._serial  # type: ignore
    replace(serial, "DataFrameScan", "...")
    assert serial == {
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
    }
