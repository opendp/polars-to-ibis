from typing import Any

import polars as pl
import pytest

from polars_to_ibis._serialization import Serialization, replace  # type: ignore

io_pairs = [
    (
        pl.LazyFrame().count(),
        {
            "Select": {
                "expr": [
                    {
                        "Agg": {
                            "Count": {  # In Polars 1.32, this is a list.
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
