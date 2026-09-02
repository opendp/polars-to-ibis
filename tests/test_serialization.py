from typing import Any

import polars as pl  # type: ignore # noqa: F401
import pytest

from polars_to_ibis._parse import tags
from polars_to_ibis._serialize import replace, serialize  # type: ignore

io_pairs = [
    (
        "pl.LazyFrame().count()",
        {
            tags.table.SELECT: {
                "expr": [
                    {
                        tags.value.AGG: {
                            "Count": {  # In Polars 1.32, this is a list.
                                "input": {"Selector": "Wildcard"},
                                "include_nulls": False,
                            }
                        }
                    }
                ],
                "input": {tags.table.DATA_FRAME_SCAN: "..."},
                "options": {
                    "run_parallel": True,
                    "duplicate_check": True,
                    "should_broadcast": True,
                },
            }
        },
    ),
    (
        "pl.LazyFrame().null_count()",
        {
            tags.table.SELECT: {
                "expr": [
                    {
                        tags.value.FUNCTION: {
                            "function": "NullCount",
                            # Unlike count(), which does not have a wrapping list.
                            "input": [
                                {
                                    "Selector": "Wildcard",
                                },
                            ],
                        },
                    },
                ],
                "input": {
                    tags.table.DATA_FRAME_SCAN: "...",
                },
                "options": {
                    "duplicate_check": True,
                    "run_parallel": True,
                    "should_broadcast": True,
                },
            },
        },
    ),
    (
        'pl.LazyFrame().sort(by="ints").head(1)',
        {
            tags.table.SLICE: {
                "input": {
                    tags.table.SORT: {
                        "input": {tags.table.DATA_FRAME_SCAN: "..."},
                        "by_column": [{tags.value.COLUMN: "ints"}],
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


@pytest.mark.parametrize("lf_str,expected", io_pairs)
def test_serialization(lf_str: str, expected: dict[str, Any]):
    lf = eval(lf_str)
    serial = serialize(lf)  # type: ignore
    replace(serial, tags.table.DATA_FRAME_SCAN, lambda _: "...")
    assert serial == expected
