"""Regression gates for preprocessing at the native/SQL histogram boundary."""

import ibis
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from polars_to_ibis import convert_polars_to_ibis

from .test_opendp_histogram import expected_histogram, logical_head


@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize(
    "expression,candidates",
    [
        (pl.col("value").fill_null(0.1), pl.Series([0, 0.1, 1], dtype=pl.Float32)),
        (
            pl.col("value").fill_null(0.1) - 0.1,
            pl.Series([-1e-10, 0, 1e-10], dtype=pl.Float32),
        ),
        (
            pl.when(pl.col("value").is_null()).then(0.1).otherwise(pl.col("value")),
            pl.Series([0, 0.1, 1], dtype=pl.Float32),
        ),
        (
            pl.col("value").fill_null(pl.lit(0.1, dtype=pl.Float64)),
            pl.Series([0, 0.1, 1], dtype=pl.Float64),
        ),
        (
            pl.col("value").fill_null(0.1 + 0.2),
            pl.Series([0, 0.3, 1], dtype=pl.Float32),
        ),
        (
            pl.col("value").fill_null(0.1) ** 2,
            pl.Series(
                [0, float(pl.Series([0.1], dtype=pl.Float32)[0]) ** 2, 1],
                dtype=pl.Float32,
            ),
        ),
    ],
    ids=[
        "fill",
        "intermediate-arithmetic",
        "ternary",
        "strong-literal",
        "weak-fill",
        "power",
    ],
)
def test_contextual_float_literals(grouped, expression, candidates):
    data = pl.DataFrame(
        {
            "key": pl.Series([None, None, 1], dtype=pl.Int32),
            "value": pl.Series([None, 0.1, 0.1], dtype=pl.Float32),
        }
    )
    connection = ibis.duckdb.connect()
    try:
        connection.create_table("private", data)
        head = logical_head(data, expression, candidates, grouped)
        query = convert_polars_to_ibis(head, "private", connection)
        actual = connection.to_polars(query)
        expected = expected_histogram(data, expression, candidates, grouped)
        assert_frame_equal(actual.sort(actual.columns), expected.sort(expected.columns))
    finally:
        connection.disconnect()


@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("floating", [False, True])
def test_nonstrict_overflow_cast(grouped, empty, floating):
    data = pl.DataFrame(
        {
            "key": pl.Series([None, None, 1, 1], dtype=pl.Int32),
            "value": (
                pl.Series([128.9, -129.9, None, 1.9], dtype=pl.Float64)
                if floating
                else pl.Series([128, -129, None, 1], dtype=pl.Int32)
            ),
        }
    )
    if empty:
        data = data.clear()
    expression = pl.col("value").cast(pl.Int8, strict=False).fill_null(0)
    candidates = pl.Series([-1, 0, 1], dtype=pl.Int8)
    connection = ibis.duckdb.connect()
    try:
        connection.create_table("private", data)
        query = convert_polars_to_ibis(
            logical_head(data, expression, candidates, grouped), "private", connection
        )
        assert "TRY_CAST" in str(query.to_sql())
        actual = connection.to_polars(query)
        expected = expected_histogram(data, expression, candidates, grouped)
        assert_frame_equal(actual.sort(actual.columns), expected.sort(expected.columns))
    finally:
        connection.disconnect()


@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize("values", [[True, True, False, None], [], [None, None]])
def test_boolean_sum_counts_true(grouped, values):
    data = pl.DataFrame(
        {
            "key": pl.Series([i % 2 for i in range(len(values))], dtype=pl.Int32),
            "value": pl.Series(values, dtype=pl.Boolean),
        }
    )
    expr = pl.col("value").sum().alias("sum")
    query = (
        data.lazy().group_by("key").agg(expr) if grouped else data.lazy().select(expr)
    )
    connection = ibis.duckdb.connect()
    try:
        connection.create_table("private", data)
        translated = convert_polars_to_ibis(query, "private", connection)
        actual, expected = connection.to_polars(translated), query.collect()
        assert_frame_equal(actual.sort(actual.columns), expected.sort(expected.columns))
        assert actual.schema["sum"] == pl.UInt32
    finally:
        connection.disconnect()


@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize("target", [pl.Int8, pl.UInt64])
def test_float_cast_does_not_overflow_an_intermediate_int64(grouped, target):
    values = [
        float("nan"),
        float("inf"),
        -float("inf"),
        1e30,
        -1e30,
        float(2**63),
        1.9,
        -1.9,
    ]
    data = pl.DataFrame({"key": [i % 2 for i in range(len(values))], "value": values})
    expression = pl.col("value").cast(target, strict=False).fill_null(0)
    candidates = pl.Series(
        [0, 1, 2] if target == pl.Int8 else [0, 1, 2**63], dtype=target
    )
    connection = ibis.duckdb.connect()
    try:
        connection.create_table("private", data)
        query = convert_polars_to_ibis(
            logical_head(data, expression, candidates, grouped), "private", connection
        )
        sql = str(query.to_sql())
        assert "TRUNC(" in sql and "TRY_CAST" in sql
        actual = connection.to_polars(query)
        expected = expected_histogram(data, expression, candidates, grouped)
        assert_frame_equal(actual.sort(actual.columns), expected.sort(expected.columns))
    finally:
        connection.disconnect()
