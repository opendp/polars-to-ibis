import ibis
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from polars_to_ibis import convert_polars_to_ibis


def execute(data, plan):
    pytest.importorskip("duckdb")
    connection = ibis.duckdb.connect()
    try:
        connection.create_table("source", data)
        expression = convert_polars_to_ibis(plan, "source", connection)
        return connection.to_polars(expression), str(expression.to_sql()).upper()
    finally:
        connection.disconnect()


@pytest.mark.parametrize(
    "dtype",
    [
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.Float32,
        pl.Float64,
    ],
)
def test_numeric_input_schema_preserves_type(dtype):
    pytest.importorskip("duckdb")
    connection = ibis.duckdb.connect()
    try:
        plan = pl.LazyFrame(schema={"value": dtype}).select("value")
        expression = convert_polars_to_ibis(plan, "source", connection)
        assert expression.schema()["value"] == ibis.dtype(str(dtype).lower())
    finally:
        connection.disconnect()


@pytest.mark.parametrize("empty", [False, True])
def test_select_mixed_aggregations(empty):
    data = pl.DataFrame(
        {
            "key": pl.Series([None, None, 1], dtype=pl.Int32),
            "value": pl.Series([1, None, 3], dtype=pl.Int32),
        }
    )
    if empty:
        data = data.clear()
    plan = data.lazy().select(
        pl.col("value").clip(0, 5).sum().alias("sum"),
        pl.len().cast(pl.UInt64).alias("length"),
        pl.col("value").count().alias("count"),
        pl.col("value").len().alias("include_nulls"),
    )
    actual, _ = execute(data, plan)
    assert_frame_equal(actual, plan.collect())


@pytest.mark.parametrize("values", [[True, True, False, None], [], [None, None]])
def test_boolean_sum_counts_true(values):
    data = pl.DataFrame({"value": pl.Series(values, dtype=pl.Boolean)})
    plan = data.lazy().select(pl.col("value").sum().alias("sum"))
    actual, _ = execute(data, plan)
    assert_frame_equal(actual, plan.collect())
    assert actual.schema["sum"] == pl.UInt32


@pytest.mark.parametrize("target", [pl.Int8, pl.UInt64])
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("floating", [False, True])
def test_nonstrict_numeric_cast(target, empty, floating):
    values = [128, -129, None, 1, -1]
    if floating:
        values = [
            128.9,
            -129.9,
            None,
            1.9,
            -1.9,
            float("nan"),
            float("inf"),
            -float("inf"),
            1e30,
            -1e30,
            float(2**63),
        ]
    data = pl.DataFrame(
        {"value": pl.Series(values, dtype=pl.Float64 if floating else pl.Int32)}
    )
    if empty:
        data = data.clear()
    plan = data.lazy().select(
        pl.col("value").cast(target, strict=False).fill_null(pl.lit(0, dtype=target))
    )
    actual, sql = execute(data, plan)
    assert_frame_equal(actual, plan.collect())
    assert "TRY_CAST" in sql
    if floating:
        assert "TRUNC(" in sql


@pytest.mark.parametrize(
    "dtype,values,pivot",
    [
        (pl.Int8, [-128, 0, 127], 0),
        (pl.UInt64, [0, 2**63, 2**64 - 1], 2**63),
        (pl.Float32, [0.0, 0.1, 1.0], 0.1),
    ],
)
def test_typed_case_literals(dtype, values, pivot):
    data = pl.DataFrame({"value": pl.Series(values, dtype=dtype)})
    expression = (
        pl.when(pl.col("value") < pl.lit(pivot, dtype=dtype))
        .then(pl.lit(0, dtype=pl.UInt64))
        .otherwise(pl.lit(1, dtype=pl.UInt64))
        .alias("bucket")
    )
    plan = data.lazy().select(expression)
    actual, _ = execute(data, plan)
    assert_frame_equal(actual, plan.collect())


def test_float32_ternary():
    data = pl.DataFrame(
        {"value": pl.Series([None, 0.1, 0.5], dtype=pl.Float32)}
    )
    expression = (
        pl.when(pl.col("value").is_null())
        .then(pl.lit(0.1, dtype=pl.Float32))
        .otherwise(pl.col("value"))
        .alias("filled")
    )
    plan = data.lazy().select(expression)
    actual, _ = execute(data, plan)
    assert_frame_equal(actual, plan.collect())

@pytest.mark.parametrize("empty", [False, True])
def test_typed_null_literal(empty):
    data = pl.DataFrame({"value": pl.Series([1, None, 3], dtype=pl.Int32)})
    if empty:
        data = data.clear()
    expression = (
        pl.when(pl.col("value").is_null())
        .then(pl.lit(None, dtype=pl.Int32))
        .otherwise(pl.col("value"))
        .alias("value")
    )
    plan = data.lazy().select(expression)
    actual, _ = execute(data, plan)
    assert_frame_equal(actual, plan.collect())
