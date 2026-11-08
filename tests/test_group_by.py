"""Ordinary typed CASE binning and aggregation through the public converter."""

import ibis
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from polars_to_ibis import convert_polars_to_ibis


def bin_plan(data, expression, candidates, grouped):
    """Build only ordinary Polars nodes, with explicit numeric types."""

    def branch(lo, hi):
        if lo == hi:
            return pl.lit(2 * lo, dtype=pl.UInt64)
        mid = (lo + hi) // 2
        pivot = pl.lit(candidates[mid], dtype=candidates.dtype)
        return (
            pl.when(pl.col("bin_value") < pivot)
            .then(branch(lo, mid))
            .otherwise(
                pl.when(pl.col("bin_value") == pivot)
                .then(pl.lit(2 * mid + 1, dtype=pl.UInt64))
                .otherwise(branch(mid + 1, hi))
            )
        )

    return (
        data.lazy()
        .with_columns(expression.cast(candidates.dtype).alias("bin_value"))
        .with_columns(branch(0, len(candidates)).alias("bucket"))
        .group_by(["key", "bucket"] if grouped else ["bucket"])
        .agg(pl.len().cast(pl.UInt64).alias("count"))
    )


def expected_bins(data, expression, candidates, grouped):
    values = data.select(expression.cast(candidates.dtype)).to_series().to_list()
    counts = {}
    for key, value in zip(data["key"], values):
        bucket = sum(c < value for c in candidates) + sum(
            c <= value for c in candidates
        )
        group = (key, bucket) if grouped else (bucket,)
        counts[group] = counts.get(group, 0) + 1
    schema = ({"key": data.schema["key"]} if grouped else {}) | {
        "bucket": pl.UInt64,
        "count": pl.UInt64,
    }
    return pl.DataFrame(
        [(*key, n) for key, n in counts.items()], schema=schema, orient="row"
    )


def execute(data, plan):
    connection = ibis.duckdb.connect()
    try:
        connection.create_table("source", data)
        query = convert_polars_to_ibis(plan, "source", connection)
        return connection.to_polars(query), str(query.to_sql()).upper()
    finally:
        connection.disconnect()


def assert_same(actual, expected):
    assert_frame_equal(actual.sort(actual.columns), expected.sort(expected.columns))


@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize(
    "dtype,values,candidates",
    [
        (pl.Int8, [-128, -3, 0, 2, 127], [-128, 0, 127]),
        (pl.Int16, [-(2**15), -3, 0, 2, 2**15 - 1], [-(2**15), 0, 2**15 - 1]),
        (pl.Int32, [None, 0, 1, 2, 3, 4, 6, 7], [1, 3, 6]),
        (pl.UInt32, [0, 1, 2, 2, 3, 2**32 - 1], [0, 2, 2**32 - 1]),
        (
            pl.UInt64,
            [0, 2**53, 2**53 + 1, 2**63, 2**64 - 1],
            [2**53, 2**53 + 1, 2**64 - 1],
        ),
        (
            pl.Int64,
            [-(2**63), -(2**63) + 1, -1, 0, 2**63 - 1],
            [-(2**63), 0, 2**63 - 1],
        ),
        (
            pl.Float32,
            [-float("inf"), -0.0, 0.0, 0.1, 0.10000001, 1.0, float("inf")],
            [0, 0.1, 1],
        ),
        (
            pl.Float32,
            [
                -3.4028234663852886e38,
                -1.401298464324817e-45,
                -0.0,
                0.0,
                1.401298464324817e-45,
                3.4028234663852886e38,
            ],
            [-3.4028234663852886e38, 0, 3.4028234663852886e38],
        ),
        (pl.Float64, [-float("inf"), -0.0, 0.1, 1.0, float("inf")], [0, 0.1, 1]),
    ],
)
def test_typed_case_group_by(grouped, empty, dtype, values, candidates):
    data = pl.DataFrame(
        {
            "key": pl.Series(
                [None if i % 2 else 1 for i in range(len(values))], dtype=pl.Int32
            ),
            "value": pl.Series(values, dtype=dtype),
        }
    )
    if empty:
        data = data.clear()
    expression = pl.col("value").fill_null(pl.lit(0, dtype=dtype))
    candidates = pl.Series(candidates, dtype=dtype)
    actual, sql = execute(data, bin_plan(data, expression, candidates, grouped))
    assert_same(actual, expected_bins(data, expression, candidates, grouped))
    assert sql.count("GROUP BY") == 1
    assert "COUNT(*)" in sql
    for forbidden in ("ARRAY_AGG", "LIST(", "UNNEST", "EXPLODE", "OPENDP"):
        assert forbidden not in sql
    if dtype == pl.Float32:
        assert "AS REAL" in sql


@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize(
    "expression",
    [
        pl.col("value").fill_null(0.1).cast(pl.Float32),
        pl.when(pl.col("value").is_null())
        .then(pl.lit(0.1, dtype=pl.Float32))
        .otherwise(pl.col("value")),
        (
            pl.col("value").fill_null(pl.lit(0.1, dtype=pl.Float32))
            + pl.lit(0.5, dtype=pl.Float32)
        ).cast(pl.Float32),
    ],
)
def test_explicitly_typed_float32_preprocessing(grouped, expression):
    # Only explicitly typed expressions/root casts are promised here, not
    # contextual weak-literal coercion in intermediate arithmetic.
    data = pl.DataFrame(
        {"key": [None, 1, 1], "value": pl.Series([None, 0.1, 0.5], dtype=pl.Float32)}
    )
    candidates = pl.Series([0.1, 0.5, 0.6], dtype=pl.Float32)
    actual, _ = execute(data, bin_plan(data, expression, candidates, grouped))
    assert_same(actual, expected_bins(data, expression, candidates, grouped))


def test_computed_key_alias_preserves_source_value():
    data = pl.DataFrame({"key": [0, 0, 1], "value": [1, 3, 6]})
    input_plan = data.lazy().with_columns(
        (pl.col("value") > 2).cast(pl.UInt64).alias("bucket")
    )
    plan = input_plan.group_by((pl.col("key") + 1).alias("value"), "bucket").agg(
        pl.len().alias("n")
    )
    actual, _ = execute(data, plan)
    assert_same(actual, plan.collect())


@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize("empty", [False, True])
def test_alias_cast_len_and_multiple_aggregates(grouped, empty):
    data = pl.DataFrame(
        {
            "key": pl.Series([None, None, 1], dtype=pl.Int32),
            "value": pl.Series([1, None, 3], dtype=pl.Int32),
        }
    )
    if empty:
        data = data.clear()
    exprs = [
        pl.col("value").clip(0, 5).sum().alias("sum"),
        pl.len().cast(pl.UInt64).alias("length"),
        pl.col("value").count().alias("count"),
        pl.col("value").len().alias("include_nulls"),
    ]
    plan = (
        data.lazy().group_by(pl.col("key").alias("renamed")).agg(exprs)
        if grouped
        else data.lazy().select(exprs)
    )
    actual, _ = execute(data, plan)
    assert_same(actual, plan.collect())


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
    plan = (
        data.lazy().group_by("key").agg(expr) if grouped else data.lazy().select(expr)
    )
    actual, _ = execute(data, plan)
    assert_same(actual, plan.collect())
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
    assert_same(actual, plan.collect())
    # Widening Int32 -> UInt64 is not safe because Int32 can be negative.
    assert "TRY_CAST" in sql
    if floating:
        assert "TRUNC(" in sql
