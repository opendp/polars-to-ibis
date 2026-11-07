"""The public converter lowers a native logical OpenDP head to direct SQL counts."""

import io
import json
import pickle
import warnings

import ibis
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from polars_to_ibis import convert_polars_to_ibis
from polars_to_ibis._parse.opendp_histogram import _parameter_dict


def serialized(lf):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return json.loads(lf.serialize(format="json"))


def logical_head(data, expression, candidates, grouped):
    # Native flatten shape, with the actual FFI aggregate expression substituted.
    # OpenDP's integration tests also pass real Rust-generated heads through this
    # same public converter; this fixture does not implement a translator mock.
    skeleton = pl.DataFrame({"key": [0], "hist": [{"bucket": 0, "count": 1}]}).lazy()
    if grouped:
        skeleton = skeleton.group_by("key").agg("hist").explode("hist")
    head = serialized(skeleton.unnest("hist"))
    source = head["MapFunction"]["input"]
    if grouped:
        source = source["MapFunction"]["input"]
    else:
        # Global histogram is a variable-length Struct expression in Select.
        source.clear()
        source["Select"] = {
            "input": {},
            "expr": [],
            "options": {
                "duplicate_check": True,
                "run_parallel": True,
                "should_broadcast": True,
            },
        }
    payload = source["GroupBy" if grouped else "Select"]
    payload["input"] = serialized(data.lazy())
    expr = serialized(data.lazy().select(expression.alias("value")))["Select"]["expr"][
        0
    ]["Alias"][0]
    args = {
        "scorer": {
            "candidates": candidates.__getstate__(),
            "alpha": (1, 2),
            "size_limit": 100,
        },
        "bucket_name": "bucket",
        "count_name": "count",
    }
    plugin = {
        "Function": {
            "input": [expr],
            "function": {
                "FfiPlugin": {
                    "lib": "/test/libopendp.so",
                    "symbol": "discrete_quantile_histogram_plugin",
                    "kwargs": list(pickle.dumps(args, protocol=3)),
                    "flags": {"check_lengths": True, "flags": "", "cast_options": None},
                }
            },
        }
    }
    payload["aggs" if grouped else "expr"] = [{"Alias": [plugin, "hist"]}]
    return pl.LazyFrame.deserialize(io.StringIO(json.dumps(head)), format="json")


def expected_histogram(data, expression, candidates, grouped):
    groups = (
        data.partition_by("key", as_dict=True, maintain_order=True)
        if grouped
        else {(): data}
    )
    rows = []
    for key, frame in groups.items():
        values = frame.select(expression).to_series().to_list()
        counts = [0] * (2 * len(candidates) + 1)
        for value in values:
            bucket = sum(c < value for c in candidates) + sum(
                c <= value for c in candidates
            )
            counts[bucket] += 1
        bins = [(i, n) for i, n in enumerate(counts) if n] or [(0, 0)]
        rows.extend(([key[0], b, n] if grouped else [b, n]) for b, n in bins)
    schema = ({"key": data.schema["key"]} if grouped else {}) | {
        "bucket": pl.UInt64,
        "count": pl.UInt64,
    }
    return pl.DataFrame(rows, schema=schema, orient="row")


@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize(
    "operation",
    [
        "plain",
        "partial",
        "all",
        "drop_nulls",
        "literal",
        "literal_empty",
        "literal_filtered_by_column",
        "same_selection",
    ],
)
def test_sparse_histogram_sql(grouped, empty, operation):
    data = pl.DataFrame(
        {
            "key": pl.Series([None, None, 1, 2], dtype=pl.Int32),
            "value": pl.Series([1, 3, 6, None], dtype=pl.Int32),
        }
    )
    if empty:
        data = data.clear()
    value = pl.col("value")
    expression = {
        "plain": value.fill_null(3),
        "partial": value.filter(value > 1).fill_null(3),
        "all": value.filter(value > 99).fill_null(3),
        "drop_nulls": value.drop_nulls(),
        "literal": pl.lit(3, dtype=pl.Int32),
        "literal_empty": pl.lit(3, dtype=pl.Int32).filter(pl.lit(False)),
        "literal_filtered_by_column": pl.lit(3, dtype=pl.Int32).filter(value > 1),
        "same_selection": value.filter(value > 1) + value.filter(value > 1),
    }[operation]
    candidates = pl.Series("", [1, 3, 6], dtype=pl.Int32)
    head = logical_head(data, expression, candidates, grouped)
    connection = ibis.duckdb.connect()
    try:
        connection.create_table("private", data)
        relation = convert_polars_to_ibis(head, "private", ibis.duckdb)
        sql = str(relation.to_sql(dialect="duckdb")).upper()
        assert "COUNT(" in sql and "GROUP BY" in sql
        for forbidden in ["ARRAY_AGG", "LIST(", "UNNEST", "EXPLODE"]:
            assert forbidden not in sql
        actual = connection.to_polars(relation)
        expected = expected_histogram(data, expression, candidates, grouped)
        assert_frame_equal(actual.sort(actual.columns), expected.sort(expected.columns))
    finally:
        connection.disconnect()


@pytest.mark.parametrize(
    "dtype,values,candidates",
    [
        (pl.UInt64, [0, 2**53, 2**53 + 1, 2**64 - 1], [2**53, 2**53 + 1, 2**64 - 1]),
        (pl.Int64, [-(2**63), -1, 0, 2**63 - 1], [-(2**63), 0, 2**63 - 1]),
        (pl.Float32, [-float("inf"), -0.0, 0.0, 1.0, float("inf")], [-1, 0, 1]),
        (pl.Float64, [None, float("nan"), -1.0, 1.0], [-1, 0, 1]),
    ],
)
def test_exact_typed_bins(dtype, values, candidates):
    data = pl.DataFrame(
        {
            "key": pl.Series([0] * len(values), dtype=pl.Int32),
            "value": pl.Series(values, dtype=dtype),
        }
    )
    expression = pl.col("value").fill_null(0)
    if dtype.is_float():
        expression = expression.fill_nan(0)
    candidates = pl.Series("", candidates, dtype=dtype)
    connection = ibis.duckdb.connect()
    try:
        connection.create_table("private", data)
        relation = convert_polars_to_ibis(
            logical_head(data, expression, candidates, True), "private", ibis.duckdb
        )
        actual = connection.to_polars(relation).sort("bucket")
        expected = expected_histogram(data, expression, candidates, True).sort("bucket")
        assert_frame_equal(actual, expected)
    finally:
        connection.disconnect()


def test_key_alias_does_not_overwrite_scorer_input():
    data = pl.DataFrame(
        {
            "key": pl.Series([0, 0, 1], dtype=pl.Int32),
            "value": pl.Series([1, 3, 6], dtype=pl.Int32),
        }
    )
    candidates = pl.Series("", [1, 3, 6], dtype=pl.Int32)
    head = serialized(logical_head(data, pl.col("value"), candidates, True))
    group = head["MapFunction"]["input"]["MapFunction"]["input"]["GroupBy"]
    group["keys"] = [{"Alias": [{"Column": "key"}, "value"]}]
    head = pl.LazyFrame.deserialize(io.StringIO(json.dumps(head)), format="json")
    connection = ibis.duckdb.connect()
    try:
        connection.create_table("private", data)
        actual = connection.to_polars(
            convert_polars_to_ibis(head, "private", ibis.duckdb)
        )
        expected = expected_histogram(data, pl.col("value"), candidates, True).rename(
            {"key": "value"}
        )
        assert_frame_equal(actual.sort(actual.columns), expected.sort(expected.columns))
    finally:
        connection.disconnect()


def test_pickle_parameters_do_not_execute_globals():
    class Attack:
        def __reduce__(self):
            return (eval, ("1 + 1",))

    with pytest.raises(ValueError, match="pickle globals"):
        _parameter_dict(pickle.dumps(Attack()))
    # An EXT opcode can otherwise bypass find_class using pickle's cache.
    with pytest.raises(ValueError, match="pickle globals"):
        _parameter_dict(b"\x80\x02\x82\x01.")


@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize("all_removed", [False, True])
def test_nan_and_null_removal(grouped, all_removed):
    values = [None, float("nan")] if all_removed else [None, float("nan"), -1.0, 1.0]
    data = pl.DataFrame(
        {
            "key": pl.Series([None] * len(values), dtype=pl.Int32),
            "value": pl.Series(values, dtype=pl.Float64),
        }
    )
    expression = pl.col("value").drop_nulls().drop_nans()
    candidates = pl.Series("", [-1.0, 0.0, 1.0])
    connection = ibis.duckdb.connect()
    try:
        connection.create_table("private", data)
        table = convert_polars_to_ibis(
            logical_head(data, expression, candidates, grouped), "private", ibis.duckdb
        )
        actual = connection.to_polars(table)
        expected = expected_histogram(data, expression, candidates, grouped)
        assert_frame_equal(actual.sort(actual.columns), expected.sort(expected.columns))
    finally:
        connection.disconnect()


@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize(
    "values", [[None, float("nan"), 1.0, 6.0], [None, float("nan")]]
)
def test_drop_nans_retains_nulls_for_outer_fill(grouped, values):
    data = pl.DataFrame(
        {
            "key": pl.Series([None] * len(values), dtype=pl.Int32),
            "value": pl.Series(values, dtype=pl.Float64),
        }
    )
    expression = pl.col("value").drop_nans().fill_null(3.0)
    candidates = pl.Series("", [1.0, 3.0, 6.0])
    connection = ibis.duckdb.connect()
    try:
        connection.create_table("private", data)
        actual = connection.to_polars(
            convert_polars_to_ibis(
                logical_head(data, expression, candidates, grouped),
                "private",
                ibis.duckdb,
            )
        )
        expected = expected_histogram(data, expression, candidates, grouped)
        assert_frame_equal(actual.sort(actual.columns), expected.sort(expected.columns))
    finally:
        connection.disconnect()


def test_literal_fill_null_preprocessing():
    data = pl.DataFrame({"key": [0, 0], "value": [1, 6]})
    expression = pl.lit(3).fill_null(3.0)
    candidates = pl.Series("", [1.0, 3.0, 6.0])
    connection = ibis.duckdb.connect()
    try:
        connection.create_table("private", data)
        actual = connection.to_polars(
            convert_polars_to_ibis(
                logical_head(data, expression, candidates, False),
                "private",
                ibis.duckdb,
            )
        )
        assert_frame_equal(
            actual, expected_histogram(data, expression, candidates, False)
        )
    finally:
        connection.disconnect()


def test_independent_positional_selections_are_not_conjoined():
    data = pl.DataFrame({"key": [0, 0, 0], "value": [1, 3, 6]})
    value = pl.col("value")
    expr = value.filter(value > 1) + value.filter(value < 6)
    head = logical_head(data, expr, pl.Series("", [1, 3, 6]), True)
    with pytest.raises(NotImplementedError, match="positional"):
        convert_polars_to_ibis(head, "private", ibis.duckdb)


@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize("empty", [False, True])
def test_compatible_scalar_head_aggregation(grouped, empty):
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
        pl.len().cast(pl.Int64).alias("length"),
        pl.col("value").count().alias("count"),
        pl.col("value").len().alias("include_nulls"),
    ]
    native = (
        data.lazy().group_by(pl.col("key").alias("renamed")).agg(exprs)
        if grouped
        else data.lazy().select(exprs)
    )
    connection = ibis.duckdb.connect()
    try:
        connection.create_table("private", data)
        actual = connection.to_polars(
            convert_polars_to_ibis(native, "private", ibis.duckdb)
        )
        expected = native.collect()
        assert_frame_equal(actual.sort(actual.columns), expected.sort(expected.columns))
    finally:
        connection.disconnect()
