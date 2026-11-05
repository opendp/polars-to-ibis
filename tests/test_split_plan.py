"""Data-only split planning, independent of any native plugin implementation."""

from copy import deepcopy

import ibis
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from polars_to_ibis import split_polars_on_ffi_plan
from polars_to_ibis._serialize import serialize
from polars_to_ibis._split import _aggregate_expr


def plugin(child, marker="test"):
    return {
        "Function": {
            "function": {
                "FfiPlugin": {
                    "symbol": marker,
                    "kwargs": [1, 2, 3],
                    "flags": {"flags": "LENGTH_PRESERVING"},
                }
            },
            "input": [child],
        }
    }


def alias(expr, name):
    return {"Alias": [expr, name]}


def binary(left, right, op="TrueDivide"):
    return {"BinaryExpr": {"left": left, "op": op, "right": right}}


def literal(value):
    return {"Literal": {"Dyn": {"Int": value}}}


SUM = {"Agg": {"Sum": {"Column": "x"}}}
COUNT_ALL = {"Agg": {"Count": {"input": {"Column": "x"}, "include_nulls": True}}}
COUNT_NONNULL = {"Agg": {"Count": {"input": {"Column": "x"}, "include_nulls": False}}}


def make_plan(monkeypatch, expressions, *, schema=None, source=None, backend=None):
    query = pl.LazyFrame(schema=schema or {"x": pl.Int64}).select(pl.lit(0))
    source = source if source is not None else serialize(query)
    source["Select"]["expr"] = deepcopy(expressions)
    original = deepcopy(source)
    monkeypatch.setattr("polars_to_ibis._split.serialize", lambda _: source)
    result = split_polars_on_ffi_plan(query, "private_table", backend or ibis.sqlite)
    assert source == original
    return result


@pytest.mark.parametrize("backend_name", ["sqlite", "duckdb"])
def test_mean_extracts_aggregates_before_division(monkeypatch, backend_name):
    connection = getattr(ibis, backend_name).connect()
    try:
        connection.create_table("private_table", pl.DataFrame({"x": [1, 2, 3, 4]}))
        plan = make_plan(
            monkeypatch,
            [alias(binary(plugin(SUM, "sum"), plugin(COUNT_ALL, "count")), "mean")],
            backend=connection,
        )
        assert [p.name for p in plan.plugins] == ["__ffi_0", "__ffi_1"]
        assert [p.parameters["symbol"] for p in plan.plugins] == ["sum", "count"]
        assert [p.input_dtype for p in plan.plugins] == [pl.Int64, pl.UInt32]
        assert connection.to_polars(plan.ibis_table).to_dict(as_series=False) == {
            "__ffi_0": [10],
            "__ffi_1": [4],
        }
        assert_frame_equal(
            plan.postprocess(pl.DataFrame({"__ffi_1": [6], "__ffi_0": [16]})),
            pl.DataFrame({"mean": [16 / 6]}),
        )
    finally:
        connection.disconnect()


@pytest.mark.parametrize("backend_name", ["sqlite", "duckdb"])
@pytest.mark.parametrize("values", [[1, None, 3], [], [None, None]])
def test_aggregate_null_and_empty_semantics(monkeypatch, backend_name, values):
    connection = getattr(ibis, backend_name).connect()
    df = pl.DataFrame({"x": values}, schema={"x": pl.Int64})
    try:
        connection.create_table("private_table", df)
        plan = make_plan(
            monkeypatch,
            [
                alias(plugin(SUM), "sum"),
                alias(plugin(COUNT_ALL), "all"),
                alias(plugin(COUNT_NONNULL), "nonnull"),
                alias(plugin("Len"), "len"),
            ],
            backend=connection,
        )
        result = connection.to_polars(plan.ibis_table)
        assert result.row(0) == (
            df["x"].sum(),
            len(df),
            df["x"].count(),
            len(df),
        )
    finally:
        connection.disconnect()


def test_repeated_occurrences_are_not_deduplicated(monkeypatch):
    same = plugin(SUM)
    plan = make_plan(
        monkeypatch,
        [alias(binary(same, same, "Minus"), "difference"), alias(same, "third")],
    )
    assert len(plan.plugins) == 3
    assert len({p.name for p in plan.plugins}) == 3
    assert_frame_equal(
        plan.postprocess(
            pl.DataFrame({"__ffi_0": [10], "__ffi_1": [7], "__ffi_2": [15]})
        ),
        pl.DataFrame({"difference": [3], "third": [15]}),
    )
    plan.plugins[0].parameters["kwargs"][0] = 99
    assert plan.plugins[1].parameters["kwargs"] == [1, 2, 3]


def test_default_output_name_and_public_literal(monkeypatch):
    plan = make_plan(monkeypatch, [binary(plugin(SUM), literal(2), "Plus")])
    assert_frame_equal(
        plan.postprocess(pl.DataFrame({"__ffi_0": [8]})),
        pl.DataFrame({"x": [10]}),
    )


def test_public_literal_types_preserve_values_and_dtypes(monkeypatch):
    literals = [
        ({"Dyn": {"Float": 1.5}}, pl.lit(1.5)),
        ({"Scalar": {"Int16": 7}}, pl.lit(7, dtype=pl.Int16)),
        ({"Scalar": {"UInt32": 8}}, pl.lit(8, dtype=pl.UInt32)),
        ({"Scalar": {"Float32": 0.25}}, pl.lit(0.25, dtype=pl.Float32)),
        ({"Scalar": {"Float64": 0.5}}, pl.lit(0.5, dtype=pl.Float64)),
        ({"Scalar": {"Boolean": True}}, pl.lit(True)),
        ({"Scalar": {"Null": "Null"}}, pl.lit(None)),
    ]
    expressions = [alias(plugin(SUM), "sum")] + [
        alias({"Literal": node}, f"public_{i}") for i, (node, _) in enumerate(literals)
    ]
    plan = make_plan(monkeypatch, expressions)
    noised = pl.DataFrame({"__ffi_0": [11]})
    expected = noised.select(
        pl.col("__ffi_0").alias("sum"),
        *(expr.alias(f"public_{i}") for i, (_, expr) in enumerate(literals)),
    )
    assert_frame_equal(plan.postprocess(noised), expected)


@pytest.mark.parametrize("backend_name", ["sqlite", "duckdb"])
def test_numeric_private_expression_and_strict_aggregate_cast(
    monkeypatch, backend_name
):
    row_expr = binary(
        {
            "Cast": {
                "expr": {"Column": "x"},
                "dtype": {"Literal": "Int64"},
                "options": "Strict",
            }
        },
        literal(1),
        "Plus",
    )
    aggregate = {
        "Cast": {
            "expr": {"Agg": {"Sum": row_expr}},
            "dtype": {"Literal": "Float64"},
            "options": "Strict",
        }
    }
    connection = getattr(ibis, backend_name).connect()
    try:
        connection.create_table("private_table", pl.DataFrame({"x": [1, 2, 3]}))
        plan = make_plan(monkeypatch, [plugin(aggregate)], backend=connection)
        assert plan.plugins[0].input_dtype == pl.Float64
        assert connection.to_polars(plan.ibis_table).item() == 9.0
    finally:
        connection.disconnect()


@pytest.mark.parametrize(
    "expression",
    [
        {"Column": "x"},
        {"Column": "__ffi_0"},
        binary(plugin(SUM), {"Column": "__ffi_0"}, "Plus"),
        {"Agg": {"Sum": plugin(SUM)}},
        {"AnonymousFunction": {"function": "not-loaded", "input": []}},
        {"Function": {"function": "FillNull", "input": [plugin(SUM), literal(0)]}},
        {
            "Cast": {
                "expr": plugin(SUM),
                "dtype": {"Literal": "Int64"},
                "options": "Strict",
            }
        },
        plugin(plugin(SUM)),
        plugin({"Column": "x"}),
        plugin({"Agg": {"Mean": {"Column": "x"}}}),
        plugin({"Agg": {"Sum": {"Agg": {"Sum": {"Column": "x"}}}}}),
        plugin(
            {
                "Agg": {
                    "Sum": {
                        "AnonymousFunction": {"input": [], "function": "not-loaded"}
                    }
                }
            }
        ),
        plugin(
            {
                "Agg": {
                    "Sum": {
                        "Function": {"input": [plugin(SUM)], "function": "FillNull"}
                    }
                }
            }
        ),
        binary(plugin(SUM), literal(2), "UnknownOperator"),
    ],
)
def test_private_or_unsupported_residual_is_rejected(monkeypatch, expression):
    with pytest.raises(NotImplementedError):
        make_plan(monkeypatch, [alias(expression, "result")])


def test_named_private_column_cannot_impersonate_noised_column(monkeypatch):
    with pytest.raises(NotImplementedError, match="postprocessing expression 'Column'"):
        make_plan(
            monkeypatch,
            [alias(plugin(SUM), "sum"), {"Column": "__ffi_0"}],
            schema={"x": pl.Int64, "__ffi_0": pl.Int64},
        )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p["Select"].update(input={"Filter": {"input": p["Select"]["input"]}}),
        lambda p: p.update(GroupBy=p.pop("Select")),
        lambda p: p["Select"]["options"].update(should_broadcast=False),
        lambda p: p["Select"].update(unexpected=True),
        lambda p: p["Select"]["input"]["DataFrameScan"]["schema"].update(
            metadata={"unknown": True}
        ),
    ],
)
def test_non_scalar_or_unknown_plan_is_rejected(monkeypatch, mutate):
    source = serialize(pl.LazyFrame(schema={"x": pl.Int64}).select(pl.lit(0)))
    source["Select"]["expr"] = [plugin(SUM)]
    mutate(source)
    monkeypatch.setattr("polars_to_ibis._split.serialize", lambda _: source)
    with pytest.raises(NotImplementedError):
        split_polars_on_ffi_plan(pl.LazyFrame(), "private_table", ibis.sqlite)


def test_no_plugin_and_duplicate_outputs(monkeypatch):
    with pytest.raises(ValueError, match="Did not find"):
        make_plan(monkeypatch, [literal(1)])
    with pytest.raises(ValueError, match="Duplicate output"):
        make_plan(monkeypatch, [plugin(SUM), plugin(SUM)])


@pytest.mark.parametrize(
    "frame",
    [
        pl.DataFrame({"__ffi_0": [], "__ffi_1": []}),
        pl.DataFrame({"__ffi_0": [1, 2], "__ffi_1": [3, 4]}),
        pl.DataFrame({"__ffi_0": [1]}),
        pl.DataFrame({"__ffi_0": [1], "__ffi_1": [2], "raw_private": [123]}),
    ],
)
def test_postprocess_rejects_wrong_shape(monkeypatch, frame):
    plan = make_plan(monkeypatch, [binary(plugin(SUM), plugin(COUNT_ALL))])
    with pytest.raises(ValueError):
        plan.postprocess(frame)
    with pytest.raises(TypeError):
        plan.postprocess({"__ffi_0": [1], "__ffi_1": [2]})


@pytest.mark.parametrize(
    "dtype", [pl.Int64, pl.UInt32, pl.UInt64, pl.Float32, pl.Float64]
)
def test_input_dtype_comes_from_polars_not_sql(monkeypatch, dtype):
    plan = make_plan(monkeypatch, [plugin(SUM)], schema={"x": dtype})
    assert plan.plugins[0].input_dtype == dtype


def test_large_integer_postprocessing_does_not_narrow(monkeypatch):
    plan = make_plan(monkeypatch, [plugin(SUM)])
    value = 2**40 + 7
    assert plan.postprocess(pl.DataFrame({"__ffi_0": [value]})).item() == value


def test_signed_count_allows_only_safe_nonstrict_widening(monkeypatch):
    cast_count = {
        "Cast": {"expr": "Len", "dtype": {"Literal": "Int64"}, "options": "NonStrict"}
    }
    plan = make_plan(monkeypatch, [plugin(cast_count)])
    assert plan.plugins[0].input_dtype == pl.Int64
    cast_count["Cast"]["dtype"]["Literal"] = "Int8"
    with pytest.raises(NotImplementedError, match="cast options"):
        make_plan(monkeypatch, [plugin(cast_count)])


@pytest.mark.parametrize("numerator,expected", [(16, float("inf")), (0, float("nan"))])
def test_zero_noisy_denominator_preserves_polars_division(
    monkeypatch, numerator, expected
):
    plan = make_plan(
        monkeypatch, [alias(binary(plugin(SUM), plugin(COUNT_ALL)), "mean")]
    )
    result = plan.postprocess(pl.DataFrame({"__ffi_0": [numerator], "__ffi_1": [0]}))
    assert_frame_equal(result, pl.DataFrame({"mean": [expected]}))


def test_explicit_private_compiler_matches_polars_float_schema():
    expr = pl.col("x").fill_nan(1).fill_null(2).clip(0, 5).sum()
    source = pl.LazyFrame(schema={"x": pl.Float32})
    node = serialize(source.select(expr))["Select"]["expr"][0]
    compiled = _aggregate_expr(node)
    assert (
        source.select(compiled).collect_schema() == source.select(expr).collect_schema()
    )
    df = pl.DataFrame({"x": [float("nan"), None, 9.0, -1.0]}, schema={"x": pl.Float32})
    assert_frame_equal(df.select(compiled), df.select(expr))


def test_non_finite_serialized_literal_is_not_treated_as_null(monkeypatch):
    node = {"Literal": {"Dyn": {"Float": None}}}
    with pytest.raises(NotImplementedError, match="finite"):
        make_plan(monkeypatch, [binary(plugin(SUM), node, "Plus")])
