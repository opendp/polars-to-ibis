"""Separate scalar FFI aggregates from their public postprocessing."""

import math
import operator
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, cast

import ibis  # pyright: ignore [reportMissingTypeStubs]
import polars as pl

from ._parse.value_handlers import polars_expr_to_ibis_value
from ._serialize import serialize


@dataclass(frozen=True)
class FfiPlugin:
    """One FFI occurrence, in expression traversal order.

    ``name`` identifies its SQL aggregate and the corresponding noised column.
    ``parameters`` is an opaque copy of the original plugin metadata. The
    consumer must validate it and apply the plugin itself. ``input_dtype`` is
    the original Polars aggregate's dtype, which need not match the SQL result
    dtype (for example, Polars ``len`` is UInt32).
    """

    name: str
    parameters: dict[str, Any]
    input_dtype: pl.DataType


@dataclass(frozen=True)
class FfiSplitPlan:
    """A scalar SQL aggregation followed by postprocessing of noised values.

    This object does not execute SQL, load plugins, or add noise. The consumer
    must apply the appropriate plugin once to each aggregate, and pass only
    those noised values to :meth:`postprocess`. Plugin metadata is not a
    privacy guarantee; validation and privacy accounting belong to the
    consumer.
    """

    ibis_table: ibis.Table
    plugins: tuple[FfiPlugin, ...]
    _expressions: tuple[pl.Expr, ...] = field(repr=False)

    def postprocess(self, noisy_values: pl.DataFrame) -> pl.DataFrame:
        """Evaluate the residual expression on one row of noised aggregates.

        All plugin names must be present, with no additional columns. Column
        order may vary. This method cannot determine whether the supplied
        values have actually been noised.
        """
        if not isinstance(noisy_values, pl.DataFrame):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("Expected a Polars DataFrame of noised aggregates")
        if noisy_values.height != 1:
            raise ValueError("Expected exactly one row of noised aggregates")
        names = {plugin.name for plugin in self.plugins}
        if set(noisy_values.columns) != names:
            raise ValueError("Expected exactly the plugin columns of this split plan")
        return noisy_values.select(self._expressions)


_DTYPES = {
    name: dtype
    for name, dtype in (
        ("Int8", pl.Int8),
        ("Int16", pl.Int16),
        ("Int32", pl.Int32),
        ("Int64", pl.Int64),
        ("UInt8", pl.UInt8),
        ("UInt16", pl.UInt16),
        ("UInt32", pl.UInt32),
        ("UInt64", pl.UInt64),
        ("Float32", pl.Float32),
        ("Float64", pl.Float64),
        ("Boolean", pl.Boolean),
        ("String", pl.String),
        ("Binary", pl.Binary),
    )
}

_BINARY_OPERATORS: dict[str, Callable[[pl.Expr, pl.Expr], pl.Expr]] = {
    "Plus": operator.add,
    "Minus": operator.sub,
    "Multiply": operator.mul,
    "TrueDivide": lambda left, right: left / right,
    "FloorDivide": lambda left, right: left // right,
    "Modulus": operator.mod,
    "Eq": operator.eq,
    "NotEq": operator.ne,
    "Gt": operator.gt,
    "GtEq": operator.ge,
    "Lt": operator.lt,
    "LtEq": operator.le,
    "And": lambda left, right: left & right,
    "Or": lambda left, right: left | right,
}


def _tag(node: Any) -> tuple[str, Any]:
    if node == "Len":
        return "Len", None
    if not isinstance(node, dict):
        raise NotImplementedError("Expected a single tagged scalar expression")
    node = cast(dict[str, Any], node)
    if len(node) != 1:
        raise NotImplementedError("Expected a single tagged scalar expression")
    return next(iter(node.items()))


def _keys(value: Any, expected: set[str]) -> None:
    if not isinstance(value, dict) or set(cast(dict[str, Any], value)) != expected:
        raise NotImplementedError("Unsupported scalar expression parameters")


def _literal(payload: Any) -> pl.Expr:
    tag, value = _tag(payload)
    dtype_name, scalar = _tag(value)
    if tag == "Dyn" and dtype_name == "Int" and type(scalar) is int:
        return pl.lit(scalar)
    if tag == "Dyn" and dtype_name == "Float":
        if type(scalar) not in (int, float) or not math.isfinite(scalar):
            raise NotImplementedError("Only finite floating literals are supported")
        return pl.lit(float(scalar))
    if tag == "Scalar":
        if dtype_name == "Null" and scalar == "Null":
            return pl.lit(None)
        if dtype_name == "Boolean" and type(scalar) is bool:
            return pl.lit(scalar)
        if (
            dtype_name.startswith(("Int", "UInt"))
            and dtype_name in _DTYPES
            and type(scalar) is int
        ):
            return pl.lit(scalar, dtype=_DTYPES[dtype_name])
        if (
            dtype_name in ("Float32", "Float64")
            and type(scalar) in (int, float)
            and math.isfinite(scalar)
        ):
            return pl.lit(scalar, dtype=_DTYPES[dtype_name])
    raise NotImplementedError("Only numeric, boolean, and null literals are supported")


def _binary(payload: Any, compile_expr: Callable[[Any], pl.Expr]) -> pl.Expr:
    _keys(payload, {"left", "op", "right"})
    operation = _BINARY_OPERATORS.get(payload["op"])
    if operation is None:
        raise NotImplementedError(f"Unsupported scalar operator {payload['op']!r}")
    return operation(compile_expr(payload["left"]), compile_expr(payload["right"]))


def _cast(
    payload: Any,
    compile_expr: Callable[[Any], pl.Expr],
    *,
    allow_count_widening: bool = False,
) -> pl.Expr:
    _keys(payload, {"expr", "dtype", "options"})
    tag, name = _tag(payload["dtype"])
    if tag != "Literal" or not isinstance(name, str) or name not in _DTYPES:
        raise NotImplementedError("Unsupported aggregate cast dtype")
    if payload["options"] == "NonStrict" and allow_count_widening:
        child_tag, child = _tag(payload["expr"])
        is_count = child_tag == "Len" or (
            child_tag == "Agg" and _tag(child)[0] == "Count"
        )
        # OpenDP signed counts widen the native UInt32 cardinality. This cast
        # cannot fail; arbitrary NonStrict SQL casts need different semantics.
        if is_count and name == "Int64" and pl.get_index_type() == pl.UInt32:
            return compile_expr(payload["expr"]).cast(pl.Int64)
    if payload["options"] != "Strict":
        raise NotImplementedError("Unsupported aggregate cast options")
    return compile_expr(payload["expr"]).cast(_DTYPES[name], strict=True)


def _row_expr(node: Any) -> pl.Expr:
    """Build only data-only row expressions, for schema inference, not execution."""
    tag, payload = _tag(node)
    if tag == "Column" and isinstance(payload, str):
        return pl.col(payload)
    if tag == "Literal":
        return _literal(payload)
    if tag == "BinaryExpr":
        return _binary(payload, _row_expr)
    if tag == "Cast":
        return _cast(payload, _row_expr)
    if tag == "Ternary":
        _keys(payload, {"predicate", "truthy", "falsy"})
        return (
            pl.when(_row_expr(payload["predicate"]))
            .then(_row_expr(payload["truthy"]))
            .otherwise(_row_expr(payload["falsy"]))
        )
    if tag == "Function":
        _keys(payload, {"input", "function"})
        inputs = payload["input"]
        if not isinstance(inputs, list):
            raise NotImplementedError("Expected row function inputs")
        inputs = cast(list[Any], inputs)
        function = payload["function"]
        if function == "FillNull" and len(inputs) == 2:
            return _row_expr(inputs[0]).fill_null(_row_expr(inputs[1]))
        if (
            function == {"Clip": {"has_min": True, "has_max": True}}
            and len(inputs) == 3
        ):
            return _row_expr(inputs[0]).clip(_row_expr(inputs[1]), _row_expr(inputs[2]))
        if isinstance(function, dict) and len(inputs) == 1:
            function = cast(dict[str, Any], function)
            boolean_functions = {
                "IsNan": pl.Expr.is_nan,
                "IsNotNan": pl.Expr.is_not_nan,
                "IsNull": pl.Expr.is_null,
                "IsNotNull": pl.Expr.is_not_null,
            }
            if set(function) == {"Boolean"}:
                operation = boolean_functions.get(function["Boolean"])
                if operation is not None:
                    return operation(_row_expr(inputs[0]))
    raise NotImplementedError(f"Unsupported private row expression {tag!r}")


def _aggregate_expr(node: Any) -> pl.Expr:
    tag, payload = _tag(node)
    if tag == "Len":
        return pl.len()
    if tag == "Cast":
        return _cast(payload, _aggregate_expr, allow_count_widening=True)
    if tag == "Agg":
        operation, child = _tag(payload)
        if operation == "Sum":
            return _row_expr(child).sum()
        if operation == "Count":
            _keys(child, {"input", "include_nulls"})
            if type(child["include_nulls"]) is not bool:
                raise NotImplementedError("Expected boolean include_nulls")
            expr = _row_expr(child["input"])
            return expr.len() if child["include_nulls"] else expr.count()
    raise NotImplementedError("Each FFI input must be a supported scalar aggregate")


def _ibis_aggregate(node: Any, table: ibis.Table) -> Any:
    tag, payload = _tag(node)
    if tag == "Len":
        return table.count()
    if tag == "Agg":
        operation, child = _tag(payload)
        if operation == "Count" and child["include_nulls"]:
            return table.count()
        if operation == "Sum":
            # Polars sum is zero for empty/all-null input; SQL SUM is NULL.
            return polars_expr_to_ibis_value(node).fill_null(
                ibis.literal(0)  # pyright: ignore[reportUnknownMemberType]
            )
    if tag == "Cast":
        return _ibis_aggregate(payload["expr"], table).cast(
            payload["dtype"]["Literal"].lower()
        )
    return polars_expr_to_ibis_value(node)


class _Splitter:
    def __init__(self, schema: pl.Schema):
        self.schema = schema
        self.plugins: list[FfiPlugin] = []
        self.aggregates: list[Any] = []

    def residual(self, node: Any) -> tuple[pl.Expr, str]:
        tag, payload = _tag(node)
        if tag == "Alias":
            if not isinstance(payload, list):
                raise NotImplementedError("Expected alias expression and name")
            payload = cast(list[Any], payload)
            if len(payload) != 2:
                raise NotImplementedError("Expected alias expression and name")
            if not isinstance(payload[1], str):
                raise NotImplementedError("Expected a string alias")
            expr, _ = self.residual(payload[0])
            return expr.alias(payload[1]), payload[1]
        if tag == "Literal":
            expr = _literal(payload)
            return expr, expr.meta.output_name()
        if tag == "BinaryExpr":
            _keys(payload, {"left", "op", "right"})
            operation = _BINARY_OPERATORS.get(payload["op"])
            if operation is None:
                raise NotImplementedError(
                    f"Unsupported scalar operator {payload['op']!r}"
                )
            left, name = self.residual(payload["left"])
            right, _ = self.residual(payload["right"])
            return operation(left, right), name
        if tag == "Function":
            _keys(payload, {"input", "function"})
            function = payload["function"]
            _keys(function, {"FfiPlugin"})
            parameters = function["FfiPlugin"]
            if not isinstance(parameters, dict):
                raise NotImplementedError("Expected an FFI parameter dictionary")
            parameters = cast(dict[str, Any], parameters)
            inputs = payload["input"]
            if not isinstance(inputs, list):
                raise NotImplementedError("Each FFI must have one aggregate input")
            inputs = cast(list[Any], inputs)
            if len(inputs) != 1:
                raise NotImplementedError("Each FFI must have one aggregate input")
            child = inputs[0]
            aggregate = _aggregate_expr(child)
            # This only resolves types against an empty schema. It neither
            # executes the original query nor loads the plugin being split.
            input_dtype = (
                pl.LazyFrame(schema=self.schema)
                .select(aggregate)
                .collect_schema()
                .dtypes()[0]
            )
            name = f"__ffi_{len(self.plugins)}"
            self.plugins.append(FfiPlugin(name, deepcopy(parameters), input_dtype))
            self.aggregates.append(deepcopy(child))
            return pl.col(name), aggregate.meta.output_name()
        # In particular, original Column nodes never become references to the
        # noised values, even when named exactly like a generated SQL alias.
        raise NotImplementedError(f"Unsupported postprocessing expression {tag!r}")


def split_polars_on_ffi_plan(
    query: pl.LazyFrame,
    table_name: str,
    backend: ibis.BaseBackend,
) -> FfiSplitPlan:
    """Split scalar FFI aggregates from supported public postprocessing.

    Unlike :func:`polars_to_ibis.split_polars_on_ffi`, this separates each
    aggregate occurrence into its own SQL column before rebuilding arithmetic
    such as a mean. Only a single Select over a DataFrameScan is supported.
    The residual accepts aliases, numeric/boolean/null literals, and basic
    binary arithmetic and comparisons. Raw columns, other functions, nested
    plugins, grouping and multi-stage plans are rejected.

    The backend parameter is retained for consistency with the existing split
    API. It does not cause database execution. Backend support for SQL-stage
    operations (including floating IsNan expressions) is still required.
    """
    plan = serialize(query)
    _keys(plan, {"Select"})
    select = plan["Select"]
    _keys(select, {"input", "expr", "options"})
    if select["options"] != {
        "run_parallel": True,
        "duplicate_check": True,
        "should_broadcast": True,
    }:
        raise NotImplementedError("Unsupported scalar Select options")
    source_tag, source = _tag(select["input"])
    if source_tag == "IR":
        _keys(source, {"dsl", "version"})
        if type(source["version"]) is not int:
            raise NotImplementedError("Unsupported input IR version")
        source_tag, source = _tag(source["dsl"])
    if source_tag != "DataFrameScan":
        raise NotImplementedError("Expected a scalar Select over one DataFrameScan")
    _keys(source, {"df", "schema"})
    schema = source["schema"]
    if not isinstance(schema, dict):
        raise NotImplementedError("Unsupported input schema for scalar FFI splitting")
    schema = cast(dict[str, Any], schema)
    if schema.get("metadata", None) is None:
        schema = {key: value for key, value in schema.items() if key != "metadata"}
    _keys(schema, {"fields"})
    fields = schema["fields"]
    if not isinstance(fields, dict):
        raise NotImplementedError("Unsupported input schema for scalar FFI splitting")
    fields = cast(dict[str, Any], fields)
    if any(
        not isinstance(name, str)  # pyright: ignore[reportUnnecessaryIsInstance]
        or not isinstance(dtype, str)
        or dtype not in _DTYPES
        for name, dtype in fields.items()
    ):
        raise NotImplementedError("Unsupported input schema for scalar FFI splitting")
    source_schema = pl.Schema({name: _DTYPES[dtype] for name, dtype in fields.items()})
    expressions: list[Any] = select["expr"]
    if not isinstance(expressions, list) or not expressions:  # pyright: ignore[reportUnnecessaryIsInstance]
        raise NotImplementedError("Expected scalar Select expressions")
    splitter = _Splitter(source_schema)
    residuals = [splitter.residual(expr) for expr in expressions]
    if not splitter.plugins:
        raise ValueError("Did not find an FFI aggregate")
    names = [name for _, name in residuals]
    if len(set(names)) != len(names):
        raise ValueError("Duplicate output names in scalar FFI split plan")
    # Schema resolution catches incompatible residual operations without
    # running either a plugin or the original private query.
    compiled = tuple(expr.alias(name) for expr, name in residuals)
    noisy_schema = pl.Schema(
        {plugin.name: plugin.input_dtype for plugin in splitter.plugins}
    )
    pl.LazyFrame(schema=noisy_schema).select(compiled).collect_schema()
    table = ibis.table(ibis.Schema.from_polars(source_schema), name=table_name)  # pyright: ignore[reportUnknownMemberType]
    ibis_table = table.aggregate(
        **{
            plugin.name: _ibis_aggregate(child, table)
            for plugin, child in zip(splitter.plugins, splitter.aggregates)
        }
    )
    return FfiSplitPlan(ibis_table, tuple(splitter.plugins), compiled)
