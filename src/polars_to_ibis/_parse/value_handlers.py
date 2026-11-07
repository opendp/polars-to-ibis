"""
This is a private module: The API may change.
"""

import io
import json
import logging
from collections.abc import Callable
from typing import Any

import ibis  # pyright: ignore [reportMissingTypeStubs]
import ibis.expr.types as ir  # pyright: ignore [reportMissingTypeStubs]
import polars as pl
from ibis import _ as defer  # pyright: ignore[reportMissingTypeStubs]
from ibis.common.deferred import Deferred, deferrable

from polars_to_ibis._utils import abbreviate

from . import tags
from .utils import assert_no_extras, indent, outdent, split_tag_payload

logger = logging.getLogger(__name__)

PolarsPlan = dict[str, Any]
ReturnsTable = Callable[..., ir.Table]
ReturnsValue = Callable[..., ir.Value]


# Main:


def polars_expr_to_ibis_value(polars_expr: PolarsPlan) -> ir.Value:
    tag, payload = split_tag_payload(polars_expr)
    try:
        func = VALUE_REGISTRY[tag]
    except KeyError as e:  # pragma: no cover
        raise NotImplementedError(f"No value handler for {tag!r}") from e
    try:
        return func(payload)
    except NotImplementedError as e:  # pragma: no cover
        raise NotImplementedError(f"{e}\nin {abbreviate(polars_expr)}")


# Registry:

VALUE_REGISTRY: dict[str, ReturnsValue] = {}


def value_handler(tag: str) -> Callable[..., ReturnsValue]:
    def deco(func: ReturnsValue) -> ReturnsValue:
        def wrapped_func(*args, **kwargs):
            if logger.isEnabledFor(logging.DEBUG):  # pragma: no cover
                # Just so we don't call abbreviate unless needed.
                logger.debug(
                    f"{indent()}--> polars value {tag}:\n{abbreviate(args[0])} "
                )
            to_return = func(*args, **kwargs)
            if logger.isEnabledFor(logging.DEBUG):  # pragma: no cover
                logger.debug(f"{outdent()}<-- ibis value:\n{to_return}")
            return to_return

        VALUE_REGISTRY[tag] = wrapped_func
        return func

    return deco


# Value Handlers:


@value_handler(tags.value.LEN)
def handle_len(_payload):
    return defer.count().cast("uint32")


@value_handler(tags.value.COUNT)
def handle_count(payload: PolarsPlan):
    match payload:
        case {
            "input": expr,
            "include_nulls": include_nulls,
            **extras_1,
        }:
            assert_no_extras(extras_1)
            value = polars_expr_to_ibis_value(expr)
            return (value.isnull() if include_nulls else value).count().cast("uint32")
        case _:
            raise NotImplementedError(f"Unsupported {tags.value.COUNT}")


_NUMERIC_LITERAL_TYPES = {
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "Float32",
    "Float64",
    "Boolean",
}


@value_handler(tags.value.LITERAL)
def handle_literal(payload: PolarsPlan):
    match payload:
        case {"Scalar": scalar, **extras} if (
            len(scalar) == 1 and next(iter(scalar)) in _NUMERIC_LITERAL_TYPES
        ):
            assert_no_extras(extras)
            dtype, value = next(iter(scalar.items()))
            return ibis.literal(value, type=dtype.lower())
        case {"Dyn": {"Float": value, **extras_1}, **extras_2}:
            assert_no_extras(extras_1, extras_2)
            # Ibis was returning a Decimal: This forces float64.
            # Hack suggested by https://github.com/ibis-project/ibis/issues/11947
            return ibis.literal(0).cast("float64") + value
        case {"Dyn": {"Int": value, **extras_1}, **extras_2}:
            # Keep a value expression, including for literal-only preprocessing
            # such as FillNull; plain Python ints have no expression methods.
            dtype = "int32" if -(2**31) <= value < 2**31 else "int64"
            return ibis.literal(value, type=dtype)
        case {"Scalar": {"String": value, **extras_1}, **extras_2}:
            assert_no_extras(extras_1, extras_2)
            return ibis.literal(value)  # pyright: ignore[reportUnknownMemberType]
        case {"Scalar": {"Null": dtype, **extras_1}, **extras_2}:
            assert_no_extras(extras_1, extras_2)
            return (
                ibis.null()
                if dtype == "Null"
                else ibis.literal(None, type=dtype.lower())
            )
        case _:
            raise NotImplementedError(f"Unsupported {tags.value.LITERAL}")


@value_handler(tags.value.COLUMN)
def handle_column(payload: PolarsPlan):
    return defer[payload]  # pyright: ignore[reportArgumentType]


@value_handler(tags.value.ALIAS)
def handle_alias(payload: PolarsPlan):
    value = polars_expr_to_ibis_value(payload[0])
    if not isinstance(value, (ir.Value, Deferred)):
        value = ibis.literal(value)
    return value.name(payload[1])


@value_handler(tags.value.CAST)
def handle_cast(payload: PolarsPlan) -> ir.Value:
    match payload:
        case {
            "dtype": {tags.value.LITERAL: dtype_literal, **extras_1},
            "expr": expr,
            "options": options,
            **extras_2,
        }:
            assert_no_extras(extras_1, extras_2)
            if options not in ("Strict", "NonStrict"):
                raise NotImplementedError(f"Unsupported cast options: {options}")
            return _polars_cast(
                polars_expr_to_ibis_value(expr), dtype_literal.lower(), options
            )
        case _:
            raise NotImplementedError(f"Unsupported {tags.value.CAST}")


@ibis.udf.scalar.builtin(name="trunc")
def _truncate(value: float) -> float:
    """SQL TRUNC without Ibis floor/ceil's intermediate signed-integer cast."""


@deferrable
def _polars_cast(value, target, options):
    source, target = value.type(), ibis.dtype(target)
    safe = source == target
    if source.is_integer() and target.is_integer():
        safe = (
            target.bounds.lower <= source.bounds.lower
            and source.bounds.upper <= target.bounds.upper
        )
    if source.is_floating() and target.is_integer():
        # Polars truncates toward zero; SQL numeric casts may round instead.
        value = _truncate(value)
    if options == "NonStrict" and not safe:
        return value.try_cast(target)
    return value.cast(target)


@deferrable
def _polars_sum(value):
    dtype = value.type()
    # Polars only widens the small integer sums. SQL SUM commonly widens all
    # integers and returns null on empty input; neither matches native heads.
    output = (
        "uint32"
        if dtype.is_boolean()
        else "int64"
        if dtype.is_integer() and dtype.nbytes < 4
        else dtype
    )
    return value.sum().fill_null(ibis.literal(0, type=output)).cast(output)


@value_handler(tags.value.SUM)
def handle_sum(payload: PolarsPlan):
    return _polars_sum(polars_expr_to_ibis_value(payload))


@value_handler(tags.value.MEAN)
def handle_mean(payload: PolarsPlan):
    return polars_expr_to_ibis_value(payload).cast("float64").mean()


@value_handler(tags.value.MEDIAN)
def handle_median(payload: PolarsPlan):
    return polars_expr_to_ibis_value(payload).median()


@value_handler(tags.value.MAX)
def handle_max(payload: PolarsPlan):
    match payload:
        case {"input": expr, "propagate_nans": False, **extras}:
            assert_no_extras(extras)
            return polars_expr_to_ibis_value(expr).max()
        case _:
            raise NotImplementedError(f"Unsupported {tags.value.MAX}")


@value_handler(tags.value.MIN)
def handle_min(payload: PolarsPlan):
    match payload:
        case {"input": expr, "propagate_nans": False, **extras}:
            assert_no_extras(extras)
            return polars_expr_to_ibis_value(expr).min()
        case _:
            raise NotImplementedError(f"Unsupported {tags.value.MIN}")


@value_handler(tags.value.VAR)
def handle_var(payload: PolarsPlan):
    match payload:
        case [expr, 1]:
            return polars_expr_to_ibis_value(expr).cast("float64").var()
        case _:
            raise NotImplementedError(f"Unsupported {tags.value.VAR}")


@value_handler(tags.value.STD)
def handle_std(payload: PolarsPlan):
    match payload:
        case [expr, 1]:
            return polars_expr_to_ibis_value(expr).cast("float64").std()
        case _:
            raise NotImplementedError(f"Unsupported {tags.value.STD}")


@value_handler(tags.value.QUANTILE)
def handle_quantile(payload: PolarsPlan):
    match payload:
        case {
            "expr": expr,
            "method": "Nearest",
            "quantile": {
                tags.value.LITERAL: {
                    "Dyn": {"Float": quantile, **extras_1},
                    **extras_2,
                },
                **extras_3,
            },
            **extras_4,
        }:
            assert_no_extras(extras_1, extras_2, extras_3, extras_4)
            return polars_expr_to_ibis_value(expr).quantile(quantile)
        case _:
            raise NotImplementedError(f"Unsupported {tags.value.QUANTILE}")


@value_handler(tags.value.AGG)
def handle_agg(payload: PolarsPlan):
    return polars_expr_to_ibis_value(payload)


@value_handler(tags.value.FUNCTION)
def handle_function(payload: PolarsPlan) -> ir.Value:
    match payload:
        case {
            "function": {"Trigonometry": "Degrees", **extras_1},
            "input": [expr],
            **extras_2,
        }:
            return polars_expr_to_ibis_value(expr).degrees()
        case {
            "function": "Log",
            "input": [expr, base_expr],
            **extras_1,
        }:
            assert_no_extras(extras_1)
            base = polars_expr_to_ibis_value(base_expr)
            return polars_expr_to_ibis_value(expr).log(base)
        case {
            "function": {
                # Ibis does not support other rounding modes.
                "Round": {"decimals": decimals, "mode": mode, **extras_1},
                **extras_2,
            },
            "input": [expr],
            **extras_3,
        }:
            assert_no_extras(extras_1, extras_2, extras_3)
            default = "HalfToEven"
            if mode != default:
                raise NotImplementedError(
                    f"Unsupported round mode: {mode}. "
                    f"Only the default ({default}) is supported."
                )
            return polars_expr_to_ibis_value(expr).round(decimals)
        case {
            "function": {"Pow": "Generic", **extras_1},
            "input": [left_expr, right_expr],
            **extras_2,
        }:
            assert_no_extras(extras_1, extras_2)
            left = polars_expr_to_ibis_value(left_expr)
            right = polars_expr_to_ibis_value(right_expr)
            return _apply_binary(pow, left, right, [left_expr, right_expr])
        case {
            "function": {"Boolean": "Not", **extras_1},
            "input": [input_expr],
            **extras_2,
        }:
            assert_no_extras(extras_1, extras_2)
            return ~polars_expr_to_ibis_value(input_expr)  # type: ignore
        case {
            "function": "Negate",
            "input": [input_expr],
            **extras,
        }:
            assert_no_extras(extras)
            return -polars_expr_to_ibis_value(input_expr)  # type: ignore
        case {
            "function": {
                "Clip": {"has_min": True, "has_max": True, **extras_1},
                **extras_2,
            },
            "input": [input_expr, lower_expr, upper_expr],
            **extras_3,
        }:
            assert_no_extras(extras_1, extras_2, extras_3)
            return _clip(
                polars_expr_to_ibis_value(input_expr),
                polars_expr_to_ibis_value(lower_expr),
                polars_expr_to_ibis_value(upper_expr),
            )
        case {  # pragma: no cover (polars<1.41.2)
            "function": {
                tags.value.QUANTILE: {"method": "Nearest", **extras_1},
                **extras_2,
            },
            "input": [
                input_expr,
                _quantile_expr,
            ],
            **extras_3,
        }:
            assert_no_extras(extras_1, extras_2, extras_3)
            raise NotImplementedError(f"Unsupported {tags.value.FUNCTION} Quantile")
        case {
            "function": "FillNull",
            "input": [input_expr, fill_expr],
            **extras_1,
        }:
            assert_no_extras(extras_1)
            fill_value = polars_expr_to_ibis_value(fill_expr)
            return _fill_null(
                polars_expr_to_ibis_value(input_expr),
                fill_value,
                [input_expr, fill_expr],
            )
        case {
            "function": {"Boolean": "IsNotNan" | "IsNan" as op, **extras_1},
            "input": [input_expr],
            **extras_2,
        }:
            assert_no_extras(extras_1, extras_2)
            value = polars_expr_to_ibis_value(input_expr).isnan()
            return ~value if op == "IsNotNan" else value
        case {
            "function": {"Boolean": "IsNull" | "IsNotNull" as op, **extras_1},
            "input": [input_expr],
            **extras_2,
        }:
            assert_no_extras(extras_1, extras_2)
            value = polars_expr_to_ibis_value(input_expr)
            return value.isnull() if op == "IsNull" else value.notnull()
        case _:
            raise NotImplementedError(f"Unsupported {tags.value.FUNCTION}")


@value_handler(tags.value.TERNARY)
def handle_ternary(payload: PolarsPlan):
    match payload:
        case {
            "predicate": predicate_expr,
            "truthy": truthy_expr,
            "falsy": falsy_expr,
            **extras,
        }:
            assert_no_extras(extras)
            return _if_else(
                polars_expr_to_ibis_value(predicate_expr),
                polars_expr_to_ibis_value(truthy_expr),
                polars_expr_to_ibis_value(falsy_expr),
                [truthy_expr, falsy_expr],
            )
        case _:
            raise NotImplementedError(f"Unsupported {tags.value.TERNARY}")


def _argument_context(values, expressions):
    schema, children = {}, []
    for index, (value, expression) in enumerate(zip(values, expressions)):
        child = pl.Expr.deserialize(io.StringIO(json.dumps(expression)), format="json")
        if child.meta.root_names():
            name = f"arg_{index}"
            schema[name] = value.type().to_polars()
            child = pl.col(name)
        children.append(child)
    return pl.LazyFrame(schema=schema), children


def _coerce_arguments(values, expressions):
    """Ask Polars for contextual literal coercion, without evaluating any data.

    Ibis literals have strong dtypes; Polars' dynamic literals adopt their
    context. Resolve that distinction at each operation, before arithmetic or
    comparisons, rather than casting the final result after precision is lost.
    """
    frame, children = _argument_context(values, expressions)
    dtype = frame.select(pl.coalesce(children).alias("value")).collect_schema()["value"]
    target = str(dtype).lower()
    if dtype.is_numeric() or dtype == pl.Boolean:
        return [value.cast(target) for value in values]
    return values


@deferrable
def _fill_null(value, replacement, expressions):
    value, replacement = _coerce_arguments([value, replacement], expressions)
    return value.fill_null(replacement)


@deferrable
def _if_else(predicate, truthy, falsy, expressions):
    truthy, falsy = _coerce_arguments([truthy, falsy], expressions)
    return predicate.ifelse(truthy, falsy)


@deferrable
def _clip(value, lower, upper):
    # Unlike fill/arithmetic, Polars casts clip bounds to the input dtype.
    return value.clip(lower.cast(value.type()), upper.cast(value.type()))


@deferrable
def _apply_binary(func, left, right, expressions):
    frame, children = _argument_context([left, right], expressions)
    dtype = frame.select(func(*children).alias("value")).collect_schema()["value"]
    left, right = _coerce_arguments([left, right], expressions)
    result = func(left, right)
    # SQL operations such as POWER may widen Float32 despite Ibis' inferred
    # dtype. Force a real SQL cast (a same-type Ibis cast would be optimized out).
    return result.cast("float64").cast("float32") if dtype == pl.Float32 else result


@value_handler(tags.value.BINARY_EXPR)
def handle_binary_expr(payload: PolarsPlan):
    match payload:
        case {"left": left, "op": op, "right": right, **extras}:
            assert_no_extras(extras)
            from operator import (
                __and__,
                __or__,
                add,
                eq,
                floordiv,
                ge,
                gt,
                le,
                lt,
                mod,
                mul,
                ne,
                sub,
                truediv,
            )

            func = {
                "Plus": add,
                "Minus": sub,
                "Multiply": mul,
                # I think "Divide" is only used when serializing Polars SQL.
                # Python expr serializations use FloorDivide and TrueDivide.
                # (There is no plain "div" operation in Python.)
                "FloorDivide": floordiv,
                "TrueDivide": truediv,
                "Divide": truediv,
                "Modulus": mod,
                "NotEq": ne,
                "Eq": eq,
                "Gt": gt,
                "GtEq": ge,
                "Lt": lt,
                "LtEq": le,
                "And": __and__,
                "Or": __or__,
            }[op]
            return _apply_binary(
                func,
                polars_expr_to_ibis_value(left),
                polars_expr_to_ibis_value(right),
                [left, right],
            )
        case _:
            raise NotImplementedError(f"Unsupported {tags.value.BINARY_EXPR}")
