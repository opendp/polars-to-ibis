"""
This is a private module: The API may change.
"""

from typing import Any, Callable

import ibis  # pyright: ignore [reportMissingTypeStubs]
import ibis.expr.types as ir  # pyright: ignore [reportMissingTypeStubs]
from ibis import _ as defer  # pyright: ignore[reportMissingTypeStubs]

from .._utils import abbreviate
from .utils import assert_no_extras, split_tag_payload

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
        VALUE_REGISTRY[tag] = func
        return func

    return deco


# Value Handlers:


@value_handler("Literal")
def handle_literal(payload: PolarsPlan):
    match payload:
        case (
            {"Dyn": {"Int": value, **extras_1}, **extras_2}
            | {"Dyn": {"Float": value, **extras_1}, **extras_2}
            | {"Scalar": {"Boolean": value, **extras_1}, **extras_2}
            | {"Scalar": {"Float32": value, **extras_1}, **extras_2}
        ):
            assert_no_extras(extras_1, extras_2)
            return value
        case {"Scalar": {"String": value, **extras_1}, **extras_2}:
            assert_no_extras(extras_1, extras_2)
            return ibis.literal(value)  # pyright: ignore[reportUnknownMemberType]
        case _:  # pragma: no cover
            raise NotImplementedError("Unsupported Literal")


@value_handler("Column")
def handle_column(payload: PolarsPlan):
    return defer[payload]  # pyright: ignore[reportArgumentType]


@value_handler("Cast")
def handle_cast(payload: PolarsPlan) -> ir.Value:
    match payload:  # pragma: no cover (Only for polars==1.36.1)
        case {
            "dtype": {"Literal": dtype_literal, **extras_1},
            "expr": expr,
            "options": "Strict",
            **extras_2,
        }:
            assert_no_extras(extras_1, extras_2)
            return ibis.literal(polars_expr_to_ibis_value(expr)).cast(  # type: ignore
                dtype_literal.lower()
            )
        case _:  # pragma: no cover
            raise NotImplementedError("Unsupported Cast")


@value_handler("Sum")
def handle_sum(payload: PolarsPlan):
    return polars_expr_to_ibis_value(payload).sum()


@value_handler("Agg")
def handle_agg(payload: PolarsPlan):
    match payload:
        case {"Mean": {"Column": column, **extras_1}, **extras_2}:
            assert_no_extras(extras_1, extras_2)
            return defer[column].mean()
        case {"Median": {"Column": column, **extras_1}, **extras_2}:
            assert_no_extras(extras_1, extras_2)
            return defer[column].median()
        case {"Sum": {"Column": column, **extras_1}, **extras_2}:
            assert_no_extras(extras_1, extras_2)
            return defer[column].sum()
        case {
            "Min": {
                "input": {"Column": column, **extras_1},
                "propagate_nans": False,
                **extras_2,
            },
            **extras_3,
        }:
            assert_no_extras(extras_1, extras_2, extras_3)
            return defer[column].min()
        case {
            "Max": {
                "input": {"Column": column, **extras_1},
                "propagate_nans": False,
                **extras_2,
            },
            **extras_3,
        }:
            assert_no_extras(extras_1, extras_2, extras_3)
            return defer[column].max()
        case {"Var": [{"Column": column, **extras_1}, 1], **extras_2}:
            assert_no_extras(extras_1, extras_2)
            return defer[column].var()
        case {"Std": [{"Column": column, **extras_1}, 1], **extras_2}:
            assert_no_extras(extras_1, extras_2)
            return defer[column].std()
        case {  # pragma: no cover (polars>=1.41.2)
            "Quantile": {
                "expr": {"Column": column, **extras_1},
                "method": "Nearest",
                "quantile": {
                    "Literal": {"Dyn": {"Float": quantile, **extras_2}, **extras_3},
                    **extras_4,
                },
                **extras_5,
            },
            **extras_6,
        }:
            assert_no_extras(extras_1, extras_2, extras_3, extras_4, extras_5, extras_6)
            return defer[column].quantile(quantile)
        case _:  # pragma: no cover
            raise NotImplementedError("Unsupported Agg")


@value_handler("Function")
def handle_function(payload: PolarsPlan) -> ir.Value:
    match payload:
        case {
            "input": [left_expr, right_expr],
            "function": {"Pow": "Generic", **extras_1},
            **extras_2,
        }:
            assert_no_extras(extras_1, extras_2)
            left = polars_expr_to_ibis_value(left_expr)
            right = polars_expr_to_ibis_value(right_expr)
            return left**right  # type: ignore
        case {
            "input": [input_expr],
            "function": {"Boolean": "Not", **extras_1},
            **extras_2,
        }:
            assert_no_extras(extras_1, extras_2)
            return ~polars_expr_to_ibis_value(input_expr)  # type: ignore
        case {"input": [input_expr], "function": "Negate", **extras}:
            assert_no_extras(extras)
            return -polars_expr_to_ibis_value(input_expr)  # type: ignore
        case {
            "input": [input_expr, lower_expr, upper_expr],
            "function": {
                "Clip": {"has_min": True, "has_max": True, **extras_1},
                **extras_2,
            },
            **extras_3,
        }:
            assert_no_extras(extras_1, extras_2, extras_3)
            lower = polars_expr_to_ibis_value(lower_expr)
            upper = polars_expr_to_ibis_value(upper_expr)
            return polars_expr_to_ibis_value(input_expr).clip(lower, upper)  # type: ignore
        case {  # pragma: no cover (polars<1.41.2)
            "input": [
                input_expr,
                _quantile_expr,  # noqa: F841 (unused)
            ],
            "function": {"Quantile": {"method": "Nearest", **extras_1}, **extras_2},
            **extras_3,
        }:
            assert_no_extras(extras_1, extras_2, extras_3)
            raise NotImplementedError("Unsupported Function Quantile")
        case _:  # pragma: no cover
            raise NotImplementedError("Unsupported Function")


@value_handler("BinaryExpr")
def handle_binary_expr(payload: PolarsPlan):
    match payload:
        case {"left": left, "op": op, "right": right, **extras}:
            assert_no_extras(extras)
            from operator import (
                __and__,
                __or__,
                add,
                eq,
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
                "TrueDivide": truediv,
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
            return func(
                polars_expr_to_ibis_value(left), polars_expr_to_ibis_value(right)
            )
            # return polars_expr_to_ibis_value(left) + polars_expr_to_ibis_value(right)
        case _:  # pragma: no cover
            raise NotImplementedError("Unsupported BinaryExpr")
