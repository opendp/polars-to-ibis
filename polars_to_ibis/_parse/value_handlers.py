"""
This is a private module: The API may change.
"""

from pprint import pformat
from typing import Any, Callable

import ibis  # pyright: ignore [reportMissingTypeStubs]
import ibis.expr.types as ir  # pyright: ignore [reportMissingTypeStubs]
from ibis import _ as defer  # pyright: ignore[reportMissingTypeStubs]

from .._serialize import replace
from .utils import assert_no_extras, split_tag_payload

PolarsPlan = dict[str, Any]
NamedValue = tuple[str, ir.Value]
ReturnsTable = Callable[..., ir.Table]
ReturnsValue = Callable[..., NamedValue]


# Main:


def polars_expr_to_ibis_value(polars_expr: PolarsPlan) -> NamedValue:
    tag, payload = split_tag_payload(polars_expr)
    try:
        func = VALUE_REGISTRY[tag]
    except KeyError as e:  # pragma: no cover
        raise NotImplementedError(f"No value handler for {tag!r}") from e
    try:
        return func(payload)
    except NotImplementedError as e:  # pragma: no cover
        replace(polars_expr, "DataFrameScan", lambda _: "...")
        raise NotImplementedError(f"{e}:\n{pformat(polars_expr)}")


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
            {"Dyn": {"Int": value}, **extras}
            | {"Dyn": {"Float": value}, **extras}
            | {"Scalar": {"Boolean": value}, **extras}
        ):
            assert_no_extras(extras)
            return value
        case {"Scalar": {"String": value}, **extras}:
            assert_no_extras(extras)
            return ibis.literal(value)  # pyright: ignore[reportUnknownMemberType]
        case _:  # pragma: no cover
            raise NotImplementedError("Unsupported Literal")


@value_handler("Column")
def handle_column(payload: PolarsPlan):
    return defer[payload]  # pyright: ignore[reportArgumentType]


@value_handler("Function")
def handle_function(payload: PolarsPlan):  # type: ignore
    match payload:
        case {
            "input": [left_expr, right_expr],
            "function": {"Pow": "Generic"},
            **extras,
        }:
            assert_no_extras(extras)
            return polars_expr_to_ibis_value(left_expr) ** polars_expr_to_ibis_value(
                right_expr
            )  # type: ignore
        case {"input": [input_expr], "function": {"Boolean": "Not"}, **extras}:
            assert_no_extras(extras)
            return ~polars_expr_to_ibis_value(input_expr)  # type: ignore
        case {"input": [input_expr], "function": "Negate", **extras}:
            assert_no_extras(extras)
            return -polars_expr_to_ibis_value(input_expr)  # type: ignore
        case {
            "input": [input_expr, lower_expr, upper_expr],
            "function": {"Clip": {"has_min": True, "has_max": True}},
            **extras,
        }:
            assert_no_extras(extras)
            lower = polars_expr_to_ibis_value(lower_expr)
            upper = polars_expr_to_ibis_value(upper_expr)
            return polars_expr_to_ibis_value(input_expr).clip(lower, upper)  # type: ignore
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
