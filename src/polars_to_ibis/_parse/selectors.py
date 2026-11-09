"""Expand multi-column Polars selectors before translating scalar expressions."""

from collections.abc import Iterator
from typing import Any

import ibis.expr.types as ir

from . import tags
from .utils import assert_no_extras

PolarsPlan = dict[str, Any]


def _columns(selector: Any, table: ir.Table) -> list[str]:
    match selector:
        case "Wildcard":
            return list(table.columns)
        case {"ByName": {"names": names, "strict": True, **extra_1}, **extra_2}:
            assert_no_extras(extra_1, extra_2)
            return names
        case {
            "Difference": [
                "Wildcard",
                {"ByName": {"names": excluded, "strict": True, **extra_1}, **extra_2},
            ],
            **extra_3,
        }:
            assert_no_extras(extra_1, extra_2, extra_3)
            return [name for name in table.columns if name not in excluded]
        case {
            "Union": [
                {"ByDType": {"AnyOf": _, **extra_1}, **extra_2},
                {"ByDType": "Decimal", **extra_3},
            ],
            **extra_4,
        }:
            assert_no_extras(extra_1, extra_2, extra_3, extra_4)
            return [
                name for name, dtype in table.schema().items() if dtype.is_numeric()
            ]
        case _:
            raise NotImplementedError(f"Unsupported selector: {selector!r}")


def _find_selectors(expr: Any) -> Iterator[Any]:
    if isinstance(expr, dict):
        if set(expr) == {tags.value.SELECTOR}:
            yield expr[tags.value.SELECTOR]
        else:
            for value in expr.values():
                yield from _find_selectors(value)
    elif isinstance(expr, list):
        for value in expr:
            yield from _find_selectors(value)


def _replace_selector(expr: Any, name: str) -> Any:
    if isinstance(expr, dict):
        if set(expr) == {tags.value.SELECTOR}:
            return {tags.value.COLUMN: name}
        return {key: _replace_selector(value, name) for key, value in expr.items()}
    if isinstance(expr, list):
        return [_replace_selector(value, name) for value in expr]
    return expr


def expand_expr(expr: PolarsPlan, table: ir.Table) -> list[PolarsPlan]:
    """Expand a single selector, then delegate expression translation."""
    selectors = list(_find_selectors(expr))
    if not selectors:
        return [expr]
    if len(selectors) != 1:
        raise NotImplementedError("Expressions with multiple selectors")
    return [_replace_selector(expr, name) for name in _columns(selectors[0], table)]
