"""
This is a private module: The API may change.
"""

from typing import Any, Callable

import ibis.expr.types as ir  # pyright: ignore [reportMissingTypeStubs]

PolarsPlan = dict[str, Any]
NamedValue = tuple[str, ir.Value]
ReturnsTable = Callable[..., ir.Table]
ReturnsValue = Callable[..., NamedValue]


def split_tag_payload(polars_plan: PolarsPlan) -> tuple[str, Any]:
    match list(polars_plan.items()):
        case [[tag, payload]]:
            return tag, payload
        case _:
            raise ValueError(
                f"Expected single-key tagged dict, got: {polars_plan!r}"
            )  # pragma: no cover


def assert_no_extras(extras: dict[str, Any]) -> None:
    unexpected = extras.keys() - {"input"}
    if unexpected:
        raise NotImplementedError(f"Unsupported extra parameters: {unexpected}")
