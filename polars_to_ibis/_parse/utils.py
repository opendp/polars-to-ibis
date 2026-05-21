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


def assert_no_extras(*extras_dicts: dict[str, Any]) -> None:
    """
    >>> extras_1 = {}
    >>> extras_2 = {'surprise': True}
    >>> extras_3 = {}
    >>> assert_no_extras(extras_1, extras_2, extras_3)
    Traceback (most recent call last):
    ...
    NotImplementedError: Unsupported extra parameters: 2: {'surprise'}
    """
    errors: list[str] = []
    for i, extras in enumerate(extras_dicts):
        unexpected = extras.keys() - {"input"}
        if unexpected:
            errors.append(f"{i+1}: {unexpected}")
    if errors:
        raise NotImplementedError(f"Unsupported extra parameters: {'; '.join(errors)}")
