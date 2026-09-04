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


def assert_no_extras(locals_dict: dict[str, Any]) -> None:
    """
    >>> extras_1 = {}
    >>> extras_2 = {'surprise': True}
    >>> extras_3 = {}
    >>> assert_no_extras(locals())
    Traceback (most recent call last):
    ...
    NotImplementedError: Unsupported extra parameters extras_2:
    {'extras_2': {'surprise': True}}
    """
    extras = {k: v for k, v in locals_dict.items() if k.startswith("extras") and v}
    if extras:
        raise NotImplementedError(
            f"Unsupported extra parameters {', '.join(extras.keys())}:\n{extras}"
        )
