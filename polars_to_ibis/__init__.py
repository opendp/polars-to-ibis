"""Convert Polars expressions to Ibis expressions"""

import json
from pathlib import Path

import ibis
import polars as pl

__version__ = (Path(__file__).parent / "VERSION").read_text().strip()


def polars_to_ibis(lf: pl.LazyFrame, table_name: str) -> ibis.Table:
    polars_json = lf.serialize(format="json")
    polars_plan = json.loads(polars_json)

    polars_schema = lf.collect_schema()
    ibis_schema = ibis.expr.schema.Schema.from_polars(polars_schema)
    ibis_table = ibis.table(ibis_schema, name=table_name)

    return _apply_polars_plan_to_ibis_table(polars_plan, ibis_table)


class UnexpectedPolarsException(Exception):
    """
    JSON structure is not what we expected.
    """

    pass


class UnhandledPolarsException(Exception):
    """
    JSON structure is not unexpected, but we just don't handle it yet.
    """

    pass


def _apply_polars_plan_to_ibis_table(polars_plan: dict, table: ibis.Table):
    polars_plan_keys = list(polars_plan.keys())
    if len(polars_plan_keys) != 1:
        raise UnexpectedPolarsException(
            f"Expected only a single key, not: {polars_plan_keys}"
        )
    operation = polars_plan_keys[0]
    params = polars_plan[operation]
    match operation:
        case "Slice":
            _raise_unexpected_params(params, {"len", "offset", "input"})
            return table.limit(params["len"], offset=params["offset"])
        case _:
            raise UnhandledPolarsException(f"Unhandled polars operation: {operation}")


def _raise_unexpected_params(params, expected):
    unexpected_params = params.keys() - expected
    if unexpected_params:  # pragma: no cover
        raise UnhandledPolarsException(f"Unhandled polars params: {unexpected_params}")
