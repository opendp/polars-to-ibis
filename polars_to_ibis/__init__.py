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
            _check_params(params, {"input", "len", "offset"})
            return table.limit(params["len"], offset=params["offset"])
        case "Sort":
            _check_params(params, {"input", "by_column", "slice", "sort_options"})
            _check_falsy_params(params, {"slice"})
            _check_falsy_params(
                params["sort_options"],
                {
                    "descending",
                    "nulls_last",
                    "maintain_order",
                    "limit",
                },
            )

            by_column = params["by_column"]
            assert len(by_column) == 1  # TODO: Loosen
            _check_params(by_column[0], {"Column"})

            return table.order_by(by_column[0]["Column"])
        case _:
            raise UnhandledPolarsException(f"Unhandled polars operation: {operation}")


def _check_params(params, expected):
    unexpected_params = params.keys() - expected
    if unexpected_params:  # pragma: no cover
        raise UnhandledPolarsException(f"Unhandled polars params: {unexpected_params}")


def _check_falsy_params(params, should_be_falsy):
    def falsy(value):
        if not value:
            return True
        # This is broader that python's notion of falsy:
        if isinstance(value, list) and all(falsy(inner) for inner in value):
            return True
        return False

    not_falsy = {k for k in should_be_falsy if not falsy(params[k])}
    if not_falsy:
        raise UnhandledPolarsException(f"Unhandled non-none polars params: {not_falsy}")
