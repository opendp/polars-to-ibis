"""Convert Polars expressions to Ibis expressions"""

import json
import re
from pathlib import Path
from warnings import warn

import ibis
import polars as pl

__version__ = (Path(__file__).parent / "VERSION").read_text().strip()

# We are primarily interested in supporting the version pinned by OpenDP,
# but if we can support a wider range without much work, great!
_min_polars = "1.25.2"
_max_polars = "1.34.0"


def polars_to_ibis(lf: pl.LazyFrame, table_name: str) -> ibis.Table:
    if not (
        _min_polars.split(".")  # Oldest supported
        <= pl.__version__.split(".")  # Installed
        <= _max_polars.split(".")  # Newest supported
    ):
        warn(  # pragma: no cover
            PolarsToIbisWarning(
                f"Polars {pl.__version__} has not been tested! "
                f"Try {_min_polars} to {_max_polars}."
            )
        )
    polars_json = lf.serialize(format="json")
    polars_plan = json.loads(polars_json)

    polars_schema = lf.collect_schema()
    ibis_schema = ibis.expr.schema.Schema.from_polars(polars_schema)
    ibis_table = ibis.table(ibis_schema, name=table_name)

    return _apply_polars_plan_to_ibis_table(polars_plan, ibis_table)


class PolarsToIbisWarning(Warning):
    pass


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
        raise UnexpectedPolarsException(  # pragma: no cover
            f"Expected only a single key, not: {polars_plan_keys}"
        )
    operation = polars_plan_keys[0]
    params = polars_plan[operation]

    if "input" not in params:
        return table

    input_polars_plan = params.pop("input")
    input_table = _apply_polars_plan_to_ibis_table(input_polars_plan, table)

    return _apply_operation_params_to_ibis_table(operation, params, input_table)


def _apply_operation_params_to_ibis_table(
    operation: str, params: dict, table: ibis.Table
):
    match operation:
        case "Slice":
            _check_params(params, {"len", "offset"})
            return table.limit(params["len"], offset=params["offset"])
        case "Sort":
            _check_params(params, {"by_column", "slice", "sort_options"})
            _check_falsy_params(params, {"slice"})

            sort_options = params["sort_options"]
            _check_params(
                sort_options,
                {
                    "multithreaded",  # defaults to True: ignored.
                    "descending",
                    "nulls_last",
                    "maintain_order",
                    "limit",
                },
            )
            _check_falsy_params(
                sort_options,
                {
                    "maintain_order",
                    "limit",
                },
            )

            by_column = params["by_column"]
            for col in by_column:
                _check_params(col, {"Column"})

            args = [col["Column"] for col in by_column]
            return table.order_by(*args)
        case "MapFunction":
            _check_params(params, {"function"})
            function = params["function"]

            _check_params(function, {"Stats"})
            stats = function["Stats"]
            if stats not in {"Max", "Min", "Mean"}:
                raise UnhandledPolarsException(
                    f"Unhandled polars stat: {stats}"
                )  # pragma: no cover

            return table.aggregate(
                [getattr(getattr(table, col), stats.lower())() for col in table.columns]
            ).rename(lambda name: re.sub(r"^\w+\((.*)\)$", r"\1", name))
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
        raise UnhandledPolarsException(
            f"Unhandled not-falsy polars params: {not_falsy}"
        )
