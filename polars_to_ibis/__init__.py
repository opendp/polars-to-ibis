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
    # We want to be sure that there are no unused parameters,
    # so we'll pop() from param, and if the local is unused,
    # linting will catch it.
    match operation:
        case "Slice":
            length = params.pop("len")
            offset = params.pop("offset")
            assert not params
            return table.limit(length, offset=offset)
        case "Sort":
            by_column = params.pop("by_column")
            slice = params.pop("slice")
            sort_options = params.pop("sort_options")
            assert not params
            assert _falsy(slice)

            multithreaded = sort_options.pop("multithreaded")
            assert multithreaded
            assert _falsy(sort_options)

            args = []
            for col in by_column:
                args.append(col.pop("Column"))
                assert not col

            return table.order_by(*args)
        case "MapFunction":
            function = params.pop("function")
            assert not params

            stats = function.pop("Stats")
            assert not function

            return table.aggregate(
                [getattr(getattr(table, col), stats.lower())() for col in table.columns]
            ).rename(lambda name: re.sub(r"^\w+\((.*)\)$", r"\1", name))
        case _:
            raise UnhandledPolarsException(f"Unhandled polars operation: {operation}")


def _falsy(value):
    if not value:
        return True
    # This is broader that python's notion of falsy:
    if isinstance(value, list) and all(_falsy(inner) for inner in value):
        return True
    if isinstance(value, dict) and all(_falsy(inner) for inner in value.values()):
        return True

    return False
