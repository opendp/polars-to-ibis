"""Convert Polars expressions to Ibis expressions"""

import re
from pathlib import Path
from typing import Any

import ibis  # pyright: ignore [reportMissingTypeStubs]
import polars as pl

from polars_to_ibis.serializer import Serialization

__version__ = (Path(__file__).parent / "VERSION").read_text().strip()

# Polars 1.32 is needed to support OpenDP 0.14.1:
_min_polars = "1.32.0"
# TODO: When we drop Polars 1.32 support, we could simplify things.
# _min_polars = "1.33.0"
_max_polars = "1.34.0"


def _warn(message: str):  # pragma: no cover
    # It's hard to remember to use the wrapping class,
    # so do it by default,
    # and keep "warn" out of the global namespace.
    from warnings import warn

    warn(PolarsToIbisWarning(message))


def _check_version():
    if not (
        _min_polars.split(".")  # Oldest supported
        <= pl.__version__.split(".")  # Installed
        <= _max_polars.split(".")  # Newest supported
    ):
        _warn(  # pragma: no cover
            f"Polars {pl.__version__} has not been tested! "
            f"Try {_min_polars} to {_max_polars}."
        )


def polars_to_ibis(lf: pl.LazyFrame, table_name: str) -> ibis.Table:
    _check_version()

    # NOTE: Tests fail if the order of serialize() and collect_schema() is switched.
    # TODO: Understand whether the schema or the plan is changing.
    polars_plan = Serialization(lf)
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


def _apply_polars_plan_to_ibis_table(polars_plan: Serialization, table: ibis.Table):
    polars_plan_keys = list(polars_plan.keys())
    if len(polars_plan_keys) != 1:
        raise UnexpectedPolarsException(  # pragma: no cover
            f"Expected only a single key, not: {polars_plan_keys}"
        )
    operation = polars_plan_keys[0]
    params = polars_plan[operation]

    if operation == "DataFrameScan":
        return table

    input_polars_plan = params.pop("input")
    input_table = _apply_polars_plan_to_ibis_table(input_polars_plan, table)

    return _apply_operation_params_to_ibis_table(operation, params, input_table)


def _apply_operation_params_to_ibis_table(
    operation: str, params: dict[str, Any], table: ibis.Table
):
    # We want to be sure that there are no unused parameters,
    # so we'll pop() from param, and if the local is unused,
    # linting will catch it.
    match operation:
        case "Select":
            # TODO: Only handling one particular case!
            options = params.pop("options")
            expr = params.pop("expr")
            _assert_empty(params)

            assert isinstance(options.pop("run_parallel"), bool)
            assert isinstance(options.pop("duplicate_check"), bool)
            assert isinstance(options.pop("should_broadcast"), bool)
            _assert_empty(options)

            # TODO: This is the same structure as at the top-level.
            assert len(expr) == 1
            assert len(expr[0]) == 1

            select_operation, inner_params = list(expr[0].items())[0]

            match select_operation:
                case "Agg":  # pragma: no cover
                    count = inner_params.pop("Count")
                    _assert_empty(inner_params)

                    # breakpoint()
                    # `pytest -k count`:
                    # pl.__version__ == '1.32.0'
                    # count == [{'Selector': 'Wildcard'}, False]

                    # pl.__version__ == '1.33.0'
                    # count == {
                    #   'input': {'Selector': 'Wildcard'},
                    #   'include_nulls': False
                    # }

                    input = count.pop("input")
                    _assert_falsy(count)

                    selector = input.pop("Selector")
                    assert selector == "Wildcard"

                    # TODO: table.count() returns an int,
                    # but Polars returns a dataframe.
                    # Can we do something else to get ibis
                    # results that will match polars?
                    raise UnhandledPolarsException(
                        f"Unhandled select operation: {select_operation}"
                    )
                case "Column":  # pragma: no cover
                    # TODO: Not working!
                    assert isinstance(inner_params, str)
                    return table.select(inner_params)
                case "Selector":
                    raise UnhandledPolarsException(
                        f"Unhandled select operation: {select_operation}"
                    )
                case _:  # pragma: no cover
                    raise UnexpectedPolarsException(
                        f"Unexpected select operation: {select_operation}"
                    )
        case "Slice":
            length = params.pop("len")
            offset = params.pop("offset")
            _assert_empty(params)
            return table.limit(length, offset=offset)
        case "Sort":
            by_column = params.pop("by_column")
            slice = params.pop("slice")
            sort_options = params.pop("sort_options")
            _assert_empty(params)
            _assert_falsy(slice)

            assert isinstance(sort_options.pop("multithreaded"), bool)
            _assert_falsy(sort_options)

            args: list[str] = []
            for col in by_column:
                args.append(col.pop("Column"))
                _assert_empty(col)

            return table.order_by(*args)
        case "MapFunction":
            function = params.pop("function")
            _assert_empty(params)

            stats = function.pop("Stats")
            _assert_empty(function)

            if isinstance(stats, str):
                return _aggregate(table, stats)

            if isinstance(stats, dict):
                items = list(stats.items())  # type: ignore
                assert len(items) == 1  # type: ignore
                key, value = items[0]  # type: ignore
                return _aggregate(table, key)  # type: ignore

            raise UnhandledPolarsException(
                f"Unhandled polars map function: {stats}"
            )  # pragma: no cover
        case "HStack":
            options = params.pop("options")

            exprs = params.pop("exprs")
            _assert_empty(params)
            expr = exprs.pop()
            _assert_empty(exprs)

            assert len(expr) == 1

            raise UnhandledPolarsException(f"Unhandled expression: {expr}")

        case _:  # pragma: no cover
            raise UnhandledPolarsException(f"Unhandled polars operation: {operation}")


def _aggregate(table: ibis.Table, function_name: str) -> ibis.Table:
    return table.aggregate(
        [getattr(getattr(table, col), function_name.lower())() for col in table.columns]
    ).rename(lambda name: re.sub(r"^\w+\((.*)\)$", r"\1", name))


def _assert_empty(params: Any):
    if len(params):  # pragma: no cover
        _warn(f"Params not empty: {params}")


def _assert_falsy(value: Any):
    if not value:
        return
    # This is broader that python's notion of falsy:
    if isinstance(value, list):
        values = value  # pyright: ignore[reportUnknownVariableType]
    elif isinstance(value, dict):
        values = value.values()  # pyright: ignore[reportUnknownVariableType]
    else:  # pragma: no cover
        _warn(f"Value not falsy: {value}")
        return

    for v in values:  # pyright: ignore[reportUnknownVariableType]
        _assert_falsy(v)
