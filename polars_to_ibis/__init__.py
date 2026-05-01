"""Convert Polars plans to Ibis tables"""

from pathlib import Path

import ibis  # pyright: ignore [reportMissingTypeStubs]
import polars as pl

__version__ = (Path(__file__).parent / "VERSION").read_text().strip()

# Polars 1.32 is needed to support OpenDP 0.14.1:
_min_polars = "1.32.0"
# TODO: When we drop Polars 1.32 support, we could simplify things.
# _min_polars = "1.33.0"
_max_polars = "1.34.0"


class PolarsToIbisWarning(Warning):
    pass


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


def convert_polars_to_ibis(lf: pl.LazyFrame, table_name: str) -> ibis.Table:
    from polars_to_ibis.parse import update_polars_to_ibis
    from polars_to_ibis.serialize import serialize

    _check_version()

    polars_plan = serialize(lf)  # type: ignore
    input_schema = _get_input_schema(polars_plan)
    ibis_table = ibis.table(input_schema, name=table_name)  # type: ignore

    return update_polars_to_ibis(
        polars_plan=polars_plan,
        table=ibis_table,
    )


def _get_input_schema(polars_plan):
    """
    lf.collect_schema() returns the OUTPUT schema,
    after columns have been dropped or added.

    Instead, we need to walk the tree to find the input schema.

    >>> polars_plan = {
    ...     'Select': {
    ...         'expr': [{'Column': 'ints'}],
    ...         'input': {
    ...             'DataFrameScan': {
    ...                 'df': ['byte list'],
    ...                 'schema': {'fields': {'ints': 'Int64', 'strs': 'String'}}
    ...             }
    ...         },
    ...         'options': {
    ...             'run_parallel': True,
    ...             'duplicate_check': True,
    ...             'should_broadcast': True
    ...         }
    ...     }
    ... }
    >>> _get_input_schema(polars_plan)
    ibis.Schema {
      ints  int64
      strs  string
    }

    """
    if not isinstance(polars_plan, dict):
        return
    if "DataFrameScan" in polars_plan:
        input_schema = {
            k: _type_map[v]
            for k, v in polars_plan["DataFrameScan"]["schema"]["fields"].items()
        }
        return ibis.expr.schema.Schema(input_schema)
    for value in polars_plan.values():
        maybe_schema = _get_input_schema(value)
        if maybe_schema:
            return maybe_schema


_type_map = {
    # TODO: Expand
    "Int32": int,
    "Int64": int,
    "Float64": float,
    "String": str,
}
