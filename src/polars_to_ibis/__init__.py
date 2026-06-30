"""Convert Polars LazyFrames to Ibis unbound tables"""

from importlib.metadata import version
from typing import Any

import ibis  # pyright: ignore [reportMissingTypeStubs]
import polars as pl

__version__ = version("dp_wizard")

_MIN_POLARS: str = "1.32.0"
_MAX_POLARS: str = "1.41.2"

__all__ = ["convert_polars_to_ibis"]


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
        _MIN_POLARS.split(".")  # Oldest supported
        <= pl.__version__.split(".")  # Installed
        <= _MAX_POLARS.split(".")  # Newest supported
    ):
        _warn(  # pragma: no cover
            f"Polars {pl.__version__} has not been tested! "
            f"Use {_MIN_POLARS} to {_MAX_POLARS}, "
            f"or submit a PR to expand test coverage."
        )


def scan_database(connection: Any, table_name: str):
    """
    Get the schema from a database table and convert it to Polars.
    """
    ibis_schema = connection.get_schema(table_name)
    return pl.LazyFrame(schema=ibis_schema.to_polars())


def convert_polars_to_ibis(lf: pl.LazyFrame, table_name: str) -> ibis.Table:
    """
    Convert a Polars LazyFrame to an Ibis unbound table.

    The name of the table in the target database is also required:
    Ibis translates dataframe syntax to idiomatic SQL,
    so it needs to have a table name to include in the SQL.
    """
    from polars_to_ibis._parse.table_handlers import update_polars_to_ibis
    from polars_to_ibis._serialize import serialize

    _check_version()

    polars_plan = serialize(lf)  # type: ignore
    input_schema = _get_input_schema(polars_plan)
    ibis_table = ibis.table(input_schema, name=table_name)  # type: ignore

    return update_polars_to_ibis(
        polars_plan=polars_plan,
        table=ibis_table,
    )


def _get_input_schema(polars_plan: dict[str, Any]) -> ibis.expr.schema.Schema:
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
    if not isinstance(polars_plan, dict):  # type: ignore
        return
    if "DataFrameScan" in polars_plan:
        input_schema = {
            k: _get_type(v)
            for k, v in polars_plan["DataFrameScan"]["schema"]["fields"].items()
        }
        return ibis.expr.schema.Schema(input_schema)
    for value in polars_plan.values():  # pragma: no cover
        maybe_schema = _get_input_schema(value)
        if maybe_schema:
            return maybe_schema


def _get_type(polars_type_name: str) -> type:
    if polars_type_name.startswith("Int"):
        return int
    if polars_type_name.startswith("Float"):
        return float
    if polars_type_name == "String":
        return str
    if polars_type_name == "Boolean":
        return bool
    if polars_type_name == "Binary":
        return bytes
    raise Exception(
        f"No python type defined for {polars_type_name}"
    )  # pragma: no cover
