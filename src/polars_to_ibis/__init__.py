"""
[![pypi](https://img.shields.io/pypi/v/polars_to_ibis)](https://pypi.org/project/polars_to_ibis/)
[![github](https://img.shields.io/badge/github-polars_to_ibis-blue?logo=github)](https://github.com/opendp/polars-to-ibis)

Convert [Polars LazyFrames](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html)
to [Ibis unbound tables](https://ibis-project.org/how-to/extending/unbound_expression#unbound-tables).

Polars and Ibis have similar APIs, but while Polars supports computation in-memory and on
[Polars Cloud](https://cloud.pola.rs/), Ibis by itself does not handle computation:
Instead it translates the dataframe expression into idiomatic SQL for a particular database.

## Example

First, we'll write to the database so we have something to query.
To connect to a particular database, you will need to install the appropriate extra.
Taking SQLite as an example:

```shell
$ pip install 'polars-to-ibis'
$ pip install 'ibis-framework[sqlite]'
```

Create the table for our example:

```python
>>> import ibis
>>> import polars as pl
>>> connection = ibis.sqlite.connect()
>>> table_name = 'readme_example'
>>> connection.create_table(
...      table_name,
...      pl.DataFrame({"ints": [1, 2, 3, 4]}),
...      overwrite=True,
... )
DatabaseTable: readme_example
  ints int64

```

Now we can demonstrate the typical use of polars-to-ibis.
To read a database table's schema and create a LazyFrame, use `scan_database`:

```python
>>> from polars_to_ibis import scan_database, convert_polars_to_ibis
>>> polars_lazy = scan_database(connection, table_name)

```

Next, make a query starting with that LazyFrame:

```python
>>> polars_query = polars_lazy.sum()
>>> ibis_unbound_table = convert_polars_to_ibis(
...     polars_query,
...     table_name=table_name,
... )
>>> print(ibis_unbound_table.to_sql())
SELECT
  SUM("t0"."ints") AS "ints"
FROM "readme_example" AS "t0"

```

Finally, we can execute in SQLite the query which we constructed in Polars and translated to Ibis:

```python
>>> connection.to_polars(ibis_unbound_table).to_dict(as_series=False)
{'ints': [10]}

```


## Limitations

- Python versions: Tested against Python 3.10 and 3.13.
- Polars versions: Tested against Polars 1.32.0, 1.36.1, and 1.41.2.
- Ibis version: Tested against Ibis 11.0.0.
- Feature coverage, and database quirks: We only cover a fraction of the Polars API,
  and even within that range there are often quirks in how a query is handled by a given database.
  The best summary is the collection of [test scenarios](https://github.com/opendp/polars-to-ibis/blob/main/tests/config_parser.py).

---
"""  # noqa: B950

from importlib.metadata import version
from typing import Any

import ibis  # pyright: ignore [reportMissingTypeStubs]
import polars as pl

from ._parse import tags
from ._parse.table_handlers import update_polars_to_ibis
from ._serialize import serialize
from ._utils import PluginReplacer

__version__ = version("polars_to_ibis")

_MIN_POLARS: str = "1.32.0"
_MAX_POLARS: str = "1.41.2"

__all__ = ["convert_polars_to_ibis", "scan_database", "split_polars_on_ffi"]


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


def scan_database(connection: Any, table_name: str) -> pl.LazyFrame:
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
    if tags.table.DATA_FRAME_SCAN in polars_plan:
        input_schema = {
            k: _get_type(v)
            for k, v in polars_plan[tags.table.DATA_FRAME_SCAN]["schema"][
                "fields"
            ].items()
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


def split_polars_on_ffi(
    query: pl.LazyFrame, table_name: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Split a Polars LazyFrame on an FFI plugin,
    returning an Ibis unbound table ready to be executed on a SQL database,
    and a list of parameter dicts for the plugin.
    """

    polars_plan = serialize(query)
    input_schema = _get_input_schema(polars_plan)

    param_dicts = PluginReplacer(polars_plan).replace()

    ibis_table = update_polars_to_ibis(
        polars_plan=polars_plan,
        table=ibis.table(input_schema, name=table_name),
    )

    return ibis_table, param_dicts
