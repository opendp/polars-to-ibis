"""
[![pypi](https://img.shields.io/pypi/v/polars_to_ibis)](https://pypi.org/project/polars_to_ibis/)
[![github](https://img.shields.io/badge/github-polars__to__ibis-blue?logo=github)](https://github.com/opendp/polars-to-ibis)

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
>>> backend = ibis.sqlite
>>> connection = backend.connect()
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
...     backend=backend,
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

## Coverage

A range of Polars expressions are handled.
Assuming this dataframe:
```json
{
    "animal": ["bird", "mouse", "butterfly", "spider"],
    "legs": [2, 4, 6, 8],
    "wings": [2, 0, 4, 0]
}
```

These [Polars SQL](https://docs.pola.rs/api/python/stable/reference/sql/) expressions produce the given results,
when translated to Ibis and executed on the target database:
```sql
SELECT wings, SUM(legs) FROM self GROUP BY wings
-- {"wings": [0, 2, 4], "legs": [12, 2, 6]}

SELECT animal, 3 * wings / legs AS ratio FROM self
-- {"animal": ["bird", "mouse", "butterfly", "spider"], "ratio": [3, 0, 2, 0]}

SELECT animal, CASE WHEN wings > 0 THEN 'flies' ELSE 'walks' END AS movement FROM self
-- {"animal": ["bird", "mouse", "butterfly", "spider"], "movement": ["flies", "walks", "flies", "walks"]}
```

For more details on what is and isn't covered,
see the [issues](https://github.com/opendp/polars-to-ibis/issues)
and the [tests](https://github.com/opendp/polars-to-ibis/blob/main/tests/config_parser.py).


## Limitations

- Tested against python versions: 3.11, 3.12, 3.13
- Tested against polars versions: 1.36.1, 1.37.0
- Tested against ibis versions: 12.0.0
- Feature coverage, and database quirks: We only cover a fraction of the Polars API,
  and even within that range there are often quirks in how a query is handled by a given database.

---

### API Documentation
"""  # noqa: B950

# Keep the "---" and "###" above: Improves the pdoc rendering.
# Third-level headings are not added to the sidebar.

from importlib.metadata import version
from typing import Any, cast

import ibis  # pyright: ignore [reportMissingTypeStubs]
import polars as pl

from ._parse import tags
from ._parse.table_handlers import update_polars_to_ibis
from ._serialize import serialize
from ._utils import PluginReplacer

__version__ = version("polars_to_ibis")

__all__ = ["convert_polars_to_ibis", "scan_database", "split_polars_on_ffi"]


class PolarsToIbisWarning(Warning):
    pass


def scan_database(connection: Any, table_name: str) -> pl.LazyFrame:
    """
    Get the schema from a database table and convert it to a Polars LazyFrame.
    If you already know the schema, or at least your columns of interest,
    feel free to construct the LazyFrame yourself to avoid the database dependency.
    """
    ibis_schema = connection.get_schema(table_name)
    return pl.LazyFrame(schema=ibis_schema.to_polars())


def convert_polars_to_ibis(
    lf: pl.LazyFrame,
    table_name: str | None = None,
    backend: ibis.BaseBackend | None = None,
    *,
    table: ibis.Table | None = None,
) -> ibis.Table:
    """
    Convert a Polars LazyFrame to an Ibis table without executing a query.

    Pass ``table_name`` and ``backend`` to construct an unbound table, as before.
    Alternatively, pass a bound ``table`` to preserve that exact relation,
    including its filters and projections. Its backend is inferred; an explicit
    ``backend`` must be that same backend instance, and ``table_name`` is forbidden.

    Bound conversion supports exactly one DataFrameScan. Its full input schema
    must match the supplied relation's column names, order, and supported types
    exactly (including integer signedness/width and floating-point width).
    Nullability is not compared: Polars schemas do not encode nullability.
    Callers must ensure the relation satisfies any additional analysis assumptions
    (such as null absence, value bounds, or margins); this is not a data check.
    Only expression/backend metadata is inspected, never private SQL results.
    """
    if table is None:
        if table_name is None or backend is None:
            raise TypeError("Provide table_name and backend, or a bound table")
    else:
        if not isinstance(table, ibis.Table):  # pyright: ignore [reportUnnecessaryIsInstance]
            raise TypeError("table must be a bound ibis.Table")
        if table_name is not None:
            raise ValueError("table and table_name cannot be supplied together")
        # _find_backend does not reject a mixture of bound and unbound sources.
        if table._find_backends()[1]:  # pyright: ignore [reportPrivateUsage]
            raise ValueError("table must not contain unbound sources")
        table_backend = table._find_backend()  # pyright: ignore [reportPrivateUsage]
        if backend is not None and backend is not table_backend:
            raise ValueError("backend must be the supplied table's backend instance")
        backend = table_backend

    polars_plan = serialize(lf)  # type: ignore
    if table is None:
        input_schema = _get_input_schema(polars_plan)
        table = ibis.table(input_schema, name=table_name)  # type: ignore
    else:
        _validate_bound_schema(polars_plan, table)

    return update_polars_to_ibis(
        polars_plan=polars_plan,
        table=table,
        backend=backend,
    )


def _validate_bound_schema(polars_plan: dict[str, Any], table: ibis.Table) -> None:
    scans: list[dict[str, Any]] = []

    def find_scans(node: Any) -> None:
        if isinstance(node, dict):
            node = cast(dict[str, Any], node)
            if tags.table.DATA_FRAME_SCAN in node:
                scans.append(node)
                return
            for value in node.values():
                find_scans(value)
        elif isinstance(node, list):
            for value in cast(list[Any], node):
                find_scans(value)

    find_scans(polars_plan)
    if len(scans) != 1:
        raise ValueError("Bound conversion requires exactly one DataFrameScan source")
    expected = _get_input_schema(scans[0])
    actual = ibis.schema(
        {name: dtype.copy(nullable=True) for name, dtype in table.schema().items()}
    )
    if not expected.equals(actual):
        raise ValueError(
            "Bound table schema must match the full Polars input schema "
            "(column names, order, and exact supported types)"
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


def _get_type(polars_type_name: str):
    if polars_type_name in {
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Float32",
        "Float64",
    }:
        return polars_type_name.lower()
    if polars_type_name == "String":
        return str
    if polars_type_name == "Boolean":
        return bool
    if polars_type_name == "Binary":
        return bytes
    raise ValueError(
        f"No python type defined for {polars_type_name}"
    )  # pragma: no cover


def split_polars_on_ffi(
    query: pl.LazyFrame,
    table_name: str,
    backend: ibis.BaseBackend,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Split a Polars LazyFrame on an FFI plugin,
    returning an Ibis unbound table ready to be executed on a backend,
    and a list of parameter dicts for the plugin.

    The backend parameter is used to determine the supported operations.
    """

    polars_plan = serialize(query)
    input_schema = _get_input_schema(polars_plan)

    param_dicts = PluginReplacer(polars_plan).replace()

    ibis_table = update_polars_to_ibis(
        polars_plan=polars_plan,
        table=ibis.table(input_schema, name=table_name),
        backend=backend,
    )

    return ibis_table, param_dicts
