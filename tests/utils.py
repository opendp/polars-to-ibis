from os import environ

import ibis  # type: ignore
import polars as pl
import pytest


def get_connection(
    df: pl.DataFrame,
    table_name: str,
    backend: ibis.BaseBackend,
):
    kwargs = (
        {
            "user": environ["USER"],
            "password": "",
            "database": environ["USER"],
        }
        if backend == "mysql"
        else {}
    )
    connection = backend.connect(**kwargs)

    # Ensure a clean slate.
    # Each backend raises its own error type
    # if the table doesn't already exist.
    # NOTE: overwrite=True would be simpler, but not supported by MySQL.
    try:
        connection.drop_table(table_name)
    except BaseException:  # noqa: B036
        pass
    connection.create_table(table_name, df)

    return connection


# Test scenarios:

backend_names = [
    # Polars could be tested, but there's an error getting the schema,
    # and since it's not a realistic target for us, drop it from coverage.
    "sqlite",
    "duckdb",
    pytest.param("postgres", marks=pytest.mark.extra_install),
    pytest.param("mysql", marks=pytest.mark.extra_install),
]


exporters = {  # type: ignore
    "to_polars": lambda conn, table: conn.to_polars(table).to_dict(as_series=False),  # type: ignore
    "to_pandas": lambda conn, table: conn.to_pandas(table).to_dict(orient="list"),  # type: ignore
    "to_pyarrow": lambda conn, table: conn.to_pyarrow(table).to_pydict(),  # type: ignore
}
