import logging
import re
from collections.abc import Callable
from os import environ
from typing import Any

import ibis  # type: ignore
import polars as pl
import pytest

logger = logging.getLogger(__name__)


class BaseScenario:
    def get_with_keys(self, errors_dict_name, backend_name, exporter_key):
        errors_dict = getattr(self, errors_dict_name)
        # There seem to be errors common to exporters across backends,
        # so check exporter_key before backend_name.
        for key in [
            "*",
            f"polars=={pl.__version__}",
            f"{backend_name}+{exporter_key}",
            exporter_key,
            backend_name,
        ]:
            try:
                return errors_dict[key]
            except KeyError:
                pass

    def assert_error_or_return_value(
        self, errors_dict_name, backend_name, exporter_key, func: Callable[[], Any]
    ) -> Any:
        expected_error = self.get_with_keys(
            errors_dict_name, backend_name, exporter_key
        )
        if expected_error is not None:
            with pytest.raises(Exception, match=re.escape(expected_error)):
                func()
            pytest.xfail(f"expected error: {expected_error}")
        try:
            result = func()
        except Exception as e:  # noqa: BLE001  # pragma: no cover
            pytest.fail(
                f"(If this is expected, add {errors_dict_name} to scenario.) {e}"
            )
        return result


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
        # Accessing the attribute actually checks for install,
        # so "backend.mysql" would fail if mysql is not installed.
        if backend.name == "mysql"
        else {}
    )
    connection = backend.connect(**kwargs)

    # Ensure a clean slate.
    # Each backend raises its own error type
    # if the table doesn't already exist.
    # NOTE: overwrite=True would be simpler, but not supported by MySQL.
    try:
        connection.drop_table(table_name)
    except Exception as e:  # noqa: BLE001
        logger.info(f"Expected error from drop_table: {e}")
    connection.create_table(table_name, df)

    return connection


# Test scenarios:

backend_names = [
    # Polars could be tested, but there's an error getting the schema,
    # and since it's not a realistic target for us, drop it from coverage.
    "sqlite",
    "duckdb",
    pytest.param("postgres", marks=pytest.mark.extra_install),
    # TODO: restore mysql
    # https://github.com/opendp/polars-to-ibis/issues/190
    # pytest.param("mysql", marks=pytest.mark.extra_install),
]


exporters = {  # type: ignore
    "to_polars": lambda conn, table: conn.to_polars(table).to_dict(as_series=False),  # type: ignore
    "to_pandas": lambda conn, table: conn.to_pandas(table).to_dict(orient="list"),  # type: ignore
    "to_pyarrow": lambda conn, table: conn.to_pyarrow(table).to_pydict(),  # type: ignore
}
