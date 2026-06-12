import re
from os import environ
from typing import Any, Callable

import ibis  # type: ignore
import polars as pl
import pytest

from polars_to_ibis import convert_polars_to_ibis, scan_database
from polars_to_ibis._parse.table_handlers import update_polars_to_ibis

from .fixtures import Fixture, fixtures, input_data

# Utilities:


def get_connection(df: pl.DataFrame, table_name: str, backend: str):
    kwargs = (
        {
            "user": environ["USER"],
            "password": "",
            "database": environ["USER"],
        }
        if backend == "mysql"
        else {}
    )
    connection = getattr(ibis, backend).connect(**kwargs)

    # Ensure a clean slate.
    # Each backend raises its own error type
    # if the table doesn't already exist.
    try:
        connection.drop_table(table_name)
    except BaseException:  # noqa: B036
        pass
    connection.create_table(table_name, df)

    return connection


# Test fixtures:

backends = [
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


# Tests:


def assert_error_or_none(
    error_type: str, expected_error: str | None, func: Callable[[], Any]
) -> Any:
    if expected_error:
        with pytest.raises(Exception, match=re.escape(expected_error)):
            func()
        pytest.xfail(f"expected error: {expected_error}")
    try:
        result = func()
    except Exception as e:  # pragma: no cover
        pytest.fail(f"Add {error_type}? {e}")
    return result


@pytest.mark.parametrize(
    "fixture", fixtures, ids=lambda fixture: f"{fixture.category}-{fixture.expression}"
)
def test_fixture_consistency(fixture: Fixture):
    # Does the polars expression have the expected result?
    globals = {"lf": pl.LazyFrame(input_data[fixture.category]), "pl": pl}
    polars_output = eval(fixture.expression, globals).collect().to_dict(as_series=False)
    assert polars_output == fixture.expected_output, "Typo in fixture?"


@pytest.mark.parametrize(
    "fixture", fixtures, ids=lambda fixture: f"{fixture.category}-{fixture.expression}"
)
@pytest.mark.parametrize("backend", backends)
@pytest.mark.parametrize("exporter_key", exporters.keys())  # type: ignore
def test_translate_table_new(fixture: Fixture, backend: str, exporter_key: str):
    table_name = "default_table"

    # Set up target database, with data:
    input_df = pl.DataFrame(input_data[fixture.category])
    connection = assert_error_or_none(
        "connection_error",
        fixture.connection_errors.get(backend),
        lambda: get_connection(input_df, table_name=table_name, backend=backend),
    )

    globals = {"lf": scan_database(connection, table_name), "pl": pl}
    lf = eval(fixture.expression, globals)

    ibis_table = convert_polars_to_ibis(lf, table_name)

    # Run query on target database:
    export = exporters[exporter_key]  # type: ignore
    expected_backend_error = fixture.backend_errors.get(
        backend
    ) or fixture.backend_errors.get(f"{backend}+{exporter_key}")
    actual_output = assert_error_or_none(
        "backend_error",
        expected_backend_error,
        lambda: export(connection, ibis_table),  # type: ignore
    )

    # Check if result is what we expect:
    if fixture.tolerance:
        assert_approx_equal(
            actual_output,  # type: ignore
            fixture.expected_output,
            fixture.tolerance,
            f"Via ibis, {backend} does not produce output within {fixture.tolerance}",
        )
    else:
        assert (
            actual_output == fixture.expected_output
        ), f"Via ibis, {backend} does not produce expected output"


def assert_approx_equal(
    actual: dict[str, list[float | str]],
    expected: dict[str, list[float | str]],
    tolerance: float,
    message: str,
):
    any_not_equal = False
    for key in actual.keys() | expected.keys():
        actual_col = actual[key]
        expected_col = expected[key]
        assert actual_col == pytest.approx(expected_col, abs=tolerance), f"{message} on {key}"  # type: ignore  # noqa: B950 (line too long)
        any_not_equal |= actual_col != expected_col
    assert any_not_equal, "All are equal; approx not needed"


@pytest.mark.parametrize(
    "polars_plan,expected_error",
    [
        (
            {},
            "Expected single-key tagged dict",
        ),
        (
            {"Scan": {}},
            "Unsupported Scan",
        ),
        (
            {"Scan": {"df": {}, "schema": {}}},
            "Unsupported Scan",
        ),
        (
            # When/if Count *is* supported, this test won't work.
            {
                "Select": {
                    "expr": [
                        {
                            "Agg": {
                                "Count": {
                                    "include_nulls": False,
                                    "input": {"Selector": "Wildcard"},
                                }
                            }
                        }
                    ],
                    "input": {"DataFrameScan": {"df": {}, "schema": {"fields": {}}}},
                    "options": {
                        "duplicate_check": True,
                        "run_parallel": True,
                        "should_broadcast": True,
                    },
                }
            },
            # Check that the input data structure is shown in error message.
            "Unsupported Agg:\n{'Select'",
        ),
    ],
    ids=lambda plan: str(plan),
)
def test_unexpected_payloads(polars_plan, expected_error):
    with pytest.raises(Exception, match=re.escape(expected_error)):
        update_polars_to_ibis(polars_plan, None)
