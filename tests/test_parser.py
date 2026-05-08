import re
from os import environ

import ibis  # type: ignore
import polars as pl
import pytest

from polars_to_ibis import convert_polars_to_ibis
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
    "polars",
    "sqlite",
    "duckdb",
    pytest.param("postgres", marks=pytest.mark.extra_install),
    # MySQL could be added, if needed, but for now
    # we want to focus on a smaller number of backends.
    # pytest.param("mysql", marks=pytest.mark.extra_install),
]


exporters = {  # type: ignore
    "to_polars": lambda conn, table: conn.to_polars(table).to_dict(as_series=False),  # type: ignore
    "to_pandas": lambda conn, table: conn.to_pandas(table).to_dict(orient="list"),  # type: ignore
}


# Tests:


@pytest.mark.parametrize(
    "fixture", fixtures, ids=lambda fixture: f"{fixture.category}-{fixture.expression}"
)
@pytest.mark.parametrize("backend", backends)
@pytest.mark.parametrize("exporter_key", exporters.keys())  # type: ignore
def test_translate_table(fixture: Fixture, backend: str, exporter_key: str):
    # Sanity check: Does the polars expression have the expected result?
    lf = pl.LazyFrame(input_data[fixture.category])
    polars_output = eval(fixture.expression).collect().to_dict(as_series=False)
    assert polars_output == fixture.expected_output, "Typo in test?"

    # Convert polars to ibis, but without any data:
    lf = pl.LazyFrame(schema=lf.collect_schema())
    lf = eval(fixture.expression)
    table_name = "default_table"
    ibis_table = convert_polars_to_ibis(lf, table_name)

    # Set up target database, with data:
    input_df = pl.DataFrame(input_data[fixture.category])
    connection = get_connection(input_df, table_name=table_name, backend=backend)

    # If errors are expected, confirm that they are raised:
    export = exporters[exporter_key]  # type: ignore
    expected_backend_error = fixture.expected_backend_errors.get(backend)
    expected_exporter_error = fixture.expected_exporter_errors.get(
        f"{backend}+{exporter_key}"
    )
    if expected_error := expected_backend_error or expected_exporter_error:
        with pytest.raises(Exception, match=re.escape(expected_error)):
            export(connection, ibis_table)
        pytest.xfail(f"expected error: {expected_error}")

    # Otherwise check for approximate or exact match:
    actual_output = export(connection, ibis_table)  # type: ignore
    if tolerance := fixture.tolerance.get(backend):
        assert_approx_equal(
            actual_output,  # type: ignore
            fixture.expected_output,
            tolerance,
            f"Via ibis, {backend} does not produce output within {tolerance}",
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
        ({}, "Expected single-key tagged dict"),
        ({"Scan": {}}, "Unsupported Scan: {}"),
        (
            {"Scan": {"df": {}, "schema": {}}},
            "Unsupported Scan: {'df': {}, 'schema': {}}",
        ),
    ],
    ids=lambda plan: str(plan),
)
def test_unexpected_payloads(polars_plan, expected_error):
    with pytest.raises(Exception, match=re.escape(expected_error)):
        update_polars_to_ibis(polars_plan, None)
