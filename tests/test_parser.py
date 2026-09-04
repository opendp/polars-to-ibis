import re

import polars as pl
import pytest

from polars_to_ibis import convert_polars_to_ibis, scan_database
from polars_to_ibis._parse import tags
from polars_to_ibis._parse.table_handlers import update_polars_to_ibis

from .config_parser import ParserScenario, input_data, parser_scenarios
from .utils import assert_error_or_none, backends, exporters, get_connection


@pytest.mark.parametrize(
    "scenario",
    parser_scenarios,
    ids=lambda scenario: (f"{scenario.category}-{scenario.expression}"),
)
def test_scenario_consistency(scenario: ParserScenario):
    # Does the polars expression have the expected result?
    globals = {"lf": pl.LazyFrame(input_data[scenario.category]), "pl": pl}
    polars_output = (
        eval(scenario.expression, globals).collect().to_dict(as_series=False)
    )
    assert polars_output == scenario.expected_output, "Typo in scenario?"


@pytest.mark.parametrize(
    "scenario",
    parser_scenarios,
    ids=lambda scenario: (f"{scenario.category}-{scenario.expression}"),
)
@pytest.mark.parametrize("backend", backends)
@pytest.mark.parametrize("exporter_key", exporters.keys())  # type: ignore
def test_translate_table_new(
    scenario: ParserScenario,
    backend: str,
    exporter_key: str,
):
    # Set up target database, with data:
    table_name = "default_table"
    input_df = pl.DataFrame(input_data[scenario.category])
    connection = assert_error_or_none(
        "connection_error",
        scenario.connection_errors.get(backend),
        lambda: get_connection(input_df, table_name=table_name, backend=backend),
    )

    globals = {"lf": scan_database(connection, table_name), "pl": pl}
    lf = eval(scenario.expression, globals)

    ibis_table = assert_error_or_none(
        "convert_error",
        scenario.convert_errors.get(f"polars=={pl.__version__}"),
        lambda: convert_polars_to_ibis(lf, table_name),
    )

    # Run query on target database:
    export = exporters[exporter_key]  # type: ignore
    expected_backend_error = (
        scenario.backend_errors.get(backend)
        or scenario.backend_errors.get(f"{backend}+{exporter_key}")
        or scenario.backend_errors.get(
            f"{backend}+{exporter_key}+polars=={pl.__version__}"
        )
    )
    actual_output = assert_error_or_none(
        "backend_error",
        expected_backend_error,
        lambda: export(connection, ibis_table),  # type: ignore
    )

    # Check if result is what we expect:
    if scenario.tolerance:
        assert_approx_equal(
            actual_output,  # type: ignore
            scenario.expected_output,
            scenario.tolerance,
            f"Via ibis, {backend} does not produce output "
            f"within {scenario.tolerance}",
        )
    else:
        assert (
            actual_output == scenario.expected_output
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
            {tags.table.SCAN: {}},
            "Unsupported Scan",
        ),
        (
            {tags.table.SCAN: {"df": {}, "schema": {}}},
            "Unsupported Scan",
        ),
        (
            # When/if Count *is* supported, this test won't work.
            {
                tags.table.SELECT: {
                    "expr": [
                        {
                            tags.value.AGG: {
                                "Count": {
                                    "include_nulls": False,
                                    "input": {"Selector": "Wildcard"},
                                }
                            }
                        }
                    ],
                    "input": {
                        tags.table.DATA_FRAME_SCAN: {"df": {}, "schema": {"fields": {}}}
                    },
                    "options": {
                        "duplicate_check": True,
                        "run_parallel": True,
                        "should_broadcast": True,
                    },
                }
            },
            # Check that the input data structure is shown in error message.
            "No value handler for 'Count'",
        ),
    ],
    ids=lambda plan: str(plan),
)
def test_unexpected_payloads(polars_plan, expected_error):
    with pytest.raises(Exception, match=re.escape(expected_error)):
        update_polars_to_ibis(polars_plan, None)
