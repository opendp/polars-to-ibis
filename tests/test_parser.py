import re

import ibis
import polars as pl
import pytest

from polars_to_ibis import convert_polars_to_ibis, scan_database
from polars_to_ibis._parse import tags
from polars_to_ibis._parse.table_handlers import update_polars_to_ibis

from .config_parser import BaseParserScenario, input_data, parser_scenarios
from .utils import backend_names, exporters, get_connection


def sort_keys(unsorted_dict):
    # We want to make sure the expected types returned, not just values,
    # but we don't care about key order.
    dict_str = str({k: unsorted_dict[k] for k in sorted(unsorted_dict.keys())})
    return re.sub(r"nan([^ ])", r"nan ± ???\1", dict_str)


@pytest.mark.parametrize(
    "scenario",
    parser_scenarios,
    ids=lambda scenario: (f"{scenario.category}-{scenario.expression}"),
)
@pytest.mark.parametrize("backend_name", backend_names)
@pytest.mark.parametrize("exporter_key", exporters.keys())  # type: ignore
def test_parser_scenarios(
    scenario: BaseParserScenario,
    backend_name: str,
    exporter_key: str,
):
    keys = [backend_name, exporter_key]

    # Just in polars, no database involved, does the scenario have the expected output?
    frames_from_scenario = {"lf": pl.LazyFrame(input_data[scenario.category])}

    polars_output = scenario.assert_error_or_return_value(
        "polars_errors",
        *keys,
        lambda: scenario.exec(frames_from_scenario).collect().to_dict(as_series=False),
    )
    assert polars_output == scenario.expected_output, "Typo in scenario?"

    # Set up target database, with data:
    table_name = "default_table"
    input_df = pl.DataFrame(input_data[scenario.category])
    backend = getattr(ibis, backend_name)

    connection = scenario.assert_error_or_return_value(
        "connection_errors",
        *keys,
        lambda: get_connection(input_df, table_name=table_name, backend=backend),
    )

    frames_from_db = {"lf": scan_database(connection, table_name)}
    lf = scenario.exec(frames_from_db)

    ibis_table = scenario.assert_error_or_return_value(
        "convert_errors",
        *keys,
        lambda: convert_polars_to_ibis(lf, table_name, backend=backend),
    )

    # Run query on target database:
    export = exporters[exporter_key]  # type: ignore
    actual_output = scenario.assert_error_or_return_value(
        "backend_errors",
        *keys,
        lambda: export(connection, ibis_table),  # type: ignore
    )

    # Check if result is what we expect:
    if scenario.tolerance:
        assert_approx_equal(
            actual_output,  # type: ignore
            scenario.expected_output,
            scenario.tolerance,
            f"Via ibis, {backend_name} does not produce output "
            f"within {scenario.tolerance}",
        )
    else:
        expected_output = (
            scenario.get_with_keys("alternative_results", *keys)
            or scenario.expected_output
        )

        assert sort_keys(actual_output) == sort_keys(
            expected_output
        ), f"Via ibis, {backend_name} does not produce expected output"


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
    ],
    ids=lambda plan: str(plan),
)
def test_unexpected_payloads(polars_plan, expected_error):
    with pytest.raises(Exception, match=re.escape(expected_error)):
        update_polars_to_ibis(polars_plan, None, ibis.backends.sqlite)
