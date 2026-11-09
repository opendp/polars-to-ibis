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
    ids=lambda scenario: f"{scenario.category}-{scenario.expression}",
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
        lambda: scenario.run(frames_from_scenario).collect().to_dict(as_series=False),
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
    lf = scenario.run(frames_from_db)

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

    connection.disconnect()

    TOLERANCE = "tolerance"
    ALT_PASS = "alt_pass"
    ALT_XFAIL = "alt_xfail"
    tolerance = getattr(scenario, TOLERANCE)
    alt_pass = scenario.get_with_keys(ALT_PASS, *keys)
    alt_xfail = scenario.get_with_keys(ALT_XFAIL, *keys)

    # alt_pass and alt_xfail dicts can both be given for a scenario,
    # but only one should apply for a given set of parameters.
    assert sum(int(bool(field)) for field in [tolerance, alt_pass, alt_xfail]) <= 1, (
        f"{TOLERANCE}, {ALT_PASS}, and {ALT_XFAIL} are mutually exclusive"
    )

    # Check if result is what we expect:
    if tolerance:
        assert_approx_equal(
            actual_output,  # type: ignore
            scenario.expected_output,
            tolerance,
            f"Via ibis, {backend_name} does not produce output within {tolerance}",
        )
    else:
        expected_output = alt_pass or alt_xfail or scenario.expected_output

        assert sort_keys(actual_output) == sort_keys(expected_output), (
            f"Via ibis, {backend_name} does not produce expected output"
        )

        if alt_xfail:
            pytest.xfail("Results as expected, but definitely not correct!")


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
        assert actual_col == pytest.approx(expected_col, abs=tolerance), (
            f"{message} on {key}"
        )  # type: ignore
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


@pytest.mark.parametrize("backend_name", backend_names)
@pytest.mark.parametrize("values", [[1, None, 3], []], ids=["nonempty", "empty"])
@pytest.mark.parametrize(
    "expr",
    [
        pl.len().cast(pl.Int64),
        pl.len().cast(pl.Int64, strict=False),
        pl.len().cast(pl.Int64).alias("n"),
    ],
    ids=["cast", "nonstrict_cast", "aliased_cast"],
)
def test_cast_len(backend_name, values, expr):
    frame = pl.DataFrame({"ints": pl.Series(values, dtype=pl.Int64)})
    expected = frame.select(expr)
    backend = getattr(ibis, backend_name)
    connection = get_connection(frame, table_name="cast_len", backend=backend)
    try:
        lf = scan_database(connection, "cast_len").select(expr)
        table = convert_polars_to_ibis(lf, "cast_len", backend=backend)
        actual = connection.to_polars(table)
        assert actual.to_dict(as_series=False) == expected.to_dict(as_series=False)
        assert actual.schema == expected.schema
    finally:
        connection.disconnect()


@pytest.mark.parametrize("backend_name", backend_names)
def test_select_broadcast_empty(backend_name):
    frame = pl.DataFrame({"ints": pl.Series([], dtype=pl.Int64)})
    backend = getattr(ibis, backend_name)
    connection = get_connection(frame, table_name="empty_select", backend=backend)
    try:
        lf = scan_database(connection, "empty_select").select(
            pl.col("ints"), pl.col("ints").sum().alias("total")
        )
        result = convert_polars_to_ibis(lf, "empty_select", backend=backend)
        assert connection.to_polars(result).to_dict(as_series=False) == {
            "ints": [],
            "total": [],
        }
    finally:
        connection.disconnect()


@pytest.mark.parametrize("level", ["DEBUG", "INFO"])
def test_table_handler_called_once_with_debug_logging(caplog, level):
    import logging

    from polars_to_ibis._parse.table_handlers import TABLE_REGISTRY, table_handler

    calls = []

    @table_handler("test_debug_call")
    def counting_handler(payload, table, backend):
        calls.append(payload)
        return table

    try:
        with caplog.at_level(
            getattr(logging, level), logger="polars_to_ibis._parse.table_handlers"
        ):
            TABLE_REGISTRY["test_debug_call"]({}, table=None, backend=None)
        assert calls == [{}]
    finally:
        del TABLE_REGISTRY["test_debug_call"]


@pytest.mark.parametrize("backend_name", backend_names)
@pytest.mark.parametrize("stat", ["mean", "var", "std"])
@pytest.mark.parametrize("whole_table", [True, False], ids=["whole", "expression"])
@pytest.mark.parametrize(
    "dtype",
    [
        pl.Int64,
        pl.Float64,
        pytest.param(
            pl.Float32,
            marks=pytest.mark.xfail(
                strict=True,
                raises=AssertionError,
                reason="Reducers force Float64 instead of preserving Polars Float32",
            ),
        ),
    ],
)
def test_statistic_dtype(backend_name, stat, whole_table, dtype):
    from polars.testing import assert_frame_equal

    frame = pl.DataFrame({"x": pl.Series([1, 2, 4], dtype=dtype)})

    def query(lf):
        if whole_table:
            return getattr(lf, stat)()
        return lf.select(getattr(pl.col("x"), stat)())

    expected = query(frame.lazy()).collect()
    backend = getattr(ibis, backend_name)
    connection = get_connection(frame, table_name="statistic_dtype", backend=backend)
    try:
        lf = query(scan_database(connection, "statistic_dtype"))
        table = convert_polars_to_ibis(lf, "statistic_dtype", backend=backend)
        assert_frame_equal(connection.to_polars(table), expected)
    finally:
        connection.disconnect()


def test_named_expression_order_and_duplicates():
    from polars_to_ibis._parse.table_handlers import translate_named_exprs
    from polars_to_ibis._serialize import serialize

    frame = pl.LazyFrame({"x": [1], "y": [2]})
    table = ibis.table({"x": "int64", "y": "int64"})
    expressions = serialize(frame.select("y", pl.col("x").alias("z")))["Select"]["expr"]
    assert list(translate_named_exprs(expressions, table)) == ["y", "z"]
    with pytest.raises(ValueError, match="Duplicate output column: y"):
        translate_named_exprs(expressions + [expressions[0]], table)


def test_sort_translates_input_once(monkeypatch):
    from polars_to_ibis._parse import table_handlers
    from polars_to_ibis._serialize import serialize

    plan = serialize(pl.LazyFrame({"x": [1, 2]}).filter(pl.col("x") > 1).sort("x"))
    table = ibis.table({"x": "int64"})
    original = table_handlers.update_polars_to_ibis
    calls = []

    def counted(plan, **kwargs):
        calls.append(plan)
        return original(plan, **kwargs)

    monkeypatch.setattr(table_handlers, "update_polars_to_ibis", counted)
    table_handlers.handle_sort(plan["Sort"], table, ibis.duckdb)
    assert calls.count(plan["Sort"]["input"]) == 1


@pytest.mark.parametrize(
    "query,expected_error",
    [
        (lambda lf: lf.select(pl.col(["missing"])), "Column 'missing' is not found"),
        (lambda lf: lf.drop("missing"), "Columns not found: ['missing']"),
    ],
    ids=["explicit", "strict_exclusion"],
)
def test_missing_selector_columns(query, expected_error):
    lf = pl.LazyFrame({"x": [1]})
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        query(lf).collect()
    with pytest.raises(Exception, match=re.escape(expected_error)):
        convert_polars_to_ibis(query(lf), "missing_columns", backend=ibis.duckdb)


def test_infer_name_skips_metadata():
    from polars_to_ibis._parse.value_handlers import infer_name

    # Metadata may precede expression children; naming must not depend on dict order.
    expression = {"Function": {"function": "Negate", "input": [{"Column": "x"}]}}
    assert infer_name(expression) == (-pl.col("x")).meta.output_name()
