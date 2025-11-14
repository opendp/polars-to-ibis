from os import environ

import ibis  # pyright: ignore[reportMissingTypeStubs]
import polars as pl
import pytest

from polars_to_ibis import UnhandledPolarsException, polars_to_ibis

ibis.set_backend("polars")

#
# Utilities
#


def get_connection_table_name(df: pl.DataFrame, backend: str):
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
    table_name = "default_table"

    # Ensure a clean slate.
    # Each backend raises its own error type.
    try:
        connection.drop_table(table_name)
    except BaseException:  # noqa: B036
        pass
    connection.create_table(table_name, df)

    return (connection, table_name)


def assert_polars_to_ibis(
    df: pl.DataFrame, expression_rows_cols: tuple[str, int, int], backend: str
):
    # Expressions as strings just for readability of test output.
    (
        str_expression,
        rows,
        cols,
    ) = expression_rows_cols
    lf = df.lazy()  # type: ignore # noqa: F841; "lf" is used in eval()
    polars_expression = eval(str_expression)
    expected_dicts = polars_expression.collect().to_dicts()

    connection, table_name = get_connection_table_name(df, backend)
    ibis_unbound_table = polars_to_ibis(polars_expression, table_name=table_name)

    # Could use to_polars() here, but we want to be extra sure
    # that the path through Ibis does not depend on Polars.
    via_ibis_df = connection.to_pandas(ibis_unbound_table)

    assert via_ibis_df.shape == (rows, cols)
    via_ibis_dicts = via_ibis_df.to_dict(orient="records")

    if rows == 1:
        # PosgreSQL shows differences in the last digit of var(),
        # so a slightly looser test:
        # (approx() doesn't work with deeper data structures.)
        assert via_ibis_dicts[0] == pytest.approx(expected_dicts[0])  # type: ignore
    else:
        assert via_ibis_dicts == expected_dicts

    # Cleanup:
    if hasattr(connection, "disconnect"):
        connection.disconnect()  # pragma: no cover


def xfail(error: type[BaseException], param: tuple[str, int, int]):
    return pytest.param(param, marks=pytest.mark.xfail(raises=error))


#
# Test Fixtures
#


mixed_data = {
    "ints": [1, 2, 3, 4],
    "floats": [0.1, 0.2, 0.3, 0.4],
    "strings": ["a", "b", "c", "d"],
    "bools": [True, True, False, False],
}
mixed_df = pl.DataFrame(mixed_data)


numeric_data = {
    "ints": [1, 2, 3, 4],
    "floats": [0.1, 0.2, 0.3, 0.4],
}
numeric_df = pl.DataFrame(numeric_data)


mixed_expressions_rows_cols = [
    #
    # Slice:
    #
    # NOTE: Non-deterministic on some backends without sort().
    ("lf.sort(by='ints').head(1)", 1, 4),
    ("lf.sort(by='ints').head(2)", 2, 4),
    # ("lf.tail(3)", 3, 4), # Fails sqlite and duckdb
    ("lf.sort(by='ints')[1:3]", 2, 4),
    ("lf.sort(by='ints').first()", 1, 4),
    # ("lf.sort(by='ints').last()", 1, 4), # Fails sqlite and duckdb
    #
    # Sort:
    #
    ("lf.sort(by='ints')", 4, 4),
    ("lf.sort(by=['ints', 'floats'])", 4, 4),
    #
    # MapFunction:
    #
    ("lf.max()", 1, 4),
    ("lf.min()", 1, 4),
    xfail(AttributeError, ("lf.mean()", 1, 4)),  # mean() doesn't work for strings
    #
    # Column:
    #
    # TODO: Not working!
    # ("lf.select('ints')", 4, 1),
    #
    # Polars 1.32 raises TypeError:
    # ("lf.count()", 0, 0),
    # Ibis returns a single number; Polars returns a DF with a count in each column:
    # ("lf.bottom_k(1, by=pl.col('ints'), reverse=True)", 0, 0)),
    xfail(UnhandledPolarsException, ("lf.drop(['ints'], strict=True)", 0, 0)),
    #
    # HStack:
    #
    xfail(UnhandledPolarsException, ("lf.cast({'ints': pl.Float32})", 0, 0)),
]
numeric_expressions_rows_cols = [
    # All of the methods listed on:
    # https://docs.pola.rs/api/python/stable/reference/lazyframe/aggregation.html
    # ("lf.count()", 1, 2),
    ("lf.max()", 1, 2),
    ("lf.mean()", 1, 2),
    # ("lf.median()", 1, 2),
    ("lf.min()", 1, 2),
    # ("lf.null_count()", 1, 2),
    # ("lf.quantile(quantile[, interpolation])
    ("lf.std()", 1, 2),
    # TODO: ("lf.std(2)", 1, 2),
    ("lf.sum()", 1, 2),
    ("lf.var()", 1, 2),
]


backends = [
    "polars",
    "sqlite",
    "duckdb",
    pytest.param("postgres", marks=pytest.mark.extra_install),
    pytest.param("mysql", marks=pytest.mark.extra_install),
]


#
# Tests
#


@pytest.mark.parametrize(
    "mixed_expressions_rows_cols",
    mixed_expressions_rows_cols,
    ids=lambda triple: triple[0],
)
@pytest.mark.parametrize("backend", backends)
def test_mixed_polars_to_ibis(
    mixed_expressions_rows_cols: tuple[str, int, int], backend: str
):
    assert_polars_to_ibis(mixed_df, mixed_expressions_rows_cols, backend)


@pytest.mark.parametrize(
    "numeric_expressions_rows_cols",
    numeric_expressions_rows_cols,
    ids=lambda triple: triple[0],
)
@pytest.mark.parametrize("backend", backends)
def test_numeric_polars_to_ibis(
    numeric_expressions_rows_cols: tuple[str, int, int], backend: str
):
    assert_polars_to_ibis(numeric_df, numeric_expressions_rows_cols, backend)
