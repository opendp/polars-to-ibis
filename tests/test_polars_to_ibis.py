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


def xfail(error: type[BaseException], param: tuple[str, str, int, int]):
    return pytest.param(param, marks=pytest.mark.xfail(raises=error))


#
# Test Fixtures
#


df = {
    "namespace": pl.DataFrame(
        {
            "ints": [1, 2, 3, 4],
            # TODO: Add more columns, once polars namespace works on at least one
        }
    ),
    "mixed": pl.DataFrame(
        {
            "ints": [1, 2, 3, 4],
            "floats": [0.1, 0.2, 0.3, 0.4],
            "strings": ["a", "b", "c", "d"],
            "bools": [True, True, False, False],
        }
    ),
    "numeric": pl.DataFrame(
        {
            "ints": [1, 2, 3, 4],
            "floats": [0.1, 0.2, 0.3, 0.4],
        }
    ),
}


@pl.api.register_lazyframe_namespace("demo")
class DemoOperations:  # type: ignore
    def __init__(self, lf: pl.LazyFrame) -> None:
        self._lf = lf

    def no_op(self) -> pl.LazyFrame:
        return self._lf

    def zero(self) -> pl.LazyFrame:
        return self._lf.with_columns(pl.lit(0))


category_expressions_rows_cols = [
    #
    # Namespace:
    #
    ("namespace", "lf.demo.no_op()", 4, 1),
    xfail(UnhandledPolarsException, ("namespace", "lf.demo.zero()", 4, 1)),
    #
    # Slice:
    #
    # NOTE: Non-deterministic on some backends without sort().
    ("mixed", "lf.sort(by='ints').head(1)", 1, 4),
    ("mixed", "lf.sort(by='ints').head(2)", 2, 4),
    # ("mixed", "lf.tail(3)", 3, 4), # Fails sqlite and duckdb
    ("mixed", "lf.sort(by='ints')[1:3]", 2, 4),
    ("mixed", "lf.sort(by='ints').first()", 1, 4),
    # ("mixed", "lf.sort(by='ints').last()", 1, 4), # Fails sqlite and duckdb
    #
    # Sort:
    #
    ("mixed", "lf.sort(by='ints')", 4, 4),
    ("mixed", "lf.sort(by=['ints', 'floats'])", 4, 4),
    #
    # MapFunction:
    #
    ("mixed", "lf.max()", 1, 4),
    ("mixed", "lf.min()", 1, 4),
    xfail(
        AttributeError, ("mixed", "lf.mean()", 1, 4)
    ),  # mean() doesn't work for strings
    #
    # Column:
    #
    # TODO: Not working!
    # ("mixed", "lf.select('ints')", 4, 1),
    #
    ("mixed", "lf.count()", 1, 4),
    # Ibis returns a single number; Polars returns a DF with a count in each column:
    # ("mixed", "lf.bottom_k(1, by=pl.col('ints'), reverse=True)", 0, 0)),
    xfail(UnhandledPolarsException, ("mixed", "lf.drop(['ints'], strict=True)", 0, 0)),
    #
    # HStack:
    #
    xfail(UnhandledPolarsException, ("mixed", "lf.cast({'ints': pl.Float32})", 0, 0)),
    #
    # Simple numeric:
    # All of the methods listed on:
    # https://docs.pola.rs/api/python/stable/reference/lazyframe/aggregation.html
    #
    ("numeric", "lf.count()", 1, 2),
    ("numeric", "lf.max()", 1, 2),
    ("numeric", "lf.mean()", 1, 2),
    ("numeric", "lf.median()", 1, 2),
    ("numeric", "lf.min()", 1, 2),
    ("numeric", "lf.null_count()", 1, 2),
    # ("numeric", "lf.quantile(quantile[, interpolation])
    ("numeric", "lf.std()", 1, 2),
    # TODO: ("numeric", "lf.std(2)", 1, 2),
    ("numeric", "lf.sum()", 1, 2),
    ("numeric", "lf.var()", 1, 2),
]


backends = [
    "polars",
    "sqlite",
    "duckdb",
    # TODO: Restore postgres: https://github.com/opendp/polars-to-ibis/issues/35
    # pytest.param("postgres", marks=pytest.mark.extra_install),
    # TODO: Restore mysql: https://github.com/opendp/polars-to-ibis/issues/34
    # pytest.param("mysql", marks=pytest.mark.extra_install),
]


#
# Tests
#


@pytest.mark.parametrize(
    "category_expressions_rows_cols",
    category_expressions_rows_cols,
    ids=lambda quad: f"{quad[0]}: {quad[1]}",
)
@pytest.mark.parametrize("backend", backends)
def test_polars_to_ibis(
    category_expressions_rows_cols: tuple[str, str, int, int], backend: str
):
    if backend == "sqlite" and "median" in category_expressions_rows_cols[1]:
        pytest.xfail("TODO: Compilation rule for 'Median' operation is not defined")
    if backend == "polars" and "count" in category_expressions_rows_cols[1]:
        pytest.xfail("TODO: No translation rule for WindowFunction")
    assert_polars_to_ibis(
        df[category_expressions_rows_cols[0]],
        category_expressions_rows_cols[1:],
        backend,
    )
