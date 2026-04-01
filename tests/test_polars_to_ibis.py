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
    df: pl.DataFrame,
    expression: str,
    backend: str,
):
    # Expressions as strings just for readability of test output.
    lf = df.lazy()  # type: ignore # noqa: F841; "lf" is used in eval()

    polars_expression = eval(expression)
    expected_dicts = polars_expression.collect().to_dicts()

    connection, table_name = get_connection_table_name(df, backend)
    ibis_unbound_table = polars_to_ibis(polars_expression, table_name=table_name)

    # Could use to_polars() here, but we want to be extra sure
    # that the path through Ibis does not depend on Polars.
    via_ibis_df = connection.to_pandas(ibis_unbound_table)

    via_ibis_dicts = via_ibis_df.to_dict(orient="records")
    if via_ibis_df.shape[0] == 1:
        # PosgreSQL shows differences in the last digit of var(),
        # so a slightly looser test:
        # (approx() doesn't work with deeper data structures.)
        assert via_ibis_dicts[0] == pytest.approx(expected_dicts[0])  # type: ignore
    else:
        assert via_ibis_dicts == expected_dicts

    # Cleanup:
    if hasattr(connection, "disconnect"):
        connection.disconnect()  # pragma: no cover


def xfail(error: type[BaseException], param: tuple[str, str]):
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


category_expressions = [
    #
    # Namespace:
    #
    ("namespace", "lf.demo.no_op()"),
    xfail(UnhandledPolarsException, ("namespace", "lf.demo.zero()")),
    #
    # Slice:
    #
    # NOTE: Non-deterministic on some backends without sort().
    ("mixed", "lf.sort(by='ints').head(1)"),
    ("mixed", "lf.sort(by='ints').head(2)"),
    # ("mixed", "lf.tail(3)"), # Fails sqlite and duckdb
    ("mixed", "lf.sort(by='ints')[1:3]"),
    ("mixed", "lf.sort(by='ints').first()"),
    # ("mixed", "lf.sort(by='ints').last()"), # Fails sqlite and duckdb
    #
    # Sort:
    #
    ("mixed", "lf.sort(by='ints')"),
    ("mixed", "lf.sort(by=['ints', 'floats'])"),
    #
    # MapFunction:
    #
    ("mixed", "lf.max()"),
    ("mixed", "lf.min()"),
    xfail(AttributeError, ("mixed", "lf.mean()")),  # mean() doesn't work for strings
    #
    # Column:
    #
    # TODO: Not working!
    # ("mixed", "lf.select('ints')"),
    #
    ("mixed", "lf.count()"),
    # Ibis returns a single number; Polars returns a DF with a count in each column:
    # ("mixed", "lf.bottom_k(1, by=pl.col('ints'), reverse=True)")),
    xfail(UnhandledPolarsException, ("mixed", "lf.drop(['ints'], strict=True)")),
    #
    # HStack:
    #
    xfail(UnhandledPolarsException, ("mixed", "lf.cast({'ints': pl.Float32})")),
    #
    # Simple numeric:
    # All of the methods listed on:
    # https://docs.pola.rs/api/python/stable/reference/lazyframe/aggregation.html
    #
    ("numeric", "lf.count()"),
    ("numeric", "lf.max()"),
    ("numeric", "lf.mean()"),
    ("numeric", "lf.median()"),
    ("numeric", "lf.min()"),
    ("numeric", "lf.null_count()"),
    # ("numeric", "lf.quantile(quantile[, interpolation])
    ("numeric", "lf.std()"),
    # TODO: ("numeric", "lf.std(2)"),
    ("numeric", "lf.sum()"),
    ("numeric", "lf.var()"),
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
    "category_expressions",
    category_expressions,
    ids=lambda cat_ex: f"{cat_ex[0]}: {cat_ex[1]}",
)
@pytest.mark.parametrize("backend", backends)
def test_polars_to_ibis(category_expressions: tuple[str, str], backend: str):
    (category, expression) = category_expressions
    if backend == "sqlite" and "median" in expression:
        pytest.xfail("TODO: Compilation rule for 'Median' operation is not defined")
    if backend == "polars" and "count" in expression:
        pytest.xfail("TODO: No translation rule for WindowFunction")
    assert_polars_to_ibis(
        df=df[category],
        expression=expression,
        backend=backend,
    )
