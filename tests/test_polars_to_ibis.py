from os import environ

import ibis
import polars as pl
import pytest

from polars_to_ibis import UnhandledPolarsException, polars_to_ibis

ibis.set_backend("polars")


def xfail(error, param):
    return pytest.param(param, marks=pytest.mark.xfail(raises=error))


mixed_data = {
    "ints": [1, 2, 3, 4],
    "floats": [0.1, 0.2, 0.3, 0.4],
    "strings": ["a", "b", "c", "d"],
    "bools": [True, True, False, False],
}
mixed_df = pl.DataFrame(mixed_data)


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


def get_connection_table_name(df, backend: str):
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


def assert_polars_to_ibis(df, expression_rows_cols, backend):
    # Expressions as strings just for readability of test output.
    (
        str_expression,
        rows,
        cols,
    ) = expression_rows_cols
    lf = df.lazy()  # noqa: F841; "lf" is used in eval()
    polars_expression = eval(str_expression)
    expected_dicts = polars_expression.collect().to_dicts()

    connection, table_name = get_connection_table_name(df, backend)
    ibis_unbound_table = polars_to_ibis(polars_expression, table_name=table_name)

    # Could use to_polars() here, but we want to be extra sure
    # that the path through Ibis does not depend on Polars.
    via_ibis_df = connection.to_pandas(ibis_unbound_table)

    assert via_ibis_df.shape == (rows, cols)
    via_ibis_dicts = via_ibis_df.to_dict(orient="records")
    assert via_ibis_dicts == expected_dicts

    # Cleanup:
    if hasattr(connection, "disconnect"):
        connection.disconnect()  # pragma: no cover


@pytest.mark.parametrize(
    "mixed_expression_rows_cols",
    mixed_expressions_rows_cols,
    ids=lambda triple: triple[0],
)
@pytest.mark.parametrize(
    "backend",
    [
        "polars",
        "sqlite",
        "duckdb",
        pytest.param("postgres", marks=pytest.mark.extra_install),
        pytest.param("mysql", marks=pytest.mark.extra_install),
    ],
)
def test_mixed_polars_to_ibis(mixed_expression_rows_cols, backend):
    assert_polars_to_ibis(mixed_df, mixed_expression_rows_cols, backend)
