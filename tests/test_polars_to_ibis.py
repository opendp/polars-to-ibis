import ibis
import polars as pl
import pytest

from polars_to_ibis import UnhandledPolarsException, polars_to_ibis

ibis.set_backend("polars")

data = {
    "ints": [1, 2, 3, 4],
    "floats": [0.1, 0.2, 0.3, 0.4],
    "strings": ["a", "b", "c", "d"],
    "bools": [True, True, False, False],
}

df = pl.DataFrame(data)
lf = df.lazy()


def xfail(error, param):
    return pytest.param(param, marks=pytest.mark.xfail(raises=error))


expressions_rows_cols = [
    # Slice:
    # NOTE: Non-deterministic on some backends without sort().
    ("lf.sort(by='ints').head(1)", 1, 4),
    ("lf.sort(by='ints').head(2)", 2, 4),
    # ("lf.tail(3)", 3, 4), # Fails sqlite and duckdb
    ("lf.sort(by='ints')[1:3]", 2, 4),
    ("lf.sort(by='ints').first()", 1, 4),
    # ("lf.sort(by='ints').last()", 1, 4), # Fails sqlite and duckdb
    # Sort:
    ("lf.sort(by='ints')", 4, 4),
    ("lf.sort(by=['ints', 'floats'])", 4, 4),
    # MapFunction:
    ("lf.max()", 1, 4),
    ("lf.min()", 1, 4),
    xfail(AttributeError, ("lf.mean()", 1, 4)),
    # TODO:
    # Select:
    xfail(UnhandledPolarsException, ("lf.count()", 0, 0)),
    xfail(AssertionError, ("lf.bottom_k(1, by=pl.col('ints'), reverse=True)", 0, 0)),
    xfail(UnhandledPolarsException, ("lf.drop(['ints'], strict=True)", 0, 0)),
    # HStack:
    xfail(UnhandledPolarsException, ("lf.cast({'ints': pl.Float32})", 0, 0)),
]


@pytest.mark.parametrize(
    "expression_rows_cols", expressions_rows_cols, ids=lambda triple: triple[0]
)
@pytest.mark.parametrize(
    "backend",
    [
        "polars",
        "sqlite",
        "duckdb",
    ],
)
def test_polars_to_ibis(expression_rows_cols, backend):
    # Expressions as strings just for readability of test output.
    (
        str_expression,
        rows,
        cols,
    ) = expression_rows_cols
    polars_expression = eval(str_expression)
    # Call collect() early, so if there's a typo we don't go any farther.
    expected = polars_expression.collect().to_dicts()

    table_name = "default_table"
    ibis_unbound_table = polars_to_ibis(polars_expression, table_name=table_name)

    connection = getattr(ibis, backend).connect()
    connection.create_table(table_name, df)

    # Could use to_polars() here, but we want to be extra sure
    # that the path through Ibis does not depend on Polars.
    via_ibis_df = connection.to_pandas(ibis_unbound_table)
    assert via_ibis_df.shape == (rows, cols)

    via_ibis_dicts = via_ibis_df.to_dict(orient="records")
    assert via_ibis_dicts == expected

    # SQLite on Python 3.13, but not 3.10, complains if we don't clean up.
    if hasattr(connection, "disconnect"):  # pragma: no cover
        connection.disconnect()
