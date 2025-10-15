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


expressions = [
    # Slice:
    # TODO: Non deterministic without order_by.
    # Different behavior with different back-ends.
    # "lf.head(1)",
    # "lf.head(2)",
    # "lf.tail(3)",
    # "lf[1:2]",
    # "lf.first()",
    # "lf.last()",
    # Sort:
    "lf.sort(by='ints')",
    "lf.sort(by=['ints', 'floats'])",
    xfail(
        UnhandledPolarsException,
        "lf.sort(by='ints', descending=True, "
        "nulls_last=True, maintain_order=True, multithreaded=True)",
    ),
    # MapFunction:
    "lf.max()",
    "lf.min()",
    xfail(AttributeError, "lf.mean()"),
    # TODO:
    # Select:
    xfail(UnhandledPolarsException, "lf.count()"),
    xfail(AssertionError, "lf.bottom_k(1, by=pl.col('ints'), reverse=True)"),
    xfail(UnhandledPolarsException, "lf.drop(['ints'], strict=True)"),
    # HStack:
    xfail(UnhandledPolarsException, "lf.cast({'ints': pl.Float32})"),
]


@pytest.mark.parametrize("str_expression", expressions)
@pytest.mark.parametrize(
    "backend",
    [
        "polars",
        "sqlite",
        "duckdb",
    ],
)
def test_polars_to_ibis(str_expression, backend):
    # Expressions as strings just for readability of test output.
    polars_expression = eval(str_expression)
    # Call collect() early, so if there's a typo we don't go any farther.
    expected = polars_expression.collect().to_dicts()

    table_name = "default_table"
    ibis_unbound_table = polars_to_ibis(polars_expression, table_name=table_name)

    connection = getattr(ibis, backend).connect()
    connection.create_table(table_name, df)

    via_ibis = connection.to_pandas(ibis_unbound_table).to_dict(orient="records")

    assert via_ibis == expected
