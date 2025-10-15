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

polars_df = pl.DataFrame(data)
polars_lazy = polars_df.lazy()


def xfail(error, param):
    return pytest.param(param, marks=pytest.mark.xfail(raises=error))


expressions = [
    # Slice:
    # TODO: Non deterministic without order_by.
    # Different behavior with different back-ends.
    # "polars_lazy.head(1)",
    # "polars_lazy.head(2)",
    # "polars_lazy.tail(3)",
    # "polars_lazy[1:2]",
    # "polars_lazy.first()",
    # "polars_lazy.last()",
    # Sort:
    "polars_lazy.sort(by='ints')",
    "polars_lazy.sort(by=['ints', 'floats'])",
    xfail(
        UnhandledPolarsException,
        "polars_lazy.sort(by='ints', descending=True, "
        "nulls_last=True, maintain_order=True, multithreaded=True)",
    ),
    # MapFunction:
    "polars_lazy.max()",
    "polars_lazy.min()",
    xfail(AttributeError, "polars_lazy.mean()"),
    # TODO:
    # Select:
    xfail(UnhandledPolarsException, "polars_lazy.count()"),
    xfail(AssertionError, "polars_lazy.bottom_k(1, by=pl.col('ints'), reverse=True)"),
    xfail(UnhandledPolarsException, "polars_lazy.drop(['ints'], strict=True)"),
    # HStack:
    xfail(UnhandledPolarsException, "polars_lazy.cast({'ints': pl.Float32})"),
]


def connect_to_polars(table_name, polars_df):
    connection = ibis.polars.connect(tables={table_name: polars_df})
    return connection


def connect_to_sqlite(table_name, polars_df):
    connection = ibis.sqlite.connect()
    connection.create_table(table_name, polars_df)
    return connection


connect_tos = [
    connect_to_polars,
    connect_to_sqlite,
]


@pytest.mark.parametrize(
    "str_expression",
    [
        # Slice:
        # TODO: Non deterministic without order_by.
        # Different behavior with different back-ends.
        # "polars_lazy.head(1)",
        # "polars_lazy.head(2)",
        # "polars_lazy.tail(3)",
        # "polars_lazy[1:2]",
        # "polars_lazy.first()",
        # "polars_lazy.last()",
        # Sort:
        "polars_lazy.sort(by='ints')",
        "polars_lazy.sort(by=['ints', 'floats'])",
        xfail(
            UnhandledPolarsException,
            "polars_lazy.sort(by='ints', descending=True, "
            "nulls_last=True, maintain_order=True, multithreaded=True)",
        ),
        # MapFunction:
        "polars_lazy.max()",
        "polars_lazy.min()",
        xfail(AttributeError, "polars_lazy.mean()"),
        # TODO:
        # Select:
        xfail(UnhandledPolarsException, "polars_lazy.count()"),
        xfail(
            AssertionError, "polars_lazy.bottom_k(1, by=pl.col('ints'), reverse=True)"
        ),
        xfail(UnhandledPolarsException, "polars_lazy.drop(['ints'], strict=True)"),
        # HStack:
        xfail(UnhandledPolarsException, "polars_lazy.cast({'ints': pl.Float32})"),
    ],
)
@pytest.mark.parametrize("connect", connect_tos)
def test_polars_to_ibis(str_expression, connect):
    # Expressions as strings just for readability of test output.
    polars_expression = eval(str_expression)
    # Call collect() early, so if there's a typo we don't go any farther.
    expected = polars_expression.collect().to_dicts()

    table_name = "default_table"
    ibis_unbound_table = polars_to_ibis(polars_expression, table_name=table_name)

    via_ibis = (
        connect(table_name, polars_df)
        .to_pandas(ibis_unbound_table)
        .to_dict(orient="records")
    )

    assert via_ibis == expected
