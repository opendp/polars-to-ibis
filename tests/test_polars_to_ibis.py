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


def xfail_assertion(param):
    return pytest.param(param, marks=pytest.mark.xfail(raises=AssertionError))


def xfail_unhandled(param):
    return pytest.param(param, marks=pytest.mark.xfail(raises=UnhandledPolarsException))


@pytest.mark.parametrize(
    "str_expression",
    [
        "polars_lazy.head(1)",
        "polars_lazy.head(2)",
        "polars_lazy.tail(3)",
        "polars_lazy[1:2]",
        xfail_unhandled(
            "polars_lazy.sort(by='ints', descending=True, "
            "nulls_last=True, maintain_order=True, multithreaded=True)"
        ),
        xfail_unhandled("polars_lazy.count()"),
        xfail_assertion("polars_lazy.bottom_k(1, by=pl.col('ints'), reverse=True)"),
        xfail_unhandled("polars_lazy.cast({'ints': pl.Float32})"),
        xfail_unhandled("polars_lazy.drop(['ints'], strict=True)"),
    ],
)
def test_polars_to_ibis(str_expression):
    # Expressions as strings just for readability of test output.
    expression = eval(str_expression)
    # Collect early, so if there's a typo we don't go any farther.
    expected = expression.collect().to_dicts()

    table_name = "default_table"
    ibis_unbound_table = polars_to_ibis(expression, table_name=table_name)

    # TODO: Connect to a backend other than polars:
    # https://github.com/opendp/polars-to-ibis/issues/7
    connection = ibis.polars.connect(tables={table_name: polars_df})
    via_ibis = connection.to_polars(ibis_unbound_table)

    assert via_ibis.to_dicts() == expected
