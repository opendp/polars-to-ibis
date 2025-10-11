import ibis
import polars as pl
import pytest

from polars_to_ibis import polars_to_ibis

ibis.set_backend("polars")

data = {
    "ints": [1, 2, 3, 4],
    "floats": [0.1, 0.2, 0.3, 0.4],
    "strings": ["a", "b", "c", "d"],
    "bools": [True, True, False, False],
}

polars_df = pl.DataFrame(data)
polars_lazy = polars_df.lazy()


def xfail(param):
    return pytest.param(param, marks=pytest.mark.xfail(raises=AssertionError))


expressions = [
    polars_lazy.head(1),
    polars_lazy.head(2),
    polars_lazy.tail(3),
    polars_lazy[1:2],
]


@pytest.mark.parametrize("expression", expressions)
def test_polars_to_ibis(expression):
    table_name = "default_table"
    ibis_unbound_table = polars_to_ibis(expression, table_name=table_name)

    # TODO: Connect to a backend other than polars.
    connection = ibis.polars.connect(tables={table_name: polars_df})
    via_ibis = connection.to_polars(ibis_unbound_table)

    assert expression.collect().to_dicts() == via_ibis.to_dicts()
