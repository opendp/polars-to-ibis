import ibis
import polars as pl
import pytest

from polars_to_ibis import TABLE_NAME, polars_to_ibis

ibis.set_backend("polars")

data = {
    "ints": [1, 2, 3, 4],
    "floats": [0.1, 0.2, 0.3, 0.4],
    "strings": ["a", "b", "c", "d"],
    "bools": [True, True, False, False],
}

polars_df = pl.DataFrame(data)
polars_lazy = polars_df.lazy()

expressions = [
    "polars_lazy.head(1)",
    "polars_lazy.head(2)",  # Expected failure!
]


@pytest.mark.parametrize("expression", expressions)
def test_polars_to_ibis(expression):
    polars_lazy_expr = eval(expression)
    ibis_unbound_table = polars_to_ibis(polars_lazy_expr)

    connection = ibis.polars.connect(tables={TABLE_NAME: polars_df})
    via_ibis = connection.to_polars(ibis_unbound_table)

    assert polars_lazy_expr.collect().to_dicts() == via_ibis.to_dicts()
