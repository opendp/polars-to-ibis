import ibis
import polars as pl

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


def test_head():
    polars_lazy_expr = polars_lazy.head(1)
    ibis_unbound_table = polars_to_ibis(polars_lazy_expr)

    pl_con = ibis.polars.connect(tables={"unbound_table_0": polars_df})
    df_via_ibis = pl_con.to_polars(ibis_unbound_table)

    assert polars_lazy_expr.collect().to_dicts() == df_via_ibis.to_dicts()
