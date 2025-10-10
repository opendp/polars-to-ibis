import ibis
import polars as pl

from polars_to_ibis import polars_expr_to_ibis

ibis.set_backend("polars")

data = {
    "ints": [1, 2, 3, 4],
    "floats": [0.1, 0.2, 0.3, 0.4],
    "strings": ["a", "b", "c", "d"],
    "bools": [True, True, False, False],
}

polars_df = pl.DataFrame(data)
ibis_table = ibis.memtable(polars_df)


def test_head():
    assert polars_df.head(1).to_dicts() == ibis_table.head(1).to_polars().to_dicts()


def test_polars_expr_to_ibis():
    pl_expr = pl.col("foo").sum().over("bar")
    ibis_deferred = polars_expr_to_ibis(pl_expr)
    assert type(ibis_deferred) is ibis.common.deferred.Deferred
    assert str(ibis_deferred) == "_.group_by('bar').agg(foo=_.foo.sum())"
