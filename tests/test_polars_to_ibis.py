import ibis
import polars as pl

from polars_to_ibis import polars_expr_to_ibis


def test_polars_expr_to_ibis():
    pl_expr = pl.col("foo").sum().over("bar")
    ibis_deferred = polars_expr_to_ibis(pl_expr)
    assert type(ibis_deferred) is ibis.common.deferred.Deferred
    assert str(ibis_deferred) == "_.group_by('bar').agg(foo=_.foo.sum())"
