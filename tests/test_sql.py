import polars as pl


def test_sql():
    lf = pl.LazyFrame({"a": [1, 2, 3, 4], "b": [4, 5, 6, 7]})  # noqa: F841
    query = pl.sql("SELECT SUM(a) FROM lf")
    result = query.collect().to_dict(as_series=False)
    assert result == {"a": [10]}
