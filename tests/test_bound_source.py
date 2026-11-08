"""Bind one symbolic Polars source to an existing Ibis relation, SQL-free."""

from contextlib import contextmanager

import ibis
import polars as pl
import pytest
from ibis.common.exceptions import IbisError
from polars.testing import assert_frame_equal

from polars_to_ibis import convert_polars_to_ibis


@pytest.fixture
def connection():
    connection = ibis.duckdb.connect()
    yield connection
    connection.disconnect()


@contextmanager
def no_queries(monkeypatch, connection):
    def forbidden(*args, **kwargs):
        pytest.fail("Conversion must not execute SQL or materialize private data")

    with monkeypatch.context() as patch:
        for method in (
            "raw_sql",
            "sql",
            "execute",
            "to_polars",
            "to_pyarrow",
            "to_pyarrow_batches",
        ):
            patch.setattr(connection, method, forbidden)
        yield


def test_preserves_filtered_projected_relation(connection, monkeypatch):
    data = pl.DataFrame({"x": [1, 2, 3], "keep": [False, True, True]})
    base = connection.create_table("source", data)
    # Rename and compute a column: substituting the base table cannot work.
    bound = base.filter(base.keep).select(value=(base.x * 10).cast("int32"))
    symbolic = pl.LazyFrame(schema={"value": pl.Int32})
    plan = symbolic.filter(pl.col("value") > 20).select(
        pl.col("value").sum().alias("total"), pl.len().alias("n")
    )
    with no_queries(monkeypatch, connection):
        assert convert_polars_to_ibis(symbolic, table=bound) is bound
        translated = convert_polars_to_ibis(plan, table=bound)
        assert translated._find_backend() is connection
    assert connection.to_polars(translated).to_dict(as_series=False) == {
        "total": [30],
        "n": [1],
    }


@pytest.mark.parametrize("empty", [False, True])
def test_bound_nulls_and_empty_results(connection, monkeypatch, empty):
    data = pl.DataFrame({"x": pl.Series([1, None, 3], dtype=pl.Int32)})
    if empty:
        data = data.clear()
    bound = connection.create_table("source", data)
    exprs = [pl.col("x").sum(), pl.len().cast(pl.UInt64).alias("n")]
    with no_queries(monkeypatch, connection):
        translated = convert_polars_to_ibis(
            pl.LazyFrame(schema=data.schema).select(exprs), table=bound
        )
    assert_frame_equal(connection.to_polars(translated), data.select(exprs))


@pytest.mark.parametrize(
    "dtype",
    [
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.Float32,
        pl.Float64,
        pl.String,
        pl.Boolean,
        pl.Binary,
    ],
)
def test_exact_supported_types(connection, monkeypatch, dtype):
    data = pl.DataFrame(schema={"value": dtype})
    bound = connection.create_table("source", data)
    with no_queries(monkeypatch, connection):
        assert convert_polars_to_ibis(data.lazy(), table=bound) is bound


def test_nullability_is_not_an_analysis_guarantee(connection, monkeypatch):
    source = connection.create_table("source", pl.DataFrame({"x": [1]}))
    bound = source.select(value=ibis.literal(1, type="!int32"))
    assert not bound.schema()["value"].nullable
    with no_queries(monkeypatch, connection):
        assert (
            convert_polars_to_ibis(
                pl.LazyFrame(schema={"value": pl.Int32}), table=bound
            )
            is bound
        )


@pytest.mark.parametrize(
    "schema",
    [
        {"renamed": pl.Int32, "other": pl.String},
        {"other": pl.String, "value": pl.Int32},
        {"value": pl.Int32},
        {"value": pl.Int32, "other": pl.String, "extra": pl.Int8},
        {"value": pl.Int64, "other": pl.String},
        {"value": pl.Int16, "other": pl.String},
        {"value": pl.UInt32, "other": pl.String},
        {"value": pl.Float32, "other": pl.String},
    ],
    ids=["name", "order", "missing", "extra", "wider", "narrower", "unsigned", "float"],
)
def test_full_input_schema_must_match(connection, monkeypatch, schema):
    bound = connection.create_table(
        "source", pl.DataFrame(schema={"value": pl.Int32, "other": pl.String})
    )
    # A projection must not hide mismatches in unused input columns.
    plan = pl.LazyFrame(schema=schema).select(pl.len())
    with (
        no_queries(monkeypatch, connection),
        pytest.raises(ValueError, match="full Polars input schema"),
    ):
        convert_polars_to_ibis(plan, table=bound)


@pytest.mark.parametrize(
    "symbolic_dtype,bound_dtype",
    [(pl.Float32, pl.Float64), (pl.Float64, pl.Float32), (pl.Int32, pl.UInt32)],
)
def test_exact_width_and_signedness(
    connection, monkeypatch, symbolic_dtype, bound_dtype
):
    bound = connection.create_table("source", pl.DataFrame(schema={"x": bound_dtype}))
    with (
        no_queries(monkeypatch, connection),
        pytest.raises(ValueError, match="exact supported types"),
    ):
        convert_polars_to_ibis(pl.LazyFrame(schema={"x": symbolic_dtype}), table=bound)


def test_unsupported_type_fails_before_query(connection, monkeypatch):
    data = pl.DataFrame(schema={"x": pl.Date})
    bound = connection.create_table("source", data)
    with (
        no_queries(monkeypatch, connection),
        pytest.raises(ValueError, match="No python type defined for Date"),
    ):
        convert_polars_to_ibis(data.lazy(), table=bound)


@pytest.mark.parametrize("mixed", [False, True])
def test_unbound_source_rejected(connection, monkeypatch, mixed):
    table = ibis.table({"x": "int32"}, name="unbound")
    if mixed:
        base = connection.create_table("source", pl.DataFrame({"y": [1]}))
        table = base.cross_join(table)
    with (
        no_queries(monkeypatch, connection),
        pytest.raises(ValueError, match="unbound sources"),
    ):
        convert_polars_to_ibis(pl.LazyFrame(schema={"x": pl.Int32}), table=table)


def test_backend_free_table_rejected(connection, monkeypatch):
    with (
        no_queries(monkeypatch, connection),
        pytest.raises(IbisError, match="no backends"),
    ):
        convert_polars_to_ibis(
            pl.LazyFrame(schema={"x": pl.Int8}),
            table=ibis.literal(1).name("x").as_table(),
        )


def test_multiple_backends_rejected(connection, monkeypatch, tmp_path):
    base = connection.create_table("source", pl.DataFrame({"x": [1]}))
    other = ibis.duckdb.connect(tmp_path / "other.duckdb")
    try:
        second = other.create_table("second", pl.DataFrame({"y": [2]}))
        bound = base.cross_join(second)
        with (
            no_queries(monkeypatch, connection),
            no_queries(monkeypatch, other),
            pytest.raises(IbisError, match="Multiple backends"),
        ):
            convert_polars_to_ibis(
                pl.LazyFrame(schema={"x": pl.Int64, "y": pl.Int64}), table=bound
            )
    finally:
        other.disconnect()


def test_ambiguous_arguments_rejected(connection, monkeypatch):
    bound = connection.create_table("source", pl.DataFrame({"x": [1]}))
    symbolic = pl.LazyFrame(schema={"x": pl.Int64})
    other = ibis.duckdb.connect()
    try:
        with no_queries(monkeypatch, connection), no_queries(monkeypatch, other):
            with pytest.raises(ValueError, match="table_name"):
                convert_polars_to_ibis(symbolic, "source", table=bound)
            with pytest.raises(ValueError, match="backend instance"):
                convert_polars_to_ibis(symbolic, backend=other, table=bound)
            with pytest.raises(ValueError, match="backend instance"):
                convert_polars_to_ibis(symbolic, backend=ibis.duckdb, table=bound)
            assert (
                convert_polars_to_ibis(symbolic, backend=connection, table=bound)
                is bound
            )
    finally:
        other.disconnect()


@pytest.mark.parametrize(
    "kwargs", [{}, {"table_name": "source"}, {"backend": ibis.duckdb}]
)
def test_legacy_arguments_required(kwargs):
    with pytest.raises(TypeError, match="table_name and backend"):
        convert_polars_to_ibis(pl.LazyFrame(schema={"x": pl.Int64}), **kwargs)


def test_non_table_rejected():
    with pytest.raises(TypeError, match="bound ibis.Table"):
        convert_polars_to_ibis(pl.LazyFrame(schema={"x": pl.Int64}), table="source")


def test_multiple_polars_sources_rejected(connection, monkeypatch):
    bound = connection.create_table("source", pl.DataFrame({"x": [1]}))
    symbolic = pl.LazyFrame(schema={"x": pl.Int64})
    with (
        no_queries(monkeypatch, connection),
        pytest.raises(ValueError, match="exactly one DataFrameScan"),
    ):
        convert_polars_to_ibis(pl.concat([symbolic, symbolic]), table=bound)


def test_non_dataframe_scan_rejected(connection, monkeypatch, tmp_path):
    bound = connection.create_table("source", pl.DataFrame({"x": [1]}))
    path = tmp_path / "data.csv"
    path.write_text("x\n1\n")
    with (
        no_queries(monkeypatch, connection),
        pytest.raises(ValueError, match="exactly one DataFrameScan"),
    ):
        convert_polars_to_ibis(pl.scan_csv(path, schema={"x": pl.Int64}), table=bound)


def test_unsupported_operation_rejected(connection, monkeypatch):
    bound = connection.create_table("source", pl.DataFrame({"x": [1]}))
    with no_queries(monkeypatch, connection), pytest.raises(NotImplementedError):
        convert_polars_to_ibis(
            pl.LazyFrame(schema={"x": pl.Int64}).reverse(), table=bound
        )


@pytest.mark.parametrize("positional", [False, True])
def test_legacy_unbound_path_unchanged(connection, monkeypatch, positional):
    data = pl.DataFrame({"x": [1, 2, 3]})
    connection.create_table("source", data)
    plan = data.lazy().select(pl.col("x").sum())
    with no_queries(monkeypatch, connection):
        translated = (
            convert_polars_to_ibis(plan, "source", connection)
            if positional
            else convert_polars_to_ibis(plan, table_name="source", backend=ibis.duckdb)
        )
        assert translated._find_backends()[1]
    assert_frame_equal(connection.to_polars(translated), plan.collect())
