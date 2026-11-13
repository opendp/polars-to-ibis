"""Plan real OpenDP queries, then execute only their pre-noise SQL aggregates."""

import ibis
import polars as pl
import pytest

from polars_to_ibis import scan_database, split_polars_on_ffi_plan

opendp = pytest.importorskip("opendp")
if pl.__version__ != opendp.mod._EXPECTED_POLARS_VERSION:  # pragma: no cover
    pytest.skip(allow_module_level=True)

import opendp.prelude as dp

dp.enable_features("contrib", "honest-but-curious")


def release_query(connection, expressions, *, schema=None):
    source = (
        pl.LazyFrame(schema=schema)
        if schema is not None
        else scan_database(connection, "private_table")
    )
    context = dp.Context.compositor(
        data=source,
        privacy_unit=dp.unit_of(contributions=1),
        privacy_loss=dp.loss_of(epsilon=1.0),
        split_evenly_over=1,
        margins=[dp.polars.Margin(max_length=4)],
    )
    return context.query().select(*expressions).release().lazy()


@pytest.mark.parametrize("backend_name", ["sqlite", "duckdb"])
@pytest.mark.parametrize("values", [[1, 2, 3, 4], [None, -1, 3, 12], []])
def test_real_opendp_integer_mean(monkeypatch, backend_name, values):
    connection = getattr(ibis, backend_name).connect()
    try:
        connection.create_table(
            "private_table", pl.DataFrame({"x": values}, schema={"x": pl.Int64})
        )
        expression = pl.col("x").fill_null(0).dp.mean((0, 10)).alias("mean")
        query = release_query(connection, [expression])

        # Planning must neither collect the original plugin plan nor load an
        # arbitrary serialized expression to infer aggregate types.
        with monkeypatch.context() as patch:
            patch.setattr(
                pl.Expr,
                "deserialize",
                lambda *a, **k: pytest.fail("deserialize called"),
            )
            patch.setattr(
                pl.LazyFrame, "collect", lambda *a, **k: pytest.fail("collect called")
            )
            plan = split_polars_on_ffi_plan(query, "private_table", connection)
        assert [p.parameters["symbol"] for p in plan.plugins] == [
            "noise_plugin",
            "noise_plugin",
        ]
        assert [p.input_dtype for p in plan.plugins] == [pl.Int64, pl.UInt32]
        expected_sum = sum(min(10, max(0, x or 0)) for x in values)
        assert connection.to_polars(plan.ibis_table).row(0) == (
            expected_sum,
            len(values),
        )
        noisy_values = pl.DataFrame({"__ffi_0": [16], "__ffi_1": [6]})
        assert plan.postprocess(noisy_values).to_dict(as_series=False) == {
            "mean": [16 / 6]
        }
    finally:
        connection.disconnect()


@pytest.mark.parametrize("backend_name", ["sqlite", "duckdb"])
def test_real_opendp_mean_and_repeated_outputs(backend_name):
    connection = getattr(ibis, backend_name).connect()
    try:
        connection.create_table("private_table", pl.DataFrame({"x": [1, 2, 3, 4]}))
        mean = pl.col("x").fill_null(0).dp.mean((0, 10))
        query = release_query(
            connection,
            [
                mean.alias("first"),
                mean.alias("second"),
                dp.len(signed=False).alias("count"),
            ],
        )
        plan = split_polars_on_ffi_plan(query, "private_table", connection)
        assert len(plan.plugins) == 5
        assert connection.to_polars(plan.ibis_table).row(0) == (10, 4, 10, 4, 4)
        result = plan.postprocess(
            pl.DataFrame({p.name: [v] for p, v in zip(plan.plugins, [16, 6, 18, 9, 5])})
        )
        assert result.to_dict(as_series=False) == {
            "first": [16 / 6],
            "second": [2.0],
            "count": [5],
        }
    finally:
        connection.disconnect()


def test_real_opendp_float_mean_duckdb():
    connection = ibis.duckdb.connect()
    try:
        connection.create_table("private_table", pl.DataFrame({"x": [0.25, 0.5, 0.75]}))
        query = release_query(
            connection,
            [pl.col("x").fill_nan(0).fill_null(0).dp.mean((0.0, 1.0)).alias("mean")],
        )
        plan = split_polars_on_ffi_plan(query, "private_table", connection)
        assert [p.input_dtype for p in plan.plugins] == [pl.Float64, pl.UInt32]
        assert connection.to_polars(plan.ibis_table).row(0) == (1.5, 3)
        assert (
            plan.postprocess(pl.DataFrame({"__ffi_0": [1.75], "__ffi_1": [4]})).item()
            == 1.75 / 4
        )
    finally:
        connection.disconnect()


@pytest.mark.parametrize("backend_name", ["sqlite", "duckdb"])
def test_real_opendp_signed_count(backend_name):
    connection = getattr(ibis, backend_name).connect()
    try:
        connection.create_table("private_table", pl.DataFrame({"x": [1, 2, 3]}))
        query = release_query(connection, [dp.len(signed=True).alias("count")])
        plan = split_polars_on_ffi_plan(query, "private_table", connection)
        assert plan.plugins[0].input_dtype == pl.Int64
        assert connection.to_polars(plan.ibis_table).item() == 3
        assert plan.postprocess(pl.DataFrame({"__ffi_0": [-1]})).item() == -1
    finally:
        connection.disconnect()
