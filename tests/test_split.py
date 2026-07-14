import re

import ibis
import opendp.prelude as dp
import polars as pl

from polars_to_ibis import split


def norm_sql(sql: str):
    return re.sub(r"\s+", " ", sql).replace('"', "").strip()


def test_split_lazyframe():
    # TODO: If end-users need these, add to public API.
    from polars_to_ibis import _get_input_schema
    from polars_to_ibis._parse.table_handlers import update_polars_to_ibis

    # Example copied from opendp:
    dp.enable_features("contrib")
    context = dp.Context.compositor(
        data=pl.scan_csv(
            dp.examples.get_france_lfs_path(),
            ignore_errors=True,
        ),
        privacy_unit=dp.unit_of(contributions=36),
        privacy_loss=dp.loss_of(epsilon=1.0),
        split_evenly_over=5,
        margins=[
            dp.polars.Margin(max_length=150_000 * 36),
        ],
    )
    # TODO: Fix upstream docs, and then fix this;
    # Any nulls would be dropped by the filter, leaving nothing to impute.
    # https://github.com/opendp/opendp/issues/2788
    query_work_hours = (
        # 99 represents "Not applicable"
        context.query().filter(pl.col("HWUSUAL") != 99.0)
        # compute the DP sum
        .select(pl.col.HWUSUAL.cast(int).fill_null(35).dp.sum(bounds=(0, 80)))
    )

    db_plan, plugin_parameters = split(query_work_hours)

    plugin_parameters["lib"] = re.sub(r".*/", ".../", plugin_parameters["lib"])
    assert plugin_parameters == {
        "flags": {"check_lengths": True, "flags": "RETURNS_SCALAR"},
        "kwargs": [],
        "lib": ".../opendp.abi3.so",
        "symbol": "dp_sum",
    }

    input_schema = _get_input_schema(db_plan)
    table_name = "placeholder"

    ibis_table = update_polars_to_ibis(
        polars_plan=db_plan,
        table=ibis.table(input_schema, name=table_name),
    )

    sql = norm_sql(ibis_table.to_sql())
    expected_sql = norm_sql(f"""
        SELECT COALESCE(CAST(t0.HWUSUAL AS BIGINT), 35) AS HWUSUAL
        FROM {table_name} AS t0
        WHERE t0.HWUSUAL <> 99.0
    """)
    assert sql == expected_sql
