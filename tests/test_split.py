import re

import opendp.prelude as dp
import polars as pl

from polars_to_ibis import scan_database, split_polars_on_ffi

from .utils import get_connection


def norm_sql(sql: str):
    return re.sub(r"\s+", " ", sql).replace('"', "").strip()


def test_split_lazyframe():
    # Set up database:
    table_name = "default_table"
    connection = get_connection(
        pl.DataFrame({"ints": [1, 2, 3, 4]}), table_name, "sqlite"
    )

    # Pretend we're software that uses OpenDP as a dependency.
    # (If there is non-OpenDP boilerplate, try to move it into polars-to-ibis.)
    dp.enable_features("contrib", "honest-but-curious")
    lf = scan_database(connection, table_name)
    context = dp.Context.compositor(
        data=lf,
        privacy_unit=dp.unit_of(contributions=1),
        privacy_loss=dp.loss_of(epsilon=1.0),
        split_evenly_over=1,
        margins=[
            dp.polars.Margin(max_length=1_000_000),
        ],
    )
    query_lf = (
        context.query()
        .select(
            # TODO: Parameterize this test and add complex examples.
            dp.len()
        )
        .release()
        .lazy()
    )

    ibis_table, plugin_parameters = split_polars_on_ffi(
        query_lf,
        table_name=table_name,
    )

    plugin_parameters["lib"] = re.sub(r".*/", ".../", plugin_parameters["lib"])
    plugin_parameters["kwargs"] = "bytes"
    assert plugin_parameters == {
        "flags": {"check_lengths": True, "flags": "ROW_SEPARABLE | LENGTH_PRESERVING"},
        "lib": ".../opendp.abi3.so",
        "symbol": "noise_plugin",
        "kwargs": "bytes",
    }

    actual_sql = norm_sql(ibis_table.to_sql())
    expected_sql = norm_sql(f"""
        SELECT COUNT(*) AS len FROM {table_name} AS t0
    """)
    assert actual_sql == expected_sql
