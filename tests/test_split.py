import re

import opendp.prelude as dp
import polars as pl
import pytest

from polars_to_ibis import scan_database, split_polars_on_ffi

from .utils import get_connection


def norm_sql(sql: str):
    return re.sub(r"\s+", " ", sql).replace('"', "").strip()


table_name = "default_table"


@pytest.mark.parametrize(
    "fixture",
    [
        (
            "context.query().select(dp.len())",
            f"SELECT COUNT(*) AS len FROM {table_name} AS t0",
        )
    ],
    ids=lambda fixture: "-".join(fixture),
)
def test_split_lazyframe(fixture):
    expression, expected_sql = fixture

    # Set up database:
    connection = get_connection(
        pl.DataFrame({"ints": [1, 2, 3, 4]}), table_name, "sqlite"
    )

    # Pretend we're software that uses OpenDP as a dependency.
    # (If there is non-OpenDP boilerplate, move it into the package.)
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
    globals = {
        "context": context,
        "dp": dp,
    }
    query_lf = eval(expression, globals).release().lazy()

    ibis_table, plugin_parameters = split_polars_on_ffi(
        query_lf,
        table_name=table_name,
    )

    # TODO: For completeness, actually run SQL to get un-noised data.
    actual_sql = norm_sql(ibis_table.to_sql())
    assert actual_sql == expected_sql

    # TODO: Demonstrate how we actually add calibrated noise, using this information.
    plugin_parameters["lib"] = re.sub(r".*/", ".../", plugin_parameters["lib"])
    assert all(0 <= arg <= 255 for arg in plugin_parameters["kwargs"])

    # TODO: Probably replace with https://github.com/google/saferpickle
    import pickle

    plugin_parameters["unpickled_kwargs"] = pickle.loads(
        bytes(plugin_parameters["kwargs"])
    )
    del plugin_parameters["kwargs"]

    assert plugin_parameters == {
        "flags": {"check_lengths": True, "flags": "ROW_SEPARABLE | LENGTH_PRESERVING"},
        "lib": ".../opendp.abi3.so",
        "symbol": "noise_plugin",
        "unpickled_kwargs": {
            "distribution": "Laplace",
            "scale": 1.0,
            "support": "Integer",
        },
    }
