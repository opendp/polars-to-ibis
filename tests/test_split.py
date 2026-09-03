import re

import ibis
import opendp.prelude as dp
import polars as pl
import pytest

from polars_to_ibis import scan_database, split_polars_on_ffi

from .utils import get_connection


def norm_sql(sql: str):
    return re.sub(r"\s+", " ", sql).replace('"', "").strip()


table_name = "default_table"


@pytest.mark.parametrize(
    "scenario",
    [
        (
            "context.query().select(dp.len())",
            f"SELECT COUNT(*) AS len FROM {table_name} AS t0",
            {"len": [4]},
        ),
        # (
        #     "context.query().select(pl.col.ints.dp.sum((0,10)))",
        #     f"... FROM {table_name} AS t0",
        # ),
    ],
    ids=lambda scenario: "-".join(scenario[:2]),
)
def test_split_lazyframe(scenario):
    expression, expected_sql, expected_result = scenario

    # Set up database:
    connection = get_connection(
        pl.DataFrame(
            {
                "ints": [1, 2, 3, 4],
                "floats": [0.1, 0.2, 0.3, 0.4],
            }
        ),
        table_name,
        ibis.sqlite,
    )

    # Pretend we're software that uses OpenDP as a dependency.
    # (If there is non-OpenDP boilerplate, move it into the package.)
    dp.enable_features("contrib", "honest-but-curious")
    schema_lf = scan_database(connection, table_name)
    context = dp.Context.compositor(
        data=schema_lf,
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
        "pl": pl,
    }
    query = eval(expression, globals)

    # TODO: Confirm that this is the interface we want.
    def helper_function_to_add_to_opendp(query, table_name, connection):
        query_lf = query.release().lazy()

        ibis_table, plugin_parameters = split_polars_on_ffi(
            query_lf,
            table_name=table_name,
            # In the future, add a parameter to specify the plugin to split on?
            backend=connection,
        )

        # Use ibis_table:

        private_result = connection.to_polars(ibis_table).to_dict(as_series=False)
        private_item = list(private_result.items())[0][1][0]

        # Test ibis_table:
        # (Remove test assertion after porting to opendp.)
        actual_sql = norm_sql(ibis_table.to_sql())
        assert actual_sql == expected_sql
        assert private_result == expected_result

        # Use plugin_parameters:

        # TODO: Probably replace with https://github.com/google/saferpickle
        # ... but that is work that can be done in opendp, after porting.
        import pickle

        kwargs = pickle.loads(bytes(plugin_parameters["kwargs"]))

        match kwargs["support"]:
            case "Integer":
                support = int
            case "Float":  # pragma: no cover
                support = float
            case _:  # pragma: no cover
                raise ValueError(
                    f"Expected 'Integer' or 'Float', not {kwargs['support']}"
                )
        input_space = dp.atom_domain(T=support, nan=False), dp.absolute_distance(
            T=support
        )

        match kwargs["distribution"]:
            case "Laplace":
                make = dp.m.make_laplace
            case "Gaussian":  # pragma: no cover
                make = dp.m.make_gaussian
            case _:  # pragma: no cover
                raise ValueError(
                    f"Expected 'Laplace' or 'Gaussian', not {kwargs['distribution']}"
                )
        measurement = make(*input_space, scale=kwargs["scale"])

        # Test plugin_parameters:
        # (Remove when porting to opendp.)
        plugin_parameters["unpickled_kwargs"] = kwargs
        del plugin_parameters["kwargs"]
        plugin_parameters["lib"] = re.sub(r".*/", ".../", plugin_parameters["lib"])
        assert plugin_parameters == {
            "flags": {
                "check_lengths": True,
                "flags": "ROW_SEPARABLE | LENGTH_PRESERVING",
            },
            "lib": ".../opendp.abi3.so",
            "symbol": "noise_plugin",
            "unpickled_kwargs": {
                "distribution": "Laplace",
                "scale": 1.0,
                "support": "Integer",
            },
        }

        # Put the pieces together:

        return measurement(private_item)

    dp_result = helper_function_to_add_to_opendp(query, table_name, connection)
    assert isinstance(dp_result, float) or isinstance(dp_result, int)
