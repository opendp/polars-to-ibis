import dataclasses
import re
from typing import Any

import opendp.prelude as dp
import polars as pl
import pytest

from polars_to_ibis import scan_database, split_polars_on_ffi

from .utils import backends, get_connection


def norm_sql(sql: str):
    return re.sub(r"\s+", " ", sql).replace('"', "").strip()


TABLE_NAME = "default_table"


@dataclasses.dataclass
class SplitScenario:
    expression: str
    expected_sql: str
    expected_result: dict[str, Any]
    expected_scale: float


CASE_CLAUSE = """
CASE
    WHEN COALESCE(t0.ints, 5) IS NULL
    THEN COALESCE(t0.ints, 5)
    ELSE LEAST(10, COALESCE(t0.ints, 5))
END
"""

scenarios = [
    SplitScenario(
        "context.query().select(dp.len())",
        f"SELECT COUNT(*) AS len FROM {TABLE_NAME} AS t0",
        {"len": [4]},
        1.0,
    ),
    SplitScenario(
        "context.query().select(pl.col.ints.dp.sum((0,10)))",
        f"""
            SELECT SUM(
                CASE
                    WHEN {CASE_CLAUSE} IS NULL
                    THEN {CASE_CLAUSE}
                    ELSE GREATEST( 0, {CASE_CLAUSE} )
                END
            ) AS ints FROM {TABLE_NAME} AS t0"
            """,
        {"ints": [10]},
        10.0,
    ),
    # TODO:
    # SplitScenario(
    #     "context.query().select(pl.col.ints.dp.mean((0,10)))",
    #     """
    #     ???
    #     """,
    #     {"ints": [2.5]},
    #     0,
    # ),
    # (
    #     "context.query().select(pl.col.ints.dp.sum((0,10)))",
    #     f"... FROM {table_name} AS t0",
    # ),
]


@pytest.mark.parametrize(
    "scenario",
    scenarios,
    ids=lambda scenario: scenario.expression,
)
@pytest.mark.parametrize("backend", backends)
def test_split_lazyframe(scenario: SplitScenario, backend: str):
    # Set up database:
    connection = get_connection(
        df=pl.DataFrame(
            {
                "ints": [1, 2, 3, 4],
                "floats": [0.1, 0.2, 0.3, 0.4],
            }
        ),
        table_name=TABLE_NAME,
        backend=backend,
    )

    # Pretend we're software that uses OpenDP as a dependency.
    # (If there is non-OpenDP boilerplate, move it into the package.)
    dp.enable_features("contrib", "honest-but-curious")
    schema_lf = scan_database(connection, TABLE_NAME)
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
    query = eval(scenario.expression, globals)

    # TODO: Confirm that this is the interface we want.
    def helper_function_to_add_to_opendp(query, table_name, connection):
        query_lf = query.release().lazy()

        ibis_table, plugin_parameters = split_polars_on_ffi(
            query_lf,
            table_name=table_name,
            # In the future, add a parameter to specify the plugin to split on?
        )

        # Use ibis_table:

        private_result = connection.to_polars(ibis_table).to_dict(as_series=False)
        private_item = list(private_result.items())[0][1][0]

        # Test ibis_table:
        # (Remove test assertion after porting to opendp.)
        actual_sql = norm_sql(ibis_table.to_sql())
        assert actual_sql == norm_sql(scenario.expected_sql)
        assert private_result == scenario.expected_result

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

        expected_parameters = {
            "flags": {
                "check_lengths": True,
                "flags": "ROW_SEPARABLE | LENGTH_PRESERVING",
            },
            "lib": ".../opendp.abi3.so",
            "symbol": "noise_plugin",
            "unpickled_kwargs": {
                "distribution": "Laplace",
                "scale": scenario.expected_scale,
                "support": "Integer",
            },
        }

        assert plugin_parameters == expected_parameters

        # Put the pieces together:

        return measurement(private_item)

    dp_result = helper_function_to_add_to_opendp(query, TABLE_NAME, connection)
    assert isinstance(dp_result, float) or isinstance(dp_result, int)
