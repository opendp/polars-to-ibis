import dataclasses
from typing import Any

TABLE_NAME = "default_table"


@dataclasses.dataclass
class SplitScenario:
    expression: str
    expected_sql: str
    expected_result: dict[str, Any]
    expected_parameters: dict[str, Any]
    backend_errors: dict[str, str] = dataclasses.field(default_factory=dict)  # type: ignore


def get_case_clause(table: str) -> str:
    return f"""
    CASE
        WHEN COALESCE({table}.ints, 5) IS NULL
        THEN COALESCE({table}.ints, 5)
        ELSE LEAST(10, COALESCE({table}.ints, 5))
    END
    """


def get_select_int_sum(table: str) -> str:
    case_clause = get_case_clause(table)
    return f"""
    SELECT SUM(
        CASE
            WHEN {case_clause} IS NULL
            THEN {case_clause}
            ELSE GREATEST( 0, {case_clause} )
        END
    ) AS ints
    """


def get_select_float_sum() -> str:
    case_when_not = """
    CASE WHEN NOT ( ISNAN(COALESCE(t0.floats, 0.5)) )
              OR ( COALESCE(t0.floats, 0.5) IS NULL )
        THEN COALESCE(t0.floats, 0.5)
        ELSE 0.5
    END
    """
    case_when_nested = f"""
    CASE WHEN {case_when_not} IS NULL
        THEN {case_when_not}
        ELSE LEAST( 1.0, {case_when_not} )
    END
    """
    return f"""
    SELECT SUM(
        CASE WHEN {case_when_nested} IS NULL
            THEN {case_when_nested}
            ELSE GREATEST( 0.0, {case_when_nested} )
        END
    ) AS floats
    """


def get_expected_parameters(scale: int | float, support: str):
    return {
        "flags": {
            "check_lengths": True,
            "flags": "ROW_SEPARABLE | LENGTH_PRESERVING",
        },
        "lib": ".../opendp.abi3.so",
        "symbol": "noise_plugin",
        "unpickled_kwargs": {
            "distribution": "Laplace",
            "scale": scale,
            "support": support,
        },
    }


split_scenarios = [
    SplitScenario(
        "context.query().select(dp.len())",
        f"SELECT COUNT(*) AS len FROM {TABLE_NAME} AS t0",
        {"len": [4]},
        get_expected_parameters(1.0, "Integer"),
    ),
    SplitScenario(
        "context.query().select(pl.col.ints.dp.sum((0,10)))",
        f"{get_select_int_sum('t0')} FROM {TABLE_NAME} AS t0",
        {"ints": [10]},
        get_expected_parameters(10.0, "Integer"),
    ),
    SplitScenario(
        "context.query().filter(pl.col.ints!=1).select(pl.col.ints.dp.sum((0,10)))",
        f"""
        {get_select_int_sum('t1')} FROM (
            SELECT * FROM {TABLE_NAME} AS t0 WHERE t0.ints <> 1
        ) AS t1
        """,
        {"ints": [9]},
        get_expected_parameters(10.0, "Integer"),
    ),
    SplitScenario(
        "context.query().select(pl.col.floats.dp.sum((0,1)))",
        f"{get_select_float_sum()} FROM {TABLE_NAME} AS t0",
        {"floats": [1]},
        get_expected_parameters(1.00044408920985, "Float"),
        backend_errors={
            "sqlite": "Compilation rule for 'IsNan' operation is not defined",
            "mysql": "FUNCTION runner.IS_NAN does not exist",
        },
    ),
    # TODO: Expand coverage.
    # SplitScenario(
    #     # Two separate DP queries that differ only in their parameters.
    #     "context.query().select(pl.col.floats.dp.sum((0,1)),pl.col.ints.dp.sum((0,10)))",
    # ),
    # SplitScenario(
    #     "context.query().select(dp.len(),pl.col.ints.dp.sum((0,10)))",
    # ),
    # SplitScenario(
    #     "context.query().select(pl.col.ints.dp.mean((0,10)))",
    # ),
]
