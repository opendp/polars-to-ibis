import dataclasses
from typing import Any

TABLE_NAME = "default_table"


@dataclasses.dataclass
class SplitScenario:
    expression: str
    expected_sql: str
    expected_result: dict[str, Any]
    expected_scale: float


def get_case_clause(table: str) -> str:
    return f"""
    CASE
        WHEN COALESCE({table}.ints, 5) IS NULL
        THEN COALESCE({table}.ints, 5)
        ELSE LEAST(10, COALESCE({table}.ints, 5))
    END
    """


def get_select_sum(table: str) -> str:
    case_clause = get_case_clause(table)
    return f"""
    SELECT SUM(
        CASE
            WHEN {case_clause} IS NULL
            THEN {case_clause}
            ELSE GREATEST( 0, {case_clause} )
        END
    ) AS ints"
    """


split_scenarios = [
    SplitScenario(
        "context.query().select(dp.len())",
        f"SELECT COUNT(*) AS len FROM {TABLE_NAME} AS t0",
        {"len": [4]},
        1.0,
    ),
    SplitScenario(
        "context.query().select(pl.col.ints.dp.sum((0,10)))",
        f"{get_select_sum('t0')} FROM {TABLE_NAME} AS t0",
        {"ints": [10]},
        10.0,
    ),
    SplitScenario(
        "context.query().filter(pl.col.ints!=1).select(pl.col.ints.dp.sum((0,10)))",
        f"""
        {get_select_sum('t1')} FROM (
            SELECT * FROM {TABLE_NAME} AS t0 WHERE t0.ints <> 1
        ) AS t1
        """,
        {"ints": [9]},
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
