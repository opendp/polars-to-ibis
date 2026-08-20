import dataclasses
from typing import Any

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

split_scenarios = [
    SplitScenario(
        "context.query().select(dp.len().alias('new_name'))",
        f"""
        SELECT COUNT(*)
        OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        AS new_name FROM {TABLE_NAME} AS t0
        """,
        {"len": [4]},
        1.0,
    ),
    # TODO: Should have two results, "len" and "duplicate"
    # SplitScenario(
    #     "context.query().select(dp.len(), dp.len().alias('duplicate'))",
    #     f"SELECT COUNT(*) AS len FROM {TABLE_NAME} AS t0",
    #     {"len": [4]},
    #     2.0,
    # ),
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
    # TODO: Add support for mean()
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
