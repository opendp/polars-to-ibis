from os import environ
from typing import Any

import ibis  # type: ignore
import polars as pl
import pytest

from polars_to_ibis._parser import node_to_ibis_table
from polars_to_ibis._serialization import Serialization

ibis.set_backend("polars")

#
# Utilities
#


def get_connection_table_name(df: pl.DataFrame, backend: str):
    kwargs = (
        {
            "user": environ["USER"],
            "password": "",
            "database": environ["USER"],
        }
        if backend == "mysql"
        else {}
    )
    connection = getattr(ibis, backend).connect(**kwargs)
    table_name = "default_table"

    # Ensure a clean slate.
    # Each backend raises its own error type
    # if the table doesn't already exist.
    try:
        connection.drop_table(table_name)
    except BaseException:  # noqa: B036
        pass
    connection.create_table(table_name, df)

    return (connection, table_name)


#
# Tests
#

backends = [
    # "polars",
    "sqlite",
    # "duckdb",
    # pytest.param("postgres", marks=pytest.mark.extra_install),
    # pytest.param("mysql", marks=pytest.mark.extra_install),
]

input_data = {
    # "namespace": pl.DataFrame(
    #     {
    #         "ints": [1, 2, 3, 4],
    #         # TODO: Add more columns, once polars namespace works on at least one
    #     }
    # ),
    # "mixed": pl.DataFrame(
    #     {
    #         "ints": [1, 2, 3, 4],
    #         "floats": [0.1, 0.2, 0.3, 0.4],
    #         "strings": ["a", "b", "c", "d"],
    #         "bools": [True, True, False, False],
    #     }
    # ),
    "numeric": pl.DataFrame(
        {
            "ints": [1, 2, 3, 4],
            "floats": [0.1, 0.2, 0.3, 0.4],
        }
    ),
}

category_expression_output_triples = [
    ("numeric", "lf.sum()", {"floats": [1.0], "ints": [10]}),
]


@pytest.mark.parametrize(
    "category_expression_output",
    category_expression_output_triples,
    ids=lambda triple: "-".join(triple[:-1]),  # Don't include output in ID.
)
@pytest.mark.parametrize("backend", backends)
def test_translate_table(
    category_expression_output: tuple[str, str, dict[str, list[Any]]], backend: str
):
    category, expression, expected_output = category_expression_output
    input_df = input_data[category]
    lf = input_df.lazy()  # type: ignore # noqa: F841; "lf" is used in eval()
    polars_expression = eval(expression)
    assert (
        polars_expression.collect().to_dict(as_series=False) == expected_output
    ), "Typo in test? Polars does not produce expected output."

    # TODO: Move out of test.
    node = Serialization(polars_expression)._serial  # type: ignore

    # TODO: Move out of test.
    polars_schema = lf.collect_schema()
    ibis_schema = ibis.expr.schema.Schema.from_polars(polars_schema)
    table_name = "default_table"
    ibis_table = ibis.table(ibis_schema, name=table_name)  # type: ignore

    # TODO: Test at higher level.
    new_table = node_to_ibis_table(node=node, table=ibis_table)

    connection, table_name = get_connection_table_name(input_df, backend=backend)
    actual_output = connection.to_pandas(new_table).to_dict(orient="list")
    assert (
        actual_output == expected_output
    ), f"Via ibis, {backend} does not produce expected output"
