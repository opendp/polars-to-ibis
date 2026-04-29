import dataclasses
import re
from os import environ

import ibis  # type: ignore
import polars as pl
import pytest

from polars_to_ibis import convert_polars_to_ibis

ibis.set_backend("polars")

# Utilities:


def get_connection(df: pl.DataFrame, table_name: str, backend: str):
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

    # Ensure a clean slate.
    # Each backend raises its own error type
    # if the table doesn't already exist.
    try:
        connection.drop_table(table_name)
    except BaseException:  # noqa: B036
        pass
    connection.create_table(table_name, df)

    return connection


# Test fixtures:

backends = [
    "polars",
    "sqlite",
    "duckdb",
    pytest.param("postgres", marks=pytest.mark.extra_install),
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


@dataclasses.dataclass
class Fixture:
    category: str
    expression: str
    expected_output: dict[str, list[float]]
    expected_errors: dict[str, str] = dataclasses.field(default_factory=dict)  # type: ignore


fixtures = [
    Fixture("numeric", "lf.sum()", {"floats": [1.0], "ints": [10]}),
    Fixture("numeric", "lf.mean()", {"floats": [0.25], "ints": [2.5]}),
    Fixture(
        "numeric",
        "lf.median()",
        {"floats": [0.25], "ints": [2.5]},
        {"sqlite": "Compilation rule for 'Median' operation is not defined"},
    ),
]


# Tests:


@pytest.mark.parametrize(
    "fixture", fixtures, ids=lambda fixture: f"{fixture.category}-{fixture.expression}"
)
@pytest.mark.parametrize("backend", backends)
def test_translate_table(fixture: Fixture, backend: str):
    # Setup:
    input_df = input_data[fixture.category]
    lf = input_df.lazy()  # type: ignore # noqa: F841; "lf" is used in eval()
    lf: pl.LazyFrame = eval(fixture.expression)
    polars_output = lf.collect().to_dict(as_series=False)
    assert (
        polars_output == fixture.expected_output
    ), "Typo in test? Polars does not produce expected output."

    table_name = "default_table"
    ibis_table = convert_polars_to_ibis(lf, table_name)

    connection = get_connection(input_df, table_name=table_name, backend=backend)
    # Using to_pandas to avoid accidental dependency on target library.
    if expected_error := fixture.expected_errors.get(backend):
        with pytest.raises(Exception, match=re.escape(expected_error)):
            connection.to_pandas(ibis_table)
    else:
        actual_output = connection.to_pandas(ibis_table).to_dict(orient="list")
        assert (
            actual_output == fixture.expected_output
        ), f"Via ibis, {backend} does not produce expected output"
