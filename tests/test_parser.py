import dataclasses
import math
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
    # MySQL could be added, if needed, but for now
    # we want to focus on a smaller number of backends.
    # pytest.param("mysql", marks=pytest.mark.extra_install),
]

input_data = {
    "numeric": pl.DataFrame(
        {
            "ints": [1, 2, 3, 4],
            "floats": [0.1, 0.2, 0.3, 0.4],
        }
    ),
    "sorting": pl.DataFrame(
        {
            "ints": [9, 9, 1, 1],
            "strs": ["Z", "A", "B", "C"],
        }
    ),
    # "select": pl.DataFrame(
    #     {
    #         "ints": [1, 2, 3],
    #         "strs": ["A", "B", "C"],
    #     }
    # ),
}


@dataclasses.dataclass
class Fixture:
    category: str
    expression: str
    expected_output: dict[str, list[float | str]]
    expected_backend_errors: dict[str, str] = dataclasses.field(default_factory=dict)  # type: ignore
    expected_exporter_errors: dict[str, str] = dataclasses.field(default_factory=dict)  # type: ignore
    tolerance: dict[str, float] = dataclasses.field(default_factory=dict)  # type: ignore


fixtures = [
    Fixture("numeric", "lf.sum()", {"floats": [1.0], "ints": [10]}),
    Fixture(
        "numeric",
        "lf.mean()",
        {"floats": [0.25], "ints": [2.5]},
        expected_exporter_errors={"postgres+to_polars": "Could not convert Decimal"},
    ),
    Fixture(
        "numeric",
        "lf.median()",
        {"floats": [0.25], "ints": [2.5]},
        expected_backend_errors={
            "sqlite": "Compilation rule for 'Median' operation is not defined"
        },
    ),
    Fixture(
        "numeric",
        # This should return the same value as median, but it doesn't!
        "lf.quantile(0.5)",
        {"floats": [0.3], "ints": [3]},
        expected_backend_errors={
            "sqlite": "Compilation rule for 'Quantile' operation is not defined"
        },
        # BIG difference between the polars native version and the DB versions!
        tolerance={"postgres": 0.5, "duckdb": 0.5, "polars": 0.5},
    ),
    Fixture(
        "numeric",
        "lf.max()",
        {"floats": [0.4], "ints": [4]},
    ),
    Fixture("numeric", "lf.min()", {"floats": [0.1], "ints": [1]}),
    Fixture(
        "numeric",
        "lf.var()",
        {"floats": [5 / 3 / 100], "ints": [5 / 3]},
        tolerance={"postgres": 10e-6},
        expected_exporter_errors={"postgres+to_polars": "Could not convert Decimal"},
    ),
    Fixture(
        "numeric",
        "lf.std()",
        {"floats": [math.sqrt(5 / 3 / 100)], "ints": [math.sqrt(5 / 3)]},
        expected_exporter_errors={"postgres+to_polars": "Could not convert Decimal"},
    ),
    Fixture(
        "sorting",
        "lf.sort(by='strs')",
        {
            "ints": [9, 1, 1, 9],
            "strs": ["A", "B", "C", "Z"],
        },
    ),
    Fixture(
        "sorting",
        "lf.sort(by=['ints', 'strs'])",
        {
            "ints": [1, 1, 9, 9],
            "strs": ["B", "C", "A", "Z"],
        },
    ),
    Fixture(
        "sorting",
        "lf.sort(by='strs', descending=True)",
        {
            "ints": [9, 1, 1, 9],
            "strs": ["Z", "C", "B", "A"],
        },
    ),
    Fixture(
        "sorting",
        "lf.sort(by=['ints', 'strs'], descending=True)",
        {
            "ints": [9, 9, 1, 1],
            "strs": ["Z", "A", "C", "B"],
        },
    ),
    Fixture(
        "sorting",
        "lf.sort(by=['ints', 'strs'], descending=[True, False])",
        {
            "ints": [9, 9, 1, 1],
            "strs": ["A", "Z", "B", "C"],
        },
    ),
    # Fixture(
    #     "select",
    #     "lf.select('ints')",
    #     {"ints": [1, 2, 3]},
    # )
    # Fixture(
    #     "grouping",
    #     "lf.group_by('ints').agg(pl.col('floats').sum()).sort(by='floats')",
    #     # Because of float arithmetic,
    #     # 0.1 + 0.2 != 0.3
    #     {"floats": [0.1 + 0.2, 0.7], "ints": [1, 4]},
    # ),
]


exporters = {
    "to_polars": lambda connection, ibis_table: connection.to_polars(
        ibis_table
    ).to_dict(as_series=False),
    "to_pandas": lambda connection, ibis_table: connection.to_pandas(
        ibis_table
    ).to_dict(orient="list"),
}

# Tests:


@pytest.mark.parametrize(
    "fixture", fixtures, ids=lambda fixture: f"{fixture.category}-{fixture.expression}"
)
@pytest.mark.parametrize("backend", backends)
@pytest.mark.parametrize("exporter_key", exporters.keys())
def test_translate_table(fixture: Fixture, backend: str, exporter_key: str):
    # Setup:
    input_df = input_data[fixture.category]
    lf = input_df.lazy()  # type: ignore # noqa: F841; "lf" is used in eval()
    lf: pl.LazyFrame = eval(fixture.expression)
    polars_output = lf.collect().to_dict(as_series=False)
    assert polars_output == fixture.expected_output, "Typo in test?"

    table_name = "default_table"
    ibis_table = convert_polars_to_ibis(lf, table_name)

    connection = get_connection(input_df, table_name=table_name, backend=backend)
    export = exporters[exporter_key]  # type: ignore
    if expected_error := fixture.expected_backend_errors.get(
        backend
    ) or fixture.expected_exporter_errors.get(f"{backend}+{exporter_key}"):
        with pytest.raises(Exception, match=re.escape(expected_error)):
            export(connection, ibis_table)
        pytest.xfail(f"expected error: {expected_error}")

    actual_output = export(connection, ibis_table)  # type: ignore
    tolerance = fixture.tolerance.get(backend)
    if not tolerance:
        assert (
            actual_output == fixture.expected_output
        ), f"Via ibis, {backend} does not produce expected output"
        return

    any_not_equal = False
    for key in actual_output.keys() | fixture.expected_output.keys():  # type: ignore
        assert actual_output[key] == pytest.approx(fixture.expected_output[key], abs=tolerance)  # type: ignore  # noqa: B950 (line too long)
        any_not_equal |= actual_output[key] != fixture.expected_output[key]  # type: ignore
    assert any_not_equal, "All are equal; approx not needed"
