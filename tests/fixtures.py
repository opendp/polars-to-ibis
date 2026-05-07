import dataclasses
import math


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
    Fixture(
        "numeric",
        "lf.sort(by='ints').head(1)",
        {
            "ints": [1],
            "floats": [0.1],
        },
    ),
    # TODO: Negative offset not implemented. Reverse?
    # Fixture(
    #     "numeric",
    #     "lf.sort(by='ints').tail(1)",
    #     {
    #         "ints": [4],
    #         'floats': [0.4],
    #     },
    # ),
    Fixture(
        "select",
        "lf.select('ints')",
        {"ints": [1, 2, 3]},
    ),
    Fixture(
        "select",
        "lf.drop(['strs', 'bools', 'bytes'])",
        {"ints": [1, 2, 3]},
    ),
    Fixture(
        "select",
        "lf.select(new_name='ints')",
        {"new_name": [1, 2, 3]},
    ),
    Fixture(
        "select",
        "lf.select('ints', ten=10)",
        {"ints": [1, 2, 3], "ten": [10, 10, 10]},
    ),
    Fixture(
        "select",
        "lf.select('ints', ten=pl.lit('ten!'))",
        {"ints": [1, 2, 3], "ten": ["ten!", "ten!", "ten!"]},
    ),
    Fixture(
        "select",
        "lf.select('ints', ten=10.0)",
        {"ints": [1, 2, 3], "ten": [10.0, 10.0, 10.0]},
        expected_exporter_errors={"postgres+to_polars": "Could not convert Decimal"},
    ),
    Fixture(
        "select",
        "lf.select('ints', ten=False)",
        {"ints": [1, 2, 3], "ten": [False, False, False]},
    ),
    # TODO: For now, names need to be explictly provided.
    # Fixture(
    #     "numeric",
    #     "lf.select(pl.col('ints') + pl.col('floats'))",
    #     {"ints": [1.1, 2.2, 3.3, 4.4]},
    # ),
    Fixture(
        "numeric",
        "lf.select(sum=pl.col('ints') + pl.col('floats'))",
        {"sum": [1.1, 2.2, 3.3, 4.4]},
    ),
    Fixture(
        "numeric",
        "lf.select(diff=pl.col('ints') - pl.col('floats'))",
        {"diff": [0.9, 1.8, 2.7, 3.6]},
    ),
    Fixture(
        "numeric",
        "lf.select(prod=pl.col('ints') * pl.col('floats'))",
        {"prod": [0.1, 0.4, 3 * 0.3, 1.6]},
    ),
    Fixture(
        "numeric",
        "lf.select(div=pl.col('ints') / 2)",
        {"div": [0.5, 1, 1.5, 2]},
    ),
    Fixture(
        "numeric",
        "lf.select(square=pl.col('ints') ** 2)",
        {"square": [1, 4, 9, 16]},
    ),
    Fixture(
        "numeric",
        "lf.select(mod=pl.col('ints') % 2)",
        {"mod": [1, 0, 1, 0]},
    ),
    Fixture(
        "select",
        "lf.select(plus_ten=(-pl.col('ints')) + 10)",
        {"plus_ten": [9, 8, 7]},
    ),
    Fixture(
        "grouping",
        "lf.group_by('keys').agg(pl.col('values').sum()).sort(by='keys').select('values').head(1)",
        {"values": [3]},
    ),
    Fixture(
        "grouping",
        "lf.filter(pl.col('values') != 1)",
        {"keys": [0, 1, 1], "values": [2, 3, 4]},
    ),
    Fixture(
        "grouping",
        "lf.filter(pl.col('keys') != 1)",
        {"keys": [0, 0], "values": [1, 2]},
    ),
    Fixture(
        "grouping",
        "lf.filter(pl.col('values') != 1)",
        {"keys": [0, 1, 1], "values": [2, 3, 4]},
    ),
    Fixture(
        "grouping",
        "lf.filter(pl.col('values') > 2).select('values')",
        {"values": [3, 4]},
    ),
    Fixture(
        "grouping",
        "lf.filter(pl.col('values') >= 2).select('values')",
        {"values": [2, 3, 4]},
    ),
    Fixture(
        "grouping",
        "lf.filter(pl.col('values') < 2).select('values')",
        {"values": [1]},
    ),
    Fixture(
        "grouping",
        "lf.filter(pl.col('values') <= 2).select('values')",
        {"values": [1, 2]},
    ),
    Fixture(
        "hundred",
        "lf.filter((pl.col('ints') % 5 == 0) & (pl.col('ints') % 7 == 0))",
        {"ints": [0, 35, 70]},
    ),
    Fixture(
        "hundred",
        "lf.filter(~(pl.col('ints') > 1) | ~(pl.col('ints') < 99))",
        {"ints": [0, 1, 99, 100]},
    ),
    # TODO: Going through a DB, None is converted to nan, and test fails.
    # Fixture(
    #     "nan_null_inf",
    #     # nan != nan, so drop it: We could compare serializations, if necessary.
    #     "lf.drop('nan')",
    #     {'inf': [0.0, float('inf')], 'null': [0.0, None]},
    # ),
    Fixture(
        "nan_null_inf",
        "lf.select('null').fill_null(111)",
        {"null": [0.0, 111.0]},
    ),
    # TODO: Fails for duckdb and postgres, but sqlite passes.
    # Fixture(
    #     "nan_null_inf",
    #     "lf.select('nan').fill_nan(111)",
    #     {"nan": [0.0, 111.0]},
    # ),
]
