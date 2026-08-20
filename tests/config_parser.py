"""
Only those parts of Polars API which are exercised below are implemented,
and even in that narrow scope you'll see a number of quirks.
"""

import dataclasses
import math

input_data = {
    "numeric": {
        "ints": [1, 2, 3, 4],
        "floats": [0.1, 0.2, 0.3, 0.4],
    },
    "sorting": {
        "ints": [9, 9, 1, 1],
        "strs": ["Z", "A", "B", "C"],
    },
    "grouping": {
        "keys": [0, 0, 1, 1],
        "values": [1, 2, 3, 4],
    },
    "triangle": {
        "keys": [1, 2, 2, 3, 3, 3, 4, 4, 4, 4],
    },
    "select": {
        "ints": [1, 2, 3],
        "strs": ["A", "B", "C"],
        "bools": [True, True, True],
        "bytes": [b"C", b"B", b"C"],
    },
    "hundred": {"ints": list(range(101))},
    "nan_null_inf": {
        "nan": [0.0, float("nan")],
        "null": [0.0, None],
        "inf": [0.0, float("inf")],
    },
}


@dataclasses.dataclass
class ParserScenario:
    category: str
    expression: str
    expected_output: dict[str, list[float | str]]
    convert_errors: dict[str, str] = dataclasses.field(default_factory=dict)  # type: ignore
    connection_errors: dict[str, str] = dataclasses.field(default_factory=dict)  # type: ignore
    backend_errors: dict[str, str] = dataclasses.field(default_factory=dict)  # type: ignore
    tolerance: float = 0


parser_scenarios = [
    ParserScenario(
        "numeric",
        "lf.select(pl.len())",
        {"len": [4]},
    ),
    ParserScenario("numeric", "lf.sum()", {"floats": [1.0], "ints": [10]}),
    ParserScenario("numeric", "lf.select(pl.col.ints.sum())", {"ints": [10]}),
    ParserScenario(
        "numeric", "lf.select(pl.col.ints.sum().name.to_uppercase())", {"INTS": [10]}
    ),
    ParserScenario(
        "numeric",
        "lf.select(pl.col.ints.sum().name.prefix('pre_'))",
        {"pre_ints": [10]},
    ),
    ParserScenario(
        "numeric",
        "lf.select(pl.col.ints.sum().name.suffix('_post'))",
        {"ints_post": [10]},
    ),
    ParserScenario(
        "numeric",
        "lf.select(pl.col.ints.sum().alias('new_name'))",
        {"new_name": [10]},
    ),
    # TODO: Currently failing, because the rename handler assumes it is an aggregation.
    # Scenario(
    #     "numeric",
    #     "lf.select(pl.col.ints.name.prefix('pre_'))",
    #     {"pre_ints": [1, 2, 3, 4]},
    # ),
    # Scenario(
    #     "numeric",
    #     "lf.select(pl.col.ints.name.suffix('_post'))",
    #     {"ints_post": [1, 2, 3, 4]},
    # ),
    ParserScenario(
        "numeric",
        "lf.select(pl.col.ints.alias('new_name'))",
        {"new_name": [1, 2, 3, 4]},
    ),
    ParserScenario(
        "numeric",
        "lf.select(pl.col.floats / 2)",
        {"floats": [0.05, 0.1, 0.15, 0.2]},
    ),
    ParserScenario(
        "numeric",
        "lf.select(2 / pl.col.floats)",
        {"literal": [20.0, 10.0, 20.0 / 3, 5.0]},
    ),
    ParserScenario(
        "numeric",
        "lf.select(pl.col.ints / pl.col.floats)",
        {"ints": [10.0, 10.0, 10.0, 10.0]},
    ),
    ParserScenario("numeric", "lf.select(pl.col.ints.clip(0,1).sum())", {"ints": [4]}),
    ParserScenario(
        "numeric",
        "lf.mean()",
        {"floats": [0.25], "ints": [2.5]},
    ),
    ParserScenario(
        "numeric",
        "lf.mean().cast(pl.Int16)",
        {"floats": [0], "ints": [2]},
    ),
    ParserScenario(
        "numeric",
        "lf.median()",
        {"floats": [0.25], "ints": [2.5]},
        backend_errors={
            "sqlite": "Compilation rule for 'Median' operation is not defined",
            "mysql": "Compilation rule for 'Median' operation is not defined",
        },
    ),
    ParserScenario(
        "numeric",
        # This should return the same value as median, but it doesn't!
        "lf.quantile(0.5)",
        {"floats": [0.3], "ints": [3]},
        backend_errors={
            "sqlite": "Compilation rule for 'Quantile' operation is not defined",
            "mysql": "Compilation rule for 'Quantile' operation is not defined",
        },
        # BIG difference between the polars native version and the DB versions!
        tolerance=0.5,
    ),
    ParserScenario(
        "numeric",
        "lf.max()",
        {"floats": [0.4], "ints": [4]},
    ),
    ParserScenario("numeric", "lf.min()", {"floats": [0.1], "ints": [1]}),
    ParserScenario(
        "numeric",
        "lf.var()",
        {"floats": [5 / 3 / 100], "ints": [5 / 3]},
        tolerance=10e-6,
    ),
    ParserScenario(
        "numeric",
        "lf.std()",
        {"floats": [math.sqrt(5 / 3 / 100)], "ints": [math.sqrt(5 / 3)]},
        tolerance=10e-6,
    ),
    ParserScenario(
        "numeric",
        "lf.select("
        "    ints=pl.col('ints').clip(2.0,3.0),"
        "    floats=pl.col('floats').clip(2,3)"
        ")",
        {"floats": [2.0, 2.0, 2.0, 2.0], "ints": [2, 2, 3, 3]},
    ),
    ParserScenario(
        "sorting",
        "lf.sort(by='strs')",
        {
            "ints": [9, 1, 1, 9],
            "strs": ["A", "B", "C", "Z"],
        },
    ),
    ParserScenario(
        "sorting",
        "lf.sort(by=['ints', 'strs'])",
        {
            "ints": [1, 1, 9, 9],
            "strs": ["B", "C", "A", "Z"],
        },
    ),
    ParserScenario(
        "sorting",
        "lf.sort(by='strs', descending=True)",
        {
            "ints": [9, 1, 1, 9],
            "strs": ["Z", "C", "B", "A"],
        },
    ),
    ParserScenario(
        "sorting",
        "lf.sort(by=['ints', 'strs'], descending=True)",
        {
            "ints": [9, 9, 1, 1],
            "strs": ["Z", "A", "C", "B"],
        },
    ),
    ParserScenario(
        "sorting",
        "lf.sort(by=['ints', 'strs'], descending=[True, False])",
        {
            "ints": [9, 9, 1, 1],
            "strs": ["A", "Z", "B", "C"],
        },
    ),
    ParserScenario(
        "numeric",
        "lf.sort(by='ints').head(1)",
        {
            "ints": [1],
            "floats": [0.1],
        },
    ),
    # TODO: Negative offset not implemented. Reverse?
    # Scenario(
    #     "numeric",
    #     "lf.sort(by='ints').tail(1)",
    #     {
    #         "ints": [4],
    #         'floats': [0.4],
    #     },
    # ),
    ParserScenario(
        "select",
        "lf.select('ints')",
        {"ints": [1, 2, 3]},
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    ParserScenario(
        "select",
        "lf.drop(['strs', 'bools', 'bytes'])",
        {"ints": [1, 2, 3]},
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    ParserScenario(
        "select",
        "lf.select(new_name='ints')",
        {"new_name": [1, 2, 3]},
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    ParserScenario(
        "select",
        "lf.select('ints', ten=10)",
        {"ints": [1, 2, 3], "ten": [10, 10, 10]},
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    ParserScenario(
        "select",
        "lf.select('ints', ten=pl.lit('ten!'))",
        {"ints": [1, 2, 3], "ten": ["ten!", "ten!", "ten!"]},
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    ParserScenario(
        "select",
        "lf.select('ints', ten=10.0)",
        {"ints": [1, 2, 3], "ten": [10.0, 10.0, 10.0]},
        backend_errors={
            # Providing a Polars type may avoid this error. See next scenario.
            "postgres+to_polars": "Could not convert Decimal",
            "postgres+to_pyarrow": "Could not convert Decimal",
        },
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    ParserScenario(
        "select",
        "lf.select('ints', ten=pl.lit(10.0, pl.Float32))",
        {"ints": [1, 2, 3], "ten": [10.0, 10.0, 10.0]},
        backend_errors={
            "postgres+to_polars+polars==1.36.1": "Could not convert Decimal",
            "postgres+to_pyarrow+polars==1.36.1": "Could not convert Decimal",
            "postgres+to_polars+polars==1.41.2": "Could not convert Decimal",
            "postgres+to_pyarrow+polars==1.41.2": "Could not convert Decimal",
        },
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    ParserScenario(
        "select",
        "lf.select('ints', ten=False)",
        {"ints": [1, 2, 3], "ten": [False, False, False]},
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    # TODO: Names need to be explictly provided.
    # Scenario(
    #     "numeric",
    #     "lf.select(pl.col('ints') + pl.col('floats'))",
    #     {"ints": [1.1, 2.2, 3.3, 4.4]},
    # ),
    ParserScenario(
        "numeric",
        "lf.select(sum=pl.col('ints') + pl.col('floats'))",
        {"sum": [1.1, 2.2, 3.3, 4.4]},
    ),
    ParserScenario(
        "numeric",
        "lf.select(diff=pl.col('ints') - pl.col('floats'))",
        {"diff": [0.9, 1.8, 2.7, 3.6]},
    ),
    ParserScenario(
        "numeric",
        "lf.select(prod=pl.col('ints') * pl.col('floats'))",
        {"prod": [0.1, 0.4, 3 * 0.3, 1.6]},
    ),
    ParserScenario(
        "numeric",
        "lf.select(div=pl.col('ints') / 2)",
        {"div": [0.5, 1, 1.5, 2]},
    ),
    ParserScenario(
        "numeric",
        "lf.select(square=pl.col('ints') ** 2)",
        {"square": [1, 4, 9, 16]},
    ),
    ParserScenario(
        "numeric",
        "lf.select(mod=pl.col('ints') % 2)",
        {"mod": [1, 0, 1, 0]},
    ),
    ParserScenario(
        "select",
        "lf.select(plus_ten=(-pl.col('ints')) + 10)",
        {"plus_ten": [9, 8, 7]},
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    ParserScenario(
        "grouping",
        "lf.group_by('keys').agg(pl.col('values').sum()).sort(by='keys').select('values').head(1)",
        {"values": [3]},
    ),
    # Scenario(
    #     "triangle",
    #     "lf.group_by('keys').agg(pl.len()).sort(by='keys')",
    #     {'keys': [1, 2, 3, 4], 'len': [1, 2, 3, 4]},
    # ),
    ParserScenario(
        "grouping",
        "lf.filter(pl.col('values') != 1)",
        {"keys": [0, 1, 1], "values": [2, 3, 4]},
    ),
    ParserScenario(
        "grouping",
        "lf.filter(pl.col('keys') != 1)",
        {"keys": [0, 0], "values": [1, 2]},
    ),
    ParserScenario(
        "grouping",
        "lf.filter(pl.col('values') != 1)",
        {"keys": [0, 1, 1], "values": [2, 3, 4]},
    ),
    ParserScenario(
        "grouping",
        "lf.filter(pl.col('values') > 2).select('values')",
        {"values": [3, 4]},
    ),
    ParserScenario(
        "grouping",
        "lf.filter(pl.col('values') >= 2).select('values')",
        {"values": [2, 3, 4]},
    ),
    ParserScenario(
        "grouping",
        "lf.filter(pl.col('values') < 2).select('values')",
        {"values": [1]},
    ),
    ParserScenario(
        "grouping",
        "lf.filter(pl.col('values') <= 2).select('values')",
        {"values": [1, 2]},
    ),
    ParserScenario(
        "hundred",
        "lf.filter((pl.col('ints') % 5 == 0) & (pl.col('ints') % 7 == 0))",
        {"ints": [0, 35, 70]},
    ),
    ParserScenario(
        "hundred",
        "lf.filter(~(pl.col('ints') > 1) | ~(pl.col('ints') < 99))",
        {"ints": [0, 1, 99, 100]},
    ),
    # TODO: Going through a DB, None is converted to nan, and test fails.
    # Scenario(
    #     "nan_null_inf",
    #     # nan != nan, so drop it: We could compare serializations, if necessary.
    #     "lf.drop('nan')",
    #     {'inf': [0.0, float('inf')], 'null': [0.0, None]},
    # ),
    ParserScenario(
        "nan_null_inf",
        "lf.select('null').fill_null(111)",
        {"null": [0.0, 111.0]},
        # This error message is generated upstream, and we can't change "can not".
        connection_errors={"mysql": (MYSQL_INF := "inf can not be used with MySQL")},
    ),
    ParserScenario(
        "nan_null_inf",
        "lf.select('nan').fill_nan(111)",
        {"nan": [0.0, 111.0]},
        connection_errors={"mysql": MYSQL_INF},
        backend_errors={
            "sqlite": "Compilation rule for 'IsNan' operation is not defined"
        },
    ),
    ParserScenario(
        "nan_null_inf",
        "lf.select(pl.col.null.fill_null(999))",
        {"null": [0, 999]},
        connection_errors={"mysql": MYSQL_INF},
    ),
    ParserScenario(
        "nan_null_inf",
        "lf.filter(pl.col('null') != 0)",
        {"inf": [], "nan": [], "null": []},
        connection_errors={"mysql": MYSQL_INF},
    ),
    ParserScenario(
        "numeric",
        "lf.select("
        "    floats=pl.col('floats').mean(),"
        "    ints=pl.col('ints').mean()"
        ")",
        {"floats": [0.25], "ints": [2.5]},
    ),
    ParserScenario(
        "numeric",
        "lf.select("
        "    floats=pl.col('floats').median(),"
        "    ints=pl.col('ints').median()"
        ")",
        {"floats": [0.25], "ints": [2.5]},
        backend_errors={
            "sqlite": "Compilation rule for 'Median' operation is not defined",
            "mysql": "Compilation rule for 'Median' operation is not defined",
        },
    ),
    ParserScenario(
        "numeric",
        "lf.select("
        "    floats=pl.col('floats').sum(),"
        "    ints=pl.col('ints').sum()"
        ")",
        {"floats": [1.0], "ints": [10]},
    ),
    ParserScenario(
        "numeric",
        "lf.select("
        "    floats=pl.col('floats').min(),"
        "    ints=pl.col('ints').min()"
        ")",
        {"floats": [0.1], "ints": [1]},
        tolerance=0.0000001,
    ),
    ParserScenario(
        "numeric",
        "lf.select("
        "    floats=pl.col('floats').max(),"
        "    ints=pl.col('ints').max()"
        ")",
        {"floats": [0.4], "ints": [4]},
        tolerance=0.0000001,
    ),
    ParserScenario(
        "numeric",
        "lf.select("
        "    floats=pl.col('floats').std(),"
        "    ints=pl.col('ints').std()"
        ")",
        {"floats": [math.sqrt(5 / 3 / 100)], "ints": [math.sqrt(5 / 3)]},
        tolerance=0.00001,
    ),
    ParserScenario(
        "numeric",
        "lf.select("
        "    floats=pl.col('floats').var(),"
        "    ints=pl.col('ints').var()"
        ")",
        {"floats": [5 / 3 / 100], "ints": [5 / 3]},
        tolerance=0.00001,
    ),
    ParserScenario(
        "numeric",
        "lf.select("
        "    floats=pl.col('floats').quantile(0.5),"
        "    ints=pl.col('ints').quantile(0.5)"
        ")",
        {"floats": [0.3], "ints": [3.0]},
        convert_errors={"polars==1.41.2": "Unsupported Function Quantile"},
        backend_errors={
            "sqlite": "Compilation rule for 'Quantile' operation is not defined",
            "mysql": "Compilation rule for 'Quantile' operation is not defined",
        },
        tolerance=0.5,
    ),
]
