"""
Only those parts of Polars API which are exercised below are implemented,
and even in that narrow scope you'll see a number of quirks.
"""

import dataclasses
import math
from abc import ABC, abstractmethod

import polars as pl

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
class BaseParserScenario(ABC):
    category: str
    expression: str
    expected_output: dict[str, list[float | str]]
    polars_errors: dict[str, str] = dataclasses.field(default_factory=dict)  # type: ignore
    convert_errors: dict[str, str] = dataclasses.field(default_factory=dict)  # type: ignore
    connection_errors: dict[str, str] = dataclasses.field(default_factory=dict)  # type: ignore
    backend_errors: dict[str, str] = dataclasses.field(default_factory=dict)  # type: ignore
    tolerance: float = 0

    @abstractmethod
    def exec(self, named_frames): ...


class EvalParserScenario(BaseParserScenario):
    def exec(self, named_frames):
        named_frames["pl"] = pl
        return eval(self.expression, named_frames)


class SQLParserScenario(BaseParserScenario):
    def exec(self, named_frames):
        return pl.SQLContext(**named_frames).execute(self.expression)


parser_scenarios = [
    SQLParserScenario(
        "numeric",
        "SELECT 1 + ints / floats FROM lf",
        {"literal": [11, 11, 11, 11]},
        convert_errors={"*": "Unsupported select expr BinaryExpr"},  # TODO
    ),
    SQLParserScenario(
        "numeric",
        "SELECT 42 AS fortytwo FROM lf",
        {"fortytwo": [42, 42, 42, 42]},
        convert_errors={"*": "Unsupported HStack"},  # TODO
    ),
    SQLParserScenario(
        "numeric",
        "SELECT CASE WHEN ints <= 3 THEN -1 END FROM lf",
        {"literal": [-1, -1, -1, None]},
        convert_errors={"*": "Unsupported Literal"},  # TODO
    ),
    # TODO: This is the output from polars: Doesn't match output from ibis.
    # SQLParserScenario(
    #     "numeric",
    #     "SELECT CASE WHEN ints <= 1 THEN -1 ELSE 100 END FROM lf",
    #     {"literal": [-1, 100, 100, 100]},
    # ),
    SQLParserScenario(
        "numeric",
        "SELECT CASE ints WHEN 1 THEN -1 END FROM lf",
        {"literal": [-1, None, None, None]},
        convert_errors={"*": "Unsupported Literal"},  # TODO
    ),
    # TODO: This is the output from polars: Doesn't match output from ibis.
    # SQLParserScenario(
    #     "numeric",
    #     "SELECT CASE ints WHEN ints THEN -1 ELSE 100 END FROM lf",
    #     {"literal": [-1, -1, -1, -1]},
    # ),
    # TODO: This is the output from polars: Doesn't match output from ibis.
    # SQLParserScenario(
    #     "numeric",
    #     "SELECT CASE WHEN SUM(ints) > 1 THEN -1 ELSE 100 END FROM lf",
    #     {"literal": [-1]},
    # ),
    # TODO: This is the output from polars: Doesn't match output from ibis.
    # SQLParserScenario(
    #     "numeric",
    #     """
    #     SELECT CASE
    #     WHEN ints <= 1 THEN -1
    #     WHEN ints > 1 AND ints <= 3 then 0
    #     ELSE 1
    #     END FROM lf
    #     """,
    #     {"literal": [-1, 0, 0, 1]},
    # ),
    SQLParserScenario(
        "numeric",
        "SELECT IIF(ints > 2, 'Big', 'Small') AS size FROM lf",
        {},
        polars_errors={"*": "unsupported function 'iif'"},
    ),
    SQLParserScenario(
        "numeric",
        "SELECT ROUND(floats * 2) FROM lf",
        {"floats": [0.0, 0.0, 1.0, 1.0]},
        convert_errors={"*": "Unsupported select expr Function"},  # TODO
    ),
    SQLParserScenario(
        "numeric",
        "SELECT PI() FROM lf LIMIT 1",
        {"literal": [math.pi]},
        convert_errors={"*": "Unsupported HStack"},  # TODO
    ),
    SQLParserScenario(
        "numeric",
        "SELECT DEGREES(ints) FROM lf LIMIT 1",
        {"ints": [180 / math.pi]},
        convert_errors={"*": "Unsupported select expr Function"},  # TODO
    ),
    SQLParserScenario(
        "numeric",
        "SELECT DEGREES(ints) FROM lf LIMIT 1",
        {"ints": [180 / math.pi]},
        convert_errors={"*": "Unsupported select expr Function"},  # TODO
    ),
    SQLParserScenario(
        "numeric",
        "SELECT CHOOSE(ints, 2, 0, 2, 6) FROM lf",
        {},
        polars_errors={"*": "unsupported function 'choose'"},
    ),
    SQLParserScenario(
        "numeric",
        "SELECT - ints AS negative FROM lf",
        {"negative": [-1, -2, -3, -4]},
    ),
    SQLParserScenario(
        "numeric",
        "SELECT '' AS empty, '\"' AS dquote FROM lf LIMIT 1",
        {"empty": [""], "dquote": ['"']},
        convert_errors={"*": "Unsupported HStack"},  # TODO
    ),
    SQLParserScenario(
        "numeric",
        "SELECT POWER(ints, 2) AS squares FROM lf",
        {"squares": [1, 4, 9, 16]},
    ),
    SQLParserScenario(
        "numeric",
        "SELECT POWER(2, ints) AS power_2 FROM lf",
        {"power_2": [2, 4, 8, 16]},
    ),
    SQLParserScenario(
        "numeric",
        "SELECT '日本' AS japan FROM lf limit 1",
        {"japan": ["日本"]},
        convert_errors={"*": "Unsupported HStack"},  # TODO
    ),
    SQLParserScenario(
        "numeric",
        "SELECT LN(ints) FROM lf limit 1",
        {"ints": [math.log(1)]},
        convert_errors={"*": "Unsupported select expr Function"},  # TODO
    ),
    SQLParserScenario(
        "numeric",
        "SELECT LOG2(ints) FROM lf limit 1",
        {"ints": [math.log(1)]},
        convert_errors={"*": "Unsupported select expr Function"},  # TODO
    ),
    EvalParserScenario(
        "numeric",
        "lf.select(pl.len())",
        {"len": [4]},
    ),
    EvalParserScenario("numeric", "lf.sum()", {"floats": [1.0], "ints": [10]}),
    EvalParserScenario("numeric", "lf.select(pl.col.ints.sum())", {"ints": [10]}),
    EvalParserScenario(
        "numeric", "lf.select(pl.col.ints.sum().name.to_uppercase())", {"INTS": [10]}
    ),
    EvalParserScenario(
        "numeric",
        "lf.select(pl.col.ints.sum().name.prefix('pre_'))",
        {"pre_ints": [10]},
    ),
    EvalParserScenario(
        "numeric",
        "lf.select(pl.col.ints.sum().name.suffix('_post'))",
        {"ints_post": [10]},
    ),
    EvalParserScenario(
        "numeric",
        "lf.select(pl.col.floats / 2)",
        {"floats": [0.05, 0.1, 0.15, 0.2]},
    ),
    EvalParserScenario(
        "numeric",
        "lf.select(2 / pl.col.floats)",
        {"literal": [20.0, 10.0, 20.0 / 3, 5.0]},
    ),
    EvalParserScenario(
        "numeric",
        "lf.select(pl.col.ints / pl.col.floats)",
        {"ints": [10.0, 10.0, 10.0, 10.0]},
    ),
    EvalParserScenario(
        # TODO: Add more tests of name inference:
        # Which expression should it be based on?
        "numeric",
        "lf.select(pl.when(pl.col.ints > 3).then(pl.col.ints).otherwise(0))",
        {"ints": [0, 0, 0, 4]},
    ),
    EvalParserScenario(
        "numeric",
        "lf.select(pl.col.ints.clip(0,1).sum())",
        {"ints": [4]},
    ),
    EvalParserScenario(
        "numeric",
        "lf.mean()",
        {"floats": [0.25], "ints": [2.5]},
    ),
    EvalParserScenario(
        "numeric",
        "lf.mean().cast(pl.Int16)",
        {"floats": [0], "ints": [2]},
    ),
    EvalParserScenario(
        "numeric",
        "lf.median()",
        {"floats": [0.25], "ints": [2.5]},
        backend_errors={
            "sqlite": "Compilation rule for 'Median' operation is not defined",
            "mysql": "Compilation rule for 'Median' operation is not defined",
        },
    ),
    EvalParserScenario(
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
    EvalParserScenario(
        "numeric",
        "lf.max()",
        {"floats": [0.4], "ints": [4]},
    ),
    EvalParserScenario("numeric", "lf.min()", {"floats": [0.1], "ints": [1]}),
    EvalParserScenario(
        "numeric",
        "lf.var()",
        {"floats": [5 / 3 / 100], "ints": [5 / 3]},
        tolerance=10e-6,
    ),
    EvalParserScenario(
        "numeric",
        "lf.std()",
        {"floats": [math.sqrt(5 / 3 / 100)], "ints": [math.sqrt(5 / 3)]},
        tolerance=10e-6,
    ),
    EvalParserScenario(
        "numeric",
        "lf.select("
        "    ints=pl.col('ints').clip(2.0,3.0),"
        "    floats=pl.col('floats').clip(2,3)"
        ")",
        {"floats": [2.0, 2.0, 2.0, 2.0], "ints": [2, 2, 3, 3]},
    ),
    EvalParserScenario(
        "sorting",
        "lf.sort(by='strs')",
        {
            "ints": [9, 1, 1, 9],
            "strs": ["A", "B", "C", "Z"],
        },
    ),
    EvalParserScenario(
        "sorting",
        "lf.sort(by=['ints', 'strs'])",
        {
            "ints": [1, 1, 9, 9],
            "strs": ["B", "C", "A", "Z"],
        },
    ),
    EvalParserScenario(
        "sorting",
        "lf.sort(by='strs', descending=True)",
        {
            "ints": [9, 1, 1, 9],
            "strs": ["Z", "C", "B", "A"],
        },
    ),
    EvalParserScenario(
        "sorting",
        "lf.sort(by=['ints', 'strs'], descending=True)",
        {
            "ints": [9, 9, 1, 1],
            "strs": ["Z", "A", "C", "B"],
        },
    ),
    EvalParserScenario(
        "sorting",
        "lf.sort(by=['ints', 'strs'], descending=[True, False])",
        {
            "ints": [9, 9, 1, 1],
            "strs": ["A", "Z", "B", "C"],
        },
    ),
    EvalParserScenario(
        "numeric",
        "lf.sort(by='ints').head(1)",
        {
            "ints": [1],
            "floats": [0.1],
        },
    ),
    EvalParserScenario(
        "select",
        "lf.select('ints')",
        {"ints": [1, 2, 3]},
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    EvalParserScenario(
        "select",
        "lf.drop(['strs', 'bools', 'bytes'])",
        {"ints": [1, 2, 3]},
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    EvalParserScenario(
        "select",
        "lf.select(new_name='ints')",
        {"new_name": [1, 2, 3]},
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    EvalParserScenario(
        "select",
        "lf.select('ints', ten=10)",
        {"ints": [1, 2, 3], "ten": [10, 10, 10]},
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    EvalParserScenario(
        "select",
        "lf.select('ints', ten=pl.lit('ten!'))",
        {"ints": [1, 2, 3], "ten": ["ten!", "ten!", "ten!"]},
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    EvalParserScenario(
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
    EvalParserScenario(
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
    EvalParserScenario(
        "select",
        "lf.select('ints', ten=False)",
        {"ints": [1, 2, 3], "ten": [False, False, False]},
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    EvalParserScenario(
        "numeric",
        "lf.select(sum=pl.col('ints') + pl.col('floats'))",
        {"sum": [1.1, 2.2, 3.3, 4.4]},
    ),
    EvalParserScenario(
        "numeric",
        "lf.select(diff=pl.col('ints') - pl.col('floats'))",
        {"diff": [0.9, 1.8, 2.7, 3.6]},
    ),
    EvalParserScenario(
        "numeric",
        "lf.select(prod=pl.col('ints') * pl.col('floats'))",
        {"prod": [0.1, 0.4, 3 * 0.3, 1.6]},
    ),
    EvalParserScenario(
        "numeric",
        "lf.select(div=pl.col('ints') / 2)",
        {"div": [0.5, 1, 1.5, 2]},
    ),
    EvalParserScenario(
        "numeric",
        "lf.select(square=pl.col('ints') ** 2)",
        {"square": [1, 4, 9, 16]},
    ),
    EvalParserScenario(
        "numeric",
        "lf.select(mod=pl.col('ints') % 2)",
        {"mod": [1, 0, 1, 0]},
    ),
    EvalParserScenario(
        "select",
        "lf.select(plus_ten=(-pl.col('ints')) + 10)",
        {"plus_ten": [9, 8, 7]},
        connection_errors={"mysql": "You have an error in your SQL syntax"},
    ),
    EvalParserScenario(
        "grouping",
        "lf.group_by('keys').agg(pl.col('values').sum()).sort(by='keys').select('values').head(1)",
        {"values": [3]},
    ),
    EvalParserScenario(
        "grouping",
        "lf.filter(pl.col('values') != 1)",
        {"keys": [0, 1, 1], "values": [2, 3, 4]},
    ),
    EvalParserScenario(
        "grouping",
        "lf.filter(pl.col('keys') != 1)",
        {"keys": [0, 0], "values": [1, 2]},
    ),
    EvalParserScenario(
        "grouping",
        "lf.filter(pl.col('values') != 1)",
        {"keys": [0, 1, 1], "values": [2, 3, 4]},
    ),
    EvalParserScenario(
        "grouping",
        "lf.filter(pl.col('values') > 2).select('values')",
        {"values": [3, 4]},
    ),
    EvalParserScenario(
        "grouping",
        "lf.filter(pl.col('values') >= 2).select('values')",
        {"values": [2, 3, 4]},
    ),
    EvalParserScenario(
        "grouping",
        "lf.filter(pl.col('values') < 2).select('values')",
        {"values": [1]},
    ),
    EvalParserScenario(
        "grouping",
        "lf.filter(pl.col('values') <= 2).select('values')",
        {"values": [1, 2]},
    ),
    EvalParserScenario(
        "hundred",
        "lf.filter((pl.col('ints') % 5 == 0) & (pl.col('ints') % 7 == 0))",
        {"ints": [0, 35, 70]},
    ),
    EvalParserScenario(
        "hundred",
        "lf.filter(~(pl.col('ints') > 1) | ~(pl.col('ints') < 99))",
        {"ints": [0, 1, 99, 100]},
    ),
    EvalParserScenario(
        "nan_null_inf",
        "lf.select('null').fill_null(111)",
        {"null": [0.0, 111.0]},
        # This error message is generated upstream, and we can't change "can not".
        connection_errors={"mysql": (MYSQL_INF := "inf can not be used with MySQL")},
    ),
    EvalParserScenario(
        "nan_null_inf",
        "lf.select('nan').fill_nan(111)",
        {"nan": [0.0, 111.0]},
        connection_errors={"mysql": MYSQL_INF},
        backend_errors={
            "sqlite": "Compilation rule for 'IsNan' operation is not defined"
        },
    ),
    EvalParserScenario(
        "nan_null_inf",
        "lf.select(pl.col.null.fill_null(999))",
        {"null": [0, 999]},
        connection_errors={"mysql": MYSQL_INF},
    ),
    EvalParserScenario(
        "nan_null_inf",
        "lf.filter(pl.col('null') != 0)",
        {"inf": [], "nan": [], "null": []},
        connection_errors={"mysql": MYSQL_INF},
    ),
    EvalParserScenario(
        "numeric",
        "lf.select("
        "    floats=pl.col('floats').mean(),"
        "    ints=pl.col('ints').mean()"
        ")",
        {"floats": [0.25], "ints": [2.5]},
    ),
    EvalParserScenario(
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
    EvalParserScenario(
        "numeric",
        "lf.select("
        "    floats=pl.col('floats').sum(),"
        "    ints=pl.col('ints').sum()"
        ")",
        {"floats": [1.0], "ints": [10]},
    ),
    EvalParserScenario(
        "numeric",
        "lf.select("
        "    floats=pl.col('floats').min(),"
        "    ints=pl.col('ints').min()"
        ")",
        {"floats": [0.1], "ints": [1]},
        tolerance=0.0000001,
    ),
    EvalParserScenario(
        "numeric",
        "lf.select("
        "    floats=pl.col('floats').max(),"
        "    ints=pl.col('ints').max()"
        ")",
        {"floats": [0.4], "ints": [4]},
        tolerance=0.0000001,
    ),
    EvalParserScenario(
        "numeric",
        "lf.select("
        "    floats=pl.col('floats').std(),"
        "    ints=pl.col('ints').std()"
        ")",
        {"floats": [math.sqrt(5 / 3 / 100)], "ints": [math.sqrt(5 / 3)]},
        tolerance=0.00001,
    ),
    EvalParserScenario(
        "numeric",
        "lf.select("
        "    floats=pl.col('floats').var(),"
        "    ints=pl.col('ints').var()"
        ")",
        {"floats": [5 / 3 / 100], "ints": [5 / 3]},
        tolerance=0.00001,
    ),
    EvalParserScenario(
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
