from os import environ

import ibis  # type: ignore
import polars as pl

from polars_to_ibis._parser import translate_table
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
    # Each backend raises its own error type.
    try:
        connection.drop_table(table_name)
    except BaseException:  # noqa: B036
        pass
    connection.create_table(table_name, df)

    return (connection, table_name)


#
# Tests
#


def test_translate_table():
    # TODO: More interesting data.
    df = pl.DataFrame(
        {
            "ints": [1, 2, 3, 4],
            "floats": [0.1, 0.2, 0.3, 0.4],
        }
    )
    lf = df.lazy().sum()

    # TODO: Move out of test.
    node = Serialization(lf)._serial  # type: ignore

    # TODO: Move out of test.
    polars_schema = lf.collect_schema()
    ibis_schema = ibis.expr.schema.Schema.from_polars(polars_schema)
    table_name = "default_table"
    ibis_table = ibis.table(ibis_schema, name=table_name)  # type: ignore

    # TODO: Test at higher level.
    new_table = translate_table(node=node, table=ibis_table)

    connection, table_name = get_connection_table_name(df, "sqlite")
    records = connection.to_pandas(new_table).to_dict(orient="list")
    assert records == {"floats": [1.0], "ints": [10]}
