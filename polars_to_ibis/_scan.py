from typing import Any

import polars as pl


def scan_database(connection: Any, table_name: str):
    ibis_schema = connection.get_schema(table_name)
    return pl.LazyFrame(schema=ibis_schema.to_polars())
