import ibis
import polars as pl


def scan_database(ibis_backend_name: str, table_name: str, **connect_kwargs: str):
    connection = getattr(ibis, ibis_backend_name).connect(**connect_kwargs)
    schema = connection.get_schema(table_name)

    pl.LazyFrame(schema=schema)
