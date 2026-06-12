import ibis
import polars as pl

from polars_to_ibis._scan import scan_database


def test_scan_database():
    backend = "sqlite"
    table_name = "default_table"

    # Used to populate DB, not downstream.
    connection = getattr(ibis, backend).connect()  # type: ignore
    input_df = pl.DataFrame(
        {
            "ints": [1, 2, 3, 4],
            "floats": [0.1, 0.2, 0.3, 0.4],
        }
    )

    connection.create_table(table_name, input_df, overwrite=True)  # type: ignore
    scan_database(connection, table_name)
