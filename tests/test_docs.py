import re
from json import loads

import ibis
import polars as pl
import pytest

import polars_to_ibis
from polars_to_ibis import convert_polars_to_ibis, scan_database


def get_code(source: str, tag: str):
    """
    >>> doc_string = '''
    ... ```abc
    ... hello
    ... world
    ... ```
    ... ```xyz
    ... not this!
    ... ```
    ... '''
    >>> get_code(doc_string, "abc")
    'hello\\nworld'
    """
    m = re.search(rf"```{tag}(.*?)```", source, flags=re.DOTALL)
    return m.group(1).strip()


def get_dataframe():
    """
    >>> get_dataframe()
    shape: ...
    """
    json_str = get_code(polars_to_ibis.__doc__, "json")
    json_data = loads(json_str)
    return pl.DataFrame(json_data)


def get_scenarios():
    """
    >>> get_scenarios()
    [('SELECT ...)]
    """
    sql_lines = get_code(polars_to_ibis.__doc__, "sql")
    sql_result_pairs = [re.split(r"\s*--\s*", line) for line in sql_lines.split("\n\n")]
    return [(sql, loads(result)) for (sql, result) in sql_result_pairs]


@pytest.mark.parametrize("scenario", get_scenarios())
def test_failing_docs(scenario):
    sql, expected_result = scenario

    backend = ibis.sqlite
    connection = backend.connect()
    table_name = "readme_example"
    connection.create_table(
        table_name,
        get_dataframe(),
        overwrite=True,
    )

    polars_lazy = scan_database(connection, table_name)

    polars_query = polars_lazy.sql(sql)
    ibis_unbound_table = convert_polars_to_ibis(
        polars_query,
        table_name=table_name,
        backend=backend,
    )
    actual_result = connection.to_polars(ibis_unbound_table).to_dict(as_series=False)

    assert actual_result == expected_result
