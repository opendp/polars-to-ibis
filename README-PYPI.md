# polars-to-ibis

[![pypi](https://img.shields.io/pypi/v/polars_to_ibis)](https://pypi.org/project/polars_to_ibis/)

Convert [Polars LazyFrames](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html) to [Ibis unbound tables](https://ibis-project.org/how-to/extending/unbound_expression#unbound-tables)

Polars and Ibis have similar APIs, but while Polars supports computation in-memory and on [Polars Cloud](https://cloud.pola.rs/), Ibis by itself does not handle computation: Instead it translates the dataframe expression into idiomatic SQL for a particular database.

The public interface of `polars_to_ibis` consists of exactly one function: `convert_polars_to_ibis`.

## Example

```python
>>> import polars as pl
>>> from polars_to_ibis import convert_polars_to_ibis

>>> polars_lazy = pl.LazyFrame(schema=pl.Schema({"ints": pl.Int32}))
>>> polars_query = polars_lazy.sum()

>>> table_name = 'readme_example'
>>> ibis_unbound_table = convert_polars_to_ibis(polars_query, table_name=table_name)
>>> print(ibis_unbound_table.to_sql())
SELECT
  SUM("t0"."ints") AS "ints"
FROM "readme_example" AS "t0"

```

This is generic SQL: To connect to a particular database, you will need to install the appropriate extra. Taking SQLite as an example:

```shell
pip install 'ibis-framework[sqlite]'
```

Now we'll actually connect to the database, and create a very small table:

```python
>>> import ibis
>>> connection = ibis.sqlite.connect()
>>> connection.create_table(table_name, pl.DataFrame({"ints": [1, 2, 3, 4]}), overwrite=True)
DatabaseTable: readme_example
  ints int64

```

Finally, we can execute in SQLite the query which we constructed in Polars and translated to Ibis:

```python
>>> connection.to_polars(ibis_unbound_table).to_dict(as_series=False)
{'ints': [10]}

```


## Limitations

- Python versions: Tested against Python 3.10 and 3.13.
- Polars versions: Tested against Polars 1.32.0 and 1.36.1.
- Ibis version: Tested against Ibis 11.0.0.
- Feature coverage, and database quirks: We only cover a fraction of the Polars API, and even within that range there are often quirks in how a query is handled by a given database. The best summary is the collection of [test fixtures](https://github.com/opendp/polars-to-ibis/blob/main/tests/fixtures.py).


## Contributions

There are several ways to contribute. First, if you find `polars_to_ibis` useful, please [let us know](mailto:contact@opendp.org) and we'll spend more time on this project. If `polars_to_ibis` doesn't work for you, we also want to know that! Please [file an issue](https://github.com/opendp/polars-to-ibis/issues/new/choose).

PRs that expand feature coverage are welcome. Please add a new fixtures to exercise new features, and run tests locally before submitting your PR.

If you have an idea that goes beyond just expanding coverage, please file an issue before beginning work, so we can make sure that your idea aligns with our roadmap.
