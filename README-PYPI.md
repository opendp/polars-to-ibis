# polars-to-ibis

[![pypi](https://img.shields.io/pypi/v/polars_to_ibis)](https://pypi.org/project/polars_to_ibis/)

Convert [Polars LazyFrames](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html)
to [Ibis unbound tables](https://ibis-project.org/how-to/extending/unbound_expression#unbound-tables)

## Usage

Besides installing this library, you will also need to install the ibis-framework extra for your target database. For example, if you wanted to target SQLite:

```shell
pip install 'ibis-framework[sqlite]'
```

**🚧 Under Construction! 🚧**

```python
>>> import polars as pl
>>> from polars_to_ibis import convert_polars_to_ibis

>>> polars_lazy = pl.LazyFrame(schema=pl.Schema({"ints": pl.Int32}))
>>> polars_query = polars_lazy.sum()

>>> ibis_unbound_table = convert_polars_to_ibis(polars_query, table_name="my_table")
>>> print(ibis_unbound_table.to_sql())
SELECT
  SUM("t0"."ints") AS "ints"
FROM "my_table" AS "t0"

```
