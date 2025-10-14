# polars-to-ibis

[![pypi](https://img.shields.io/pypi/v/polars_to_ibis)](https://pypi.org/project/polars_to_ibis/)

Convert Polars expressions to Ibis expressions

## Usage

**🚧 Under Construction! 🚧**

```python
>>> import polars as pl
>>> from polars_to_ibis import polars_to_ibis

>>> data = {
...     "ints": [1, 2, 3, 4],
... }
>>> polars_df = pl.DataFrame(data)
>>> polars_lazy = polars_df.lazy().head(1)

>>> ibis_unbound_table = polars_to_ibis(polars_lazy, table_name="my_table")
>>> print(ibis_unbound_table.to_sql())
SELECT
  *
FROM "my_table" AS "t0"
LIMIT 1

```
