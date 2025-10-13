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
>>> ibis_unbound_table
r0 := UnboundTable: my_table
  ints int64
<BLANKLINE>
Limit[r0, n=1]

```
