# CHANGELOG

## v0.2.0

- Split LazyFrame on FFI plugin with `split_polars_on_ffi` (for a very limited set of expressions) [#93](https://github.com/opendp/polars-to-ibis/pull/93)
- Add `scan_database` to create a new LazyFrame with a database table's schema  [#98](https://github.com/opendp/polars-to-ibis/pull/98)

Also:

- Upgrade and record hashes for precommit hooks [#104](https://github.com/opendp/polars-to-ibis/pull/104)
- Migrate to uv [#102](https://github.com/opendp/polars-to-ibis/pull/102)
- Reorder README to avoid calling pl.LazyFrame() explicitly [#100](https://github.com/opendp/polars-to-ibis/pull/100)
- Support polars 1.41.2 [#95](https://github.com/opendp/polars-to-ibis/pull/95)
- Just overwrite, instead of trying to delete first [#99](https://github.com/opendp/polars-to-ibis/pull/99)
- Support polars 1.36.1 [#94](https://github.com/opendp/polars-to-ibis/pull/94)

## v0.1.0

Initial release. This provides one public function, `convert_polars_to_ibis`, which converts Polars LazyFrames to Ibis unbound tables.

Partially supported databases:
- SQLite
- Postgres
- MySQL
- DuckDB

Partially supported operations:
- Selecting, dropping, and adding columns
- Sorting
- Filtering
- Aggregate functions (`mean`, `median`, `min`, `max`, `std`, `var`, `quantile`, `sum`)
- Grouping
- Mathetical operators (`+`, `-`, `*`, `/`, `**`, `%`)
- Logical operators (`&`, `|`, `~`)
- Comparison operators (`==`, `!=`, `<`, `>`, `<=`, `>=`)
- Data cleaning (`fill_nan`, `fill_null`, `cast`)
- Slicing (`head`)
