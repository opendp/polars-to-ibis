# CHANGELOG

## v0.0.0

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
