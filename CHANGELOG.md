# CHANGELOG

## v0.3.0

Functionality:

- support SQL DISTINCT [#155](https://github.com/opendp/polars-to-ibis/pull/155)
- Handle more test cases [#141](https://github.com/opendp/polars-to-ibis/pull/141)
- Allow ibis generation to depend on features of target DB [#123](https://github.com/opendp/polars-to-ibis/pull/123)
- handle len along with other select expressions [#120](https://github.com/opendp/polars-to-ibis/pull/120)
- Handle `dp.sum` and some column rename functions [#108](https://github.com/opendp/polars-to-ibis/pull/108)

Documentation:

- Move example from README to API docs [#127](https://github.com/opendp/polars-to-ibis/pull/127)
- Add py.typed and docs site [#126](https://github.com/opendp/polars-to-ibis/pull/126)

Internals and cleanup:

- Polish before release [#154](https://github.com/opendp/polars-to-ibis/pull/154)
- fix merge [#140](https://github.com/opendp/polars-to-ibis/pull/140)
- Fill in SQL tests [#138](https://github.com/opendp/polars-to-ibis/pull/138)
- Framework for SQL tests [#137](https://github.com/opendp/polars-to-ibis/pull/137)
- Clean up expression handling [#134](https://github.com/opendp/polars-to-ibis/pull/134)
- remove old commented-out blocks [#132](https://github.com/opendp/polars-to-ibis/pull/132)
- More serialization constants [#129](https://github.com/opendp/polars-to-ibis/pull/129)
- Add tag name constants [#118](https://github.com/opendp/polars-to-ibis/pull/118)
- better recursion logic in `replace_ffi_with_input` [#112](https://github.com/opendp/polars-to-ibis/pull/112)

CI:

- check precommits in CI [#136](https://github.com/opendp/polars-to-ibis/pull/136)
- add missing build step [#107](https://github.com/opendp/polars-to-ibis/pull/107)

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
