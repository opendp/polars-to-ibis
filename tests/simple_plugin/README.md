# Native test plugin

This fixture adds an integer offset to an Int64 series. It tests the existing
FFI splitter without importing or depending on OpenDP.

`tests/test_split_plugin.py` builds the shared library once per pytest session
with `cargo build --locked`, so Rust and Cargo must be installed. Build failures
fail the test instead of skipping it. `CARGO_TARGET_DIR` can point to a shared
build cache. The library is loaded through Polars' plugin API; it is not a
Python extension package.

The Rust dependencies are pinned to Polars 0.52 and pyo3-polars 0.25, matching
the plugin ABI used by the project's Polars 1.36.1 dependency. The same fixture
also runs in the existing Polars 1.37.0 test group. New versions should be
checked explicitly before updating these pins.

The test retains the existing SQLite, DuckDB and PostgreSQL parameterization.
PostgreSQL uses the existing `extra_install` marker and requires the usual
local database setup.
