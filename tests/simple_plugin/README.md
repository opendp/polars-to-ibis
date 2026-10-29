# Native test plugin

This fixture adds an integer offset to an Int64 series. It tests the existing
FFI splitter without importing or depending on OpenDP.

`tests/test_split_plugin.py` builds the shared library once per pytest session
with `cargo build --locked`, so Rust and Cargo must be installed. Build failures
fail the test instead of skipping it. `CARGO_TARGET_DIR` can point to a shared
build cache. The library is loaded through Polars' plugin API; it is not a
Python extension package.
