# polars-to-ibis

[![pypi](https://img.shields.io/pypi/v/polars_to_ibis)](https://pypi.org/project/polars_to_ibis/) [![docs](https://img.shields.io/badge/docs-latest-blue)](https://opendp.github.io/polars-to-ibis)

Convert [Polars LazyFrames](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html) to [Ibis unbound tables](https://ibis-project.org/how-to/extending/unbound_expression#unbound-tables).

Polars and Ibis have similar APIs, but while Polars supports computation in-memory and on [Polars Cloud](https://cloud.pola.rs/), Ibis by itself does not handle computation: Instead it translates the dataframe expression into idiomatic SQL for a particular database.

## Contributions

There are several ways to contribute. First, if you find `polars_to_ibis` useful, please [let us know](mailto:contact@opendp.org) and we'll spend more time on this project. If `polars_to_ibis` doesn't work for you, we also want to know that! Please [file an issue](https://github.com/opendp/polars-to-ibis/issues/new/choose).

PRs that expand feature coverage are welcome. Please add a new scenarios to exercise new features, and run tests locally before submitting your PR.

If you have an idea that goes beyond just expanding coverage, please file an issue before beginning work, so we can make sure that your idea aligns with our roadmap.


## Development

### Getting Started

Install [Rust and Cargo](https://rust-lang.org/tools/install/) for the native
plugin test, and make sure `cargo` is on your `PATH`.

```shell
$ git clone https://github.com/opendp/polars-to-ibis.git
$ cd polars-to-ibis
$ pip install uv
$ uv sync --group polars-1.36.1
$ uv run pre-commit install
```

### Testing

In-memory databases are handled by python and pip, but other databases covered by the tests will require installation and startup. (If you don't want to install extra database engines right now, they can be skipped during test runs: `uv run pytest -k 'not extra_install'`)

On MacOS we recommend:
```shell
$ uv run scripts/setup.sh
```

At this point, tests should pass for any supported dependency version, and code coverage should be complete (except blocks we explicitly ignore):
```shell
$ uv run --group polars-1.36.1 scripts/ci.sh
$ uv run --group polars-1.37.0 scripts/ci.sh
```

The `debug.sh` script may also be helpful: It runs tests step-wise with debug logging. Both `debug.sh` and `ci.sh` pass extra parameters through to pytest.

### Documentation

To build the docs locally:
```
pdoc -o docs/ src/polars_to_ibis
```

The Github Pages [docs site](https://opendp.github.io/polars-to-ibis/polars_to_ibis.html) is updated automatically on pushes to main, so it may be ahead of the latest release.

### Scalar FFI aggregates and postprocessing

`split_polars_on_ffi_plan(query, table_name, backend)` provides a two-stage
alternative to `split_polars_on_ffi`. It extracts one SQL aggregate for each
FFI occurrence and keeps the remaining supported expression for evaluation
after the caller applies the plugins. For example, a private mean containing
two plugins is split into separate sum and count columns: division happens
after both values have been noised, rather than in SQL before noise is added.

The returned `FfiSplitPlan` has:

- `ibis_table`: an unbound Ibis table returning one row of private aggregates.
- `plugins`: ordered records with `name` (the SQL/noised column name),
  `parameters` (an opaque copy of the original FFI metadata), and `input_dtype`
  (the original Polars aggregate dtype).
- `postprocess(noisy_values)`: evaluates the residual expression on one row
  containing exactly those named noised columns. Column order may vary.

The caller must validate **all** plugin parameters and prepare the corresponding
mechanisms before executing SQL. Then it obtains the private aggregates,
applies the appropriate plugin once per record, and passes only the resulting
noised values to `postprocess`. Preserve each record's dtype when applying its
plugin and constructing the noised frame: SQL may return an Int64 count even
though the original Polars count was UInt32. Identical plugin occurrences are
kept separate; this API does not share noise samples between them.

This path currently accepts a scalar Select over one DataFrameScan (including
the single IR wrapper emitted by OpenDP). Supported aggregate inputs are Len,
Sum, Count, and limited aggregate casts, with explicit data-only row
expressions for null/NaN filling, clipping, and basic arithmetic. The residual
accepts aliases, finite numeric/boolean/null literals, and basic binary
arithmetic/comparisons. Raw private columns outside plugins, nested plugins,
UDFs, other residual functions, grouping, and multi-stage relations are
rejected. The SQL table must correspond to the query's source schema.

The SQL aggregate stage preserves Polars' empty/all-null sum of zero, and
distinguishes all-row counts from nonnull counts. Other backend limitations
still apply; for example, floating NaN handling is not supported by SQLite's
Ibis compiler. This splitter does not execute the original query, deserialize
arbitrary expressions, load FFI libraries, interpret plugin metadata, add noise,
or perform privacy accounting. `postprocess` cannot verify that values were
actually noised. A consumer must establish the privacy and numerical
preconditions of the original query and the target backend separately. Retaining
the input dtype does not establish identical accumulation arithmetic: for
example, a SQL engine can accumulate a Float32 sum at a different precision
than Polars. This API does not establish a new differential privacy guarantee
for arbitrary SQL aggregates.

The existing tuple-returning `split_polars_on_ffi` API is unchanged.

### Release

- Make one last feature branch with the new version number in the name:
  - Run `uv run scripts/changelog.py` to update the `CHANGELOG.md`.
  - Review the updates and pull a couple highlights to the top.
  - `uv version --bump minor`, and add the new number at the top of the `CHANGELOG.md`.
  - Commit your changes, make a PR, and merge this branch to main.
- Update `main` with the latest changes: `git checkout main; git pull`
- Build: `uv build`
- With `~/.pypirc` in place, run `uvx uv-publish`.

### Conventions

Branch names should be of the form `NNNN-short-description`, where `NNNN` is the issue number being addressed.
