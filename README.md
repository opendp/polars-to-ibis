# polars-to-ibis

[![pypi](https://img.shields.io/pypi/v/polars_to_ibis)](https://pypi.org/project/polars_to_ibis/) [![docs](https://img.shields.io/badge/docs-latest-blue)](https://opendp.github.io/polars-to-ibis)

Convert [Polars LazyFrames](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html) to [Ibis unbound tables](https://ibis-project.org/how-to/extending/unbound_expression#unbound-tables).

Polars and Ibis have similar APIs, but while Polars supports computation in-memory and on [Polars Cloud](https://cloud.pola.rs/), Ibis by itself does not handle computation: Instead it translates the dataframe expression into idiomatic SQL for a particular database.

## OpenDP sparse histogram heads

`convert_polars_to_ibis` recognizes OpenDP's internal
`discrete_quantile_histogram_plugin` aggregate and its native sparse-struct
flattening. It lowers that logical relation directly to typed bucket assignment
and `COUNT(*) GROUP BY` the original keys and bucket ID. Binning is a converter
implementation choice; the current implementation uses balanced Ibis CASE
expressions. SQL does not build lists, collect rows into arrays, or UNNEST them.

Expression-local filtering, `drop_nulls`, `drop_nans`, replacement and casts apply
only to the histogram's contributing values. A null-safe join with the original
private observed-key domain retains groups with no contributions as a zero-count
histogram. Empty global input still has one aggregate result. Literal input is
one observation per group (or one globally), unless a row-dependent filter
broadcasts that literal. Public-key imputation remains in OpenDP's shared
continuation, not this SQL lowering.

The converter leaves exact score reconstruction, noisy max, candidate lookup,
other noise and cross-head postprocessing in Polars. The existing
`split_polars_on_ffi` scalar-plugin eraser is **not** this protocol: convert each
deterministic head relation with `convert_polars_to_ibis` instead. Protected IPC,
snapshots, one-shot release and cleanup remain the trusted executor's job.

This route is tested with real OpenDP-generated heads, Polars 1.36.1 and
Ibis/DuckDB. Other backends must support the required exact numeric types,
null-safe joins, TRUNC for float-to-integer conversion, and TRY_CAST for non-strict
narrowing casts; no floating-point
substitution is made for large integers. Polars schema inference (without
executing input data) preserves contextual dynamic-literal coercion at each
operation, including Float32 fills and intermediate arithmetic. Strong typed
literals retain their explicit dtype.
Independent positional selections inside one expression are explicitly
unsupported rather than silently translated as an AND of row predicates. The
plugin metadata decoder accepts primitive pickle data only, not executable
pickle objects. Plugin names and library provenance follow OpenDP's existing
resolved-plan convention, not cryptographic authentication.

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
