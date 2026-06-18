# polars-to-ibis

[![pypi](https://img.shields.io/pypi/v/polars_to_ibis)](https://pypi.org/project/polars_to_ibis/)

Convert [Polars LazyFrames](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html) to [Ibis unbound tables](https://ibis-project.org/how-to/extending/unbound_expression#unbound-tables)

Polars and Ibis have similar APIs, but while Polars supports computation in-memory and on [Polars Cloud](https://cloud.pola.rs/), Ibis by itself does not handle computation: Instead it translates the dataframe expression into idiomatic SQL for a particular database.

## Example

```python
>>> import polars as pl
>>> from polars_to_ibis import convert_polars_to_ibis

>>> polars_lazy = pl.LazyFrame(schema=pl.Schema({"ints": pl.Int32}))
>>> polars_query = polars_lazy.sum()

>>> table_name = 'readme_example'
>>> ibis_unbound_table = convert_polars_to_ibis(polars_query, table_name=table_name)
>>> print(ibis_unbound_table.to_sql())
SELECT
  SUM("t0"."ints") AS "ints"
FROM "readme_example" AS "t0"

```

This is generic SQL: To connect to a particular database, you will need to install the appropriate extra. Taking SQLite as an example:

```shell
pip install 'ibis-framework[sqlite]'
```

Now we'll actually connect to the database, and create a very small table:

```python
>>> import ibis
>>> connection = ibis.sqlite.connect()
>>> connection.create_table(table_name, pl.DataFrame({"ints": [1, 2, 3, 4]}), overwrite=True)
DatabaseTable: readme_example
  ints int64

```

Finally, we can execute in SQLite the query which we constructed in Polars and translated to Ibis:

```python
>>> connection.to_polars(ibis_unbound_table).to_dict(as_series=False)
{'ints': [10]}

```

In this example we somewhat artificially started with a Polars LazyFrame.
In the real world, you more likely would start with a database.
To read a database table's schema and create from that a LazyFrame, use `scan_database`:

```python
>>> from polars_to_ibis import scan_database
>>> dict(scan_database(connection, table_name).collect_schema())
{'ints': Int64}

```


## Limitations

- Python versions: Tested against Python 3.10 and 3.13.
- Polars versions: Tested against Polars 1.32.0, 1.36.1, and 1.41.2.
- Ibis version: Tested against Ibis 11.0.0.
- Feature coverage, and database quirks: We only cover a fraction of the Polars API, and even within that range there are often quirks in how a query is handled by a given database. The best summary is the collection of [test fixtures](https://github.com/opendp/polars-to-ibis/blob/main/tests/fixtures.py).


## Contributions

There are several ways to contribute. First, if you find `polars_to_ibis` useful, please [let us know](mailto:contact@opendp.org) and we'll spend more time on this project. If `polars_to_ibis` doesn't work for you, we also want to know that! Please [file an issue](https://github.com/opendp/polars-to-ibis/issues/new/choose).

PRs that expand feature coverage are welcome. Please add a new fixtures to exercise new features, and run tests locally before submitting your PR.

If you have an idea that goes beyond just expanding coverage, please file an issue before beginning work, so we can make sure that your idea aligns with our roadmap.


## Development

### Getting Started

`polars_to_ibis` supports multiple Python versions, but for the fewest surprises during development, it makes sense to use the oldest supported version in a virtual environment. On MacOS:
```shell
$ git clone https://github.com/opendp/polars-to-ibis.git
$ cd polars-to-ibis
$ brew install python@3.10
$ python3.10 -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements-dev.txt
$ pre-commit install
$ pip install --editable .
```

### Testing

In-memory databases are handled by python and pip, but other databases covered by the tests will require installation and startup. (If you don't want to install extra database engines right now, they can be skipped during test runs: `pytest -k 'not extra_install'`)

On MacOS we recommend:
```shell
$ scripts/setup.sh
```

At this point, tests should pass, and code coverage should be complete (except blocks we explicitly ignore):
```shell
$ scripts/ci.sh
```

### Release

- Make sure you're up to date, and have the git-ignored credentials file `.pypirc`.
- Make one last feature branch with the new version number in the name:
  - Run `scripts/changelog.py` to update the `CHANGELOG.md`.
  - Review the updates and pull a couple highlights to the top.
  - Bump `polars_to_ibis/VERSION`, and add the new number at the top of the `CHANGELOG.md`.
  - Commit your changes, make a PR, and merge this branch to main.
- Update `main` with the latest changes: `git checkout main; git pull`
- Publish: `flit publish --pypirc .pypirc`

### Conventions

Branch names should be of the form `NNNN-short-description`, where `NNNN` is the issue number being addressed.

Add developer-only dependencies in `requirements-dev.in`; Add other dependencies in `requirements.in`. After an edit to either file run `scripts/requirements.py` to install the new dependency locally and update `pyproject.toml`.
