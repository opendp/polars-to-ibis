# polars-to-ibis

[![pypi](https://img.shields.io/pypi/v/polars_to_ibis)](https://pypi.org/project/polars_to_ibis/)

Convert [Polars LazyFrames](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html) to [Ibis unbound tables](https://ibis-project.org/how-to/extending/unbound_expression#unbound-tables).

Polars and Ibis have similar APIs, but while Polars supports computation in-memory and on [Polars Cloud](https://cloud.pola.rs/), Ibis by itself does not handle computation: Instead it translates the dataframe expression into idiomatic SQL for a particular database.

For examples of using `polars-to-ibis`, see the [API docs](https://opendp.github.io/polars-to-ibis).

## Contributions

There are several ways to contribute. First, if you find `polars_to_ibis` useful, please [let us know](mailto:contact@opendp.org) and we'll spend more time on this project. If `polars_to_ibis` doesn't work for you, we also want to know that! Please [file an issue](https://github.com/opendp/polars-to-ibis/issues/new/choose).

PRs that expand feature coverage are welcome. Please add a new scenarios to exercise new features, and run tests locally before submitting your PR.

If you have an idea that goes beyond just expanding coverage, please file an issue before beginning work, so we can make sure that your idea aligns with our roadmap.


## Development

### Getting Started

```shell
$ git clone https://github.com/opendp/polars-to-ibis.git
$ cd polars-to-ibis
$ pip install uv
$ uv sync
$ uv run pre-commit install
```

### Testing

In-memory databases are handled by python and pip, but other databases covered by the tests will require installation and startup. (If you don't want to install extra database engines right now, they can be skipped during test runs: `uv run pytest -k 'not extra_install'`)

On MacOS we recommend:
```shell
$ uv run scripts/setup.sh
```

At this point, tests should pass, and code coverage should be complete (except blocks we explicitly ignore):
```shell
$ uv run scripts/ci.sh
```

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
