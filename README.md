# polars-to-ibis

[![pypi](https://img.shields.io/pypi/v/polars_to_ibis)](https://pypi.org/project/polars_to_ibis/)

Convert Polars expressions to Ibis expressions

## Usage

TODO

## Contributions

There are several ways to contribute. First, if you find `polars_to_ibis` useful, please [let us know](mailto:info@opendp.org) and we'll spend more time on this project. If `polars_to_ibis` doesn't work for you, we also want to know that! Please [file an issue](https://github.com/opendp/polars-to-ibis/issues/new/choose) and we'll look into it.

We also welcome PRs, but if you have an idea for a new feature, it may be helpful to get in touch before you begin, to make sure your idea is in line with our vision.

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

Tests should pass, and code coverage should be complete (except blocks we explicitly ignore):
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
