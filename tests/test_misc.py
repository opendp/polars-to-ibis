import re
import subprocess
from pathlib import Path

import pytest
import yaml

import polars_to_ibis
from polars_to_ibis import _MAX_POLARS as MAX_POLARS
from polars_to_ibis import _MIN_POLARS as MIN_POLARS

tests = {
    "flake8 linting": "flake8 . --count --show-source --statistics",
    "pyright type checking": "pyright",
}


@pytest.mark.parametrize("cmd", tests.values(), ids=tests.keys())
def test_subprocess(cmd: str):
    result = subprocess.run(cmd, shell=True)
    assert result.returncode == 0, f'"{cmd}" failed'


def test_version():
    assert re.match(r"\d+\.\d+\.\d+", polars_to_ibis.__version__)


def get_tested_polars_versions():
    test_workflow = yaml.safe_load(
        (Path(__file__).parent.parent / ".github/workflows/test.yml").read_text()
    )
    return test_workflow["jobs"]["test"]["strategy"]["matrix"]["polars-version"]


def test_polars_versions_in_ci_matrix():
    tested_polars_versions = get_tested_polars_versions()
    assert MIN_POLARS in tested_polars_versions
    assert MAX_POLARS in tested_polars_versions


def test_polars_versions_in_readme():
    readme = (Path(__file__).parent.parent / "README.md").read_text()
    assert all(v in readme for v in get_tested_polars_versions())


# @pytest.mark.parametrize(
#     "rel_path",
#     [
#         "polars_to_ibis/__init__.py",
#         "README.md",
#         ".github/workflows/test.yml",
#         "pyproject.toml",
#     ],
# )
# def test_python_min_version(rel_path):
#     root = Path(__file__).parent.parent
#     text = (root / rel_path).read_text()
#     assert "3.10" in text
#     if "README" in rel_path:
#         # Make sure we haven't upgraded one reference by mistake.
#         assert not re.search(r"3.1[^0]", text)
