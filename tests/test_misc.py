import re
import subprocess
from pathlib import Path

import pytest
import yaml

import polars_to_ibis

tests = {
    "pyright type checking": "pyright",
}


@pytest.mark.parametrize("cmd", tests.values(), ids=tests.keys())
def test_subprocess(cmd: str):
    result = subprocess.run(cmd, shell=True, check=False)
    assert result.returncode == 0, f'"{cmd}" failed'


def test_package_version_format():
    assert re.match(r"\d+\.\d+\.\d+", polars_to_ibis.__version__)


def get_test_versions(target: str):
    test_workflow = yaml.safe_load(
        (Path(__file__).parent.parent / ".github/workflows/test.yml").read_text()
    )
    matrix = test_workflow["jobs"]["test"]["strategy"]["matrix"]
    test_version = matrix.get(f"{target}-version")
    if test_version is not None:
        return test_version
    # If there's just one version, it won't be in the test matrix.
    return [eval(f"{target}.__version__")]


def get_doc_versions(target: str):
    docs = (Path(__file__).parent.parent / "src/polars_to_ibis/__init__.py").read_text()
    lines = [line for line in docs.splitlines() if f"{target} versions:" in line]
    assert len(lines) == 1
    joined_versions = lines[0].split(":")[1]
    return [v.strip() for v in joined_versions.split(", ")]


@pytest.mark.parametrize(
    "target",
    ["python", "polars", "ibis"],
)
def test_versions_consistent(target: str):
    test_versions = get_test_versions(target)
    doc_versions = get_doc_versions(target)
    assert test_versions == doc_versions


@pytest.mark.parametrize(
    "rel_path",
    [
        ".python-version",
        "pyproject.toml",
        "src/polars_to_ibis/__init__.py",
        "uv.lock",
        ".github/workflows/test.yml",
        "tests/test_misc.py",
    ],
)
def test_python_min_version(rel_path):
    root = Path(__file__).parent.parent
    text = (root / rel_path).read_text()
    assert "3.11" in text
