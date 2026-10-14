import re
import subprocess
from pathlib import Path

import pytest
import tomlkit
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


ROOT = Path(__file__).parent.parent


def get_test_versions(target: str):
    test_workflow = yaml.safe_load((ROOT / ".github/workflows/test.yml").read_text())
    matrix = test_workflow["jobs"]["test"]["strategy"]["matrix"]
    test_version = matrix.get(f"{target}-version")
    if test_version is not None:
        return test_version
    # If there's just one version, it won't be in the test matrix.
    assert target == "ibis"
    import ibis

    return [ibis.__version__]


def get_doc_versions(target: str):
    docs = (ROOT / "src/polars_to_ibis/__init__.py").read_text()
    lines = [line for line in docs.splitlines() if f"{target} versions:" in line]
    assert len(lines) == 1
    joined_versions = lines[0].split(":")[1]
    return [v.strip() for v in joined_versions.split(", ")]


def get_readme_versions(target: str):
    assert target == "polars"
    readme = (ROOT / "README.md").read_text()
    return re.findall(rf"--group {target}-(\S+)", readme)


def get_group_name_versions(target: str):
    assert target == "polars"
    pyproject = tomlkit.loads((ROOT / "pyproject.toml").read_text())
    names = [name for name in pyproject["dependency-groups"] if target in name]
    return [name.split("-")[1] for name in names]


def get_group_pin_versions(target: str):
    assert target == "polars"
    pyproject = tomlkit.loads((ROOT / "pyproject.toml").read_text())
    pins = [
        [
            dep
            for dep in pyproject["dependency-groups"][f"{target}-{v}"]
            if f"{target}==" in dep
        ]
        for v in get_group_name_versions(target)
    ]
    return [pin[0].split("==")[1] for pin in pins]


@pytest.mark.parametrize(
    "target",
    ["python", "polars", "ibis"],
)
def test_versions_consistent(target: str):
    test_versions = get_test_versions(target)
    doc_versions = get_doc_versions(target)
    assert test_versions == doc_versions


@pytest.mark.parametrize(
    "function",
    [function for name, function in locals().items() if name.startswith("get_")],
)
def test_polars_version_consistent(function):
    target = "polars"
    readme_versions = get_readme_versions(target)
    other_versions = function(target)
    assert readme_versions == other_versions


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
    text = (ROOT / rel_path).read_text()
    assert "3.11" in text
