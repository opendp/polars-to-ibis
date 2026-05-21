import re
import subprocess
from pathlib import Path

import pytest
import yaml

import polars_to_ibis

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


def test_readme():
    root = Path(__file__).parent.parent
    long = (root / "README.md").read_text().strip()
    short = (root / "README-PYPI.md").read_text().strip()
    assert short in long


def test_polars_versions():
    test_workflow = yaml.safe_load(
        (Path(__file__).parent.parent / ".github/workflows/test.yml").read_text()
    )
    ci_matrix = test_workflow["jobs"]["test"]["strategy"]["matrix"]["polars-version"]
    from polars_to_ibis import _max_polars  # pyright: ignore[reportPrivateUsage]
    from polars_to_ibis import _min_polars  # pyright: ignore[reportPrivateUsage]

    assert _min_polars in ci_matrix
    assert _max_polars in ci_matrix

    requirements_in = (Path(__file__).parent.parent / "requirements.in").read_text()
    polars_requirement = [
        line for line in requirements_in.splitlines() if "polars" in line
    ]
    assert len(polars_requirement) == 1
    assert f">={_min_polars}" in polars_requirement[0]


def test_extras_in_case_statements():
    for file in ["table_handlers.py", "value_handlers.py"]:
        src = (
            Path(__file__).parent.parent / "polars_to_ibis/_parse" / file
        ).read_text()
        matches = re.findall(r"case [\[{(].*?[\])}]:", src, flags=re.DOTALL)
        for case_match in matches:
            # remove white space:
            case_match = re.sub(r"\s+", "", case_match)
            # remove trailing commas:
            case_match = re.sub(r",([\])}])", r"\1", case_match)
            # look just before closing braces:
            extra_matches = re.findall(r"(?:\w*)\}", case_match)
            for extra_match in extra_matches:
                has_extras = extra_match.startswith("extras")
                assert (
                    has_extras
                ), f'Add "**extras" near "{extra_match}" in:\n{case_match}\n\n'


# @pytest.mark.parametrize(
#     "rel_path",
#     [
#         "polars_to_ibis/__init__.py",
#         "README.md",
#         "README-PYPI.md",
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
