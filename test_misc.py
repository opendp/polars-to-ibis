import re
import subprocess

import pytest

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
