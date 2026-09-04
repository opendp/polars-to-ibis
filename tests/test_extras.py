import re
from pathlib import Path

import pytest


def strip_comments(src: str):
    """
    >>> print(strip_comments("first()\\nsecond() # third\\nfourth()"))
    first()
    second()
    fourth()
    """
    return re.sub(r"\s*#.*", "", src)


def find_case_and_next_line(src: str):
    """
    >>> src = '''
    ... ignore
    ... case (something):
    ...     ignore
    ... '''
    >>> find_case_and_next_line(src)
    ['case(something):']
    """
    matches = re.findall(r"case [\[{(].*?[\])}]:", src, flags=re.DOTALL)
    cleaned: list[str] = []
    for case_match in matches:
        # remove white space:
        case_match = re.sub(r"\s+", "", case_match)
        # remove trailing commas:
        case_match = re.sub(r",([\])}])", r"\1", case_match)
        cleaned.append(case_match)
    return cleaned


case_matches: list[str] = []

for path in (Path(__file__).parent.parent / "src/polars_to_ibis/_parse").glob(
    "*_handlers.py"
):
    case_matches += find_case_and_next_line(strip_comments(path.read_text()))


@pytest.mark.parametrize("case_match", case_matches)
def test_extras_last_in_dict_in_case_statements(case_match: str):
    extra_matches = re.findall(r"(?:\w*)\}", case_match)
    for extra_match in extra_matches:
        has_extras = extra_match.startswith("extras")
        assert has_extras, f'Add "**extras" near "{extra_match}" in:\n{case_match}\n\n'
