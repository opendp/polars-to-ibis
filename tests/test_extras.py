import re
from pathlib import Path

import pytest


def strip_comments(src):
    """
    >>> print(strip_comments("first()\\nsecond() # third\\nfourth()"))
    first()
    second()
    fourth()
    """
    return re.sub(r"\s*#.*", "", src)


case_matches: list[str] = []

for file in ["table_handlers.py", "value_handlers.py"]:
    src = (
        Path(__file__).parent.parent / "src/polars_to_ibis/_parse" / file
    ).read_text()
    src = strip_comments(src)
    matches = re.findall(r"case [\[{(].*?[\])}]:\n[^\n]+\n", src, flags=re.DOTALL)
    for case_match in matches:
        # remove white space:
        case_match = re.sub(r"\s+", "", case_match)
        # remove trailing commas:
        case_match = re.sub(r",([\])}])", r"\1", case_match)
        case_matches.append(case_match)


@pytest.mark.parametrize("case_match", case_matches)
def test_extras_last_in_dict_in_case_statements(case_match: str):
    extra_matches = re.findall(r"(?:\w*)\}", case_match)
    for extra_match in extra_matches:
        has_extras = extra_match.startswith("extras")
        assert has_extras, f'Add "**extras" near "{extra_match}" in:\n{case_match}\n\n'
