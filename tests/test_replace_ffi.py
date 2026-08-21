import pytest

from polars_to_ibis._utils import replace_ffi_with_input

replace_ffi_scenarios = [
    (
        {
            "Select": {
                "expr": [
                    {
                        "Function": {
                            "function": {"FfiPlugin": "flags-lib-symbol-kwargs"},
                            "input": ["Len"],
                        }
                    }
                ]
            }
        },
        {"Select": {"expr": ["Len"]}},
    ),
    (
        {
            "Alias": [
                {
                    "Function": {
                        "input": ["Len"],
                        "function": {"FfiPlugin": "flags-lib-symbol-kwargs"},
                    }
                },
                "new_name",
            ]
        },
        {"Alias": ["Len", "new_name"]},
    ),
    (
        {
            "Select": {
                "expr": [
                    {
                        "Alias": [
                            {
                                "Function": {
                                    "input": ["Len"],
                                    "function": {
                                        "FfiPlugin": "flags-lib-symbol-kwargs"
                                    },
                                }
                            },
                            "new_name",
                        ]
                    }
                ],
                "input": "input-IR",
                "options": "options",
            }
        },
        {
            "Select": {
                "expr": [
                    {
                        "Alias": ["Len", "new_name"],
                    },
                ],
                "input": "input-IR",
                "options": "options",
            },
        },
    ),
]


@pytest.mark.parametrize(
    "scenario",
    replace_ffi_scenarios,
    ids=lambda scenario: scenario[1],
)
def test_replace_ffi_with_input(scenario):
    source, expected_source = scenario
    ffi_details = replace_ffi_with_input(source)
    assert ffi_details == "flags-lib-symbol-kwargs"
    assert source == expected_source
