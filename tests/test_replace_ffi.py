import pytest

from polars_to_ibis._utils import PluginReplacer

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
        ["flags-lib-symbol-kwargs"],
        {"Select": {"expr": ["Len"]}},
    ),
    (
        {
            "Select": {
                "expr": [
                    {
                        "Function": {
                            "function": {"FfiPlugin": "Len-flags-lib-symbol-kwargs"},
                            "input": ["Len"],
                        }
                    },
                    {
                        "Function": {
                            "function": {"FfiPlugin": "Sum-flags-lib-symbol-kwargs"},
                            "input": [{"Agg": {"Sum": "sum"}}],
                        }
                    },
                ],
                "input": "input",
                "options": "options",
            }
        },
        ["Len-flags-lib-symbol-kwargs", "Sum-flags-lib-symbol-kwargs"],
        {
            "Select": {
                "expr": ["Len", {"Agg": {"Sum": "sum"}}],
                "input": "input",
                "options": "options",
            }
        },
    ),
    # TODO: Add an end-to-end test with alias(),
    # to confirm that this is actually the right behavior.
    # (
    #     {
    #         "Alias": [
    #             {
    #                 "Function": {
    #                     "input": ["Len"],
    #                     "function": {"FfiPlugin": "flags-lib-symbol-kwargs"},
    #                 }
    #             },
    #             "new_name",
    #         ]
    #     },
    #     {"Alias": ["Len", "new_name"]},
    # ),
    # (
    #     {
    #         "Select": {
    #             "expr": [
    #                 {
    #                     "Alias": [
    #                         {
    #                             "Function": {
    #                                 "input": ["Len"],
    #                                 "function": {
    #                                     "FfiPlugin": "flags-lib-symbol-kwargs"
    #                                 },
    #                             }
    #                         },
    #                         "new_name",
    #                     ]
    #                 }
    #             ],
    #             "input": "input-IR",
    #             "options": "options",
    #         }
    #     },
    #     {
    #         "Select": {
    #             "expr": [
    #                 {
    #                     "Alias": ["Len", "new_name"],
    #                 },
    #             ],
    #             "input": "input-IR",
    #             "options": "options",
    #         },
    #     },
    # ),
]


@pytest.mark.parametrize(
    "scenario",
    replace_ffi_scenarios,
    ids=lambda scenario: scenario[2],
)
def test_replace_ffi_with_input(scenario):
    source, expected_params, expected_source = scenario
    ffi_params = PluginReplacer(source).replace()
    assert ffi_params == expected_params
    assert source == expected_source
