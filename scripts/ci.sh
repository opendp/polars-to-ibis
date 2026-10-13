#!/bin/bash

set -euo pipefail

if python -c 'import opendp'; then
    # If opendp is not installed, test_split is skipped,
    # and some functionality is not tested.
    coverage run -m pytest -vv --failed-first --durations=5 "$@"
    coverage report
else
    pytest -vv --failed-first --durations=5 "$@"
fi
