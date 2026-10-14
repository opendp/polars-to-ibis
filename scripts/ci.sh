#!/bin/bash

set -euo pipefail

if python -c 'import polars as pl; assert pl.__version__ == "1.36.1"'; then
    # This is the polars version pinned by opendp:
    # if not present, some functionality is not tested.
    coverage run -m pytest -vv --failed-first --durations=5 "$@"
    coverage report
else
    pytest -vv --failed-first --durations=5 "$@"
fi
