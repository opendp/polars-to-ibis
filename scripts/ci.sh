#!/bin/bash

set -euo pipefail

coverage run -m pytest -vv --failed-first --durations=5 $@
coverage report
