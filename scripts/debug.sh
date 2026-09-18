#!/bin/bash

set -euo pipefail

pytest -vv --sw --log-cli-level=DEBUG --log-format='%(levelname)s %(message)s' "$@"
