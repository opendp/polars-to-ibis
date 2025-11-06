#!/bin/bash

set -euo pipefail

# PostgreSQL:
brew install postgresql
brew services run postgresql
# Tests will create and drop "default_table" in this database:
createdb $USER
