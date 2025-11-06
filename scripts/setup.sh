#!/bin/bash

set -euo pipefail

# PostgreSQL:
brew install postgresql
# Configure PostgreSQL to start on boot:
brew services start postgresql
# Tests will create and drop "default_table" in this database:
createdb $USER
