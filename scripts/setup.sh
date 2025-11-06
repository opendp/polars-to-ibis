#!/bin/bash

set -euo pipefail

# PostgreSQL:
brew install postgresql
brew services run postgresql

while true
do
  # Tests will create and drop "default_table" in this database:
  createdb $USER && break || echo 'Try again...'
  sleep 1
done
