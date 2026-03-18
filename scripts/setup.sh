#!/bin/bash

set -euo pipefail


# PostgreSQL:
brew install postgresql@16
brew services run postgresql@16

for i in {1..10}
do
  # Tests will create and drop "default_table" in this database:
  createdb $USER && break || echo 'Try again...'
  sleep 1
done


# MySQL:
brew install mysql@8.4
brew services run mysql@8.4

for i in {1..10}
do
  mysql -u root -e "CREATE USER $USER" && break || echo 'Try again...'
  sleep 1
done
# Tests will create and drop "default_table" in this database:
mysql -u root -e "CREATE DATABASE $USER"
mysql -u root -e "GRANT ALL ON $USER.* TO '$USER'@'%'"
