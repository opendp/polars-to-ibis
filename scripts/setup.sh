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


# MySQL:
brew install mysql
brew services run mysql

while true
do
  mysql -u root -e "CREATE USER $USER" && break || echo 'Try again...'
  sleep 1
done
# Tests will create and drop "default_table" in this database:
mysql -u root -e "CREATE DATABASE $USER"
mysql -u root -e "GRANT ALL ON $USER.* TO '$USER'@'%'"
