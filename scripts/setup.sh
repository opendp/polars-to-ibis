#!/bin/bash

set -euo pipefail


# PostgreSQL:
# pl/python is not part of the default homebrew install,
# so we'll use a third-party.
# This is used as the example for "brew tab",
# so seems trust-worthy, if a bit behind main.
brew tap petere/postgresql
brew install petere/postgresql/postgresql@16

psql postgres \
  -c 'create extension plpython3u'

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
