#!/bin/bash

set -euo pipefail

# PostgreSQL:
PG='postgresql@16'
brew install $PG
brew services stop $PG || echo "$PG not already running? Continue..."
brew services start $PG
PG_PRE=$( brew --prefix $PG )/bin

for ((I = 0 ; I < 20 ; I++)); do
  echo "$I: Create postgres database: $USER"
  # Tests will create and drop "default_table" in this database:
  $PG_PRE/dropdb $USER || echo "No pre-existing DB?"
  $PG_PRE/createdb $USER && break
  echo 'Try again...'
  sleep 1
done


# MySQL:
# "pkg-config" is required by Python connector:
# https://github.com/PyMySQL/mysqlclient/blob/main/README.md#macos-homebrew
brew install pkg-config

MY='mysql@8.4'
brew install $MY
brew services stop $MY || echo "$MY not already running? Continue..."
brew services start $MY
MY_PRE=$( brew --prefix $MY )/bin

for ((I = 0 ; I < 20 ; I++))
do
  CMD="DROP USER '$USER'@'%'"
  echo "$I: Drop mysql user: $CMD"
  $MY_PRE/mysql -u root -e "$CMD" || echo "No pre-existing user?"

  CMD="CREATE USER '$USER'@'%'"
  echo "$I: Create mysql user: $CMD"
  $MY_PRE/mysql -u root -e "$CMD" && break
  echo 'Try again...'
  sleep 1
done
# Tests will create and drop "default_table" in this database:
CMD="DROP DATABASE $USER"
echo "Drop database: $CMD"
$MY_PRE/mysql -u root -e "$CMD" || echo "No pre-existing database?"

CMD="CREATE DATABASE $USER"
echo "Create database: $CMD"
$MY_PRE/mysql -u root -e "$CMD"

# Make sure '*' is passed through verbatim:
CMD="GRANT ALL PRIVILEGES ON $USER."'*'" TO '$USER'@'%' WITH GRANT OPTION"
echo "Grant privs: $CMD"
$MY_PRE/mysql -u root -e "$CMD"
