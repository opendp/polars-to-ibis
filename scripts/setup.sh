#!/bin/bash

set -euo pipefail

RETRIES=20

# PostgreSQL:
PG='postgresql@16'
brew install $PG
brew services start $PG
PG_PRE=$( brew --prefix $PG )/bin

for i in {1..$RETRIES}
do
  echo "Create postgres user..."
  # Tests will create and drop "default_table" in this database:
  $PG_PRE/createdb $USER && break || echo 'Try again...'
  sleep 1
done


# MySQL:
MY='mysql@8.4'
brew install $MY
brew services start $MY
MY_PRE=$( brew --prefix $MY )/bin

for i in {1..$RETRIES}
do
  echo "Create mysql user..."
  $MY_PRE/mysql -u root -e "CREATE USER $USER" && break || echo 'Try again...'
  sleep 1
done
# Tests will create and drop "default_table" in this database:
echo "Create database..."
$MY_PRE/mysql -u root -e "CREATE DATABASE $USER"
echo "Grant privs..."
$MY_PRE/mysql -u root -e "GRANT ALL PRIVILEGES ON $USER."'*'" TO '$USER'@'%' WITH GRANT OPTION"
