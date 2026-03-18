#!/bin/bash

set -euo pipefail

RETRIES=20

# PostgreSQL:
PG='postgresql@16'
brew install $PG
brew services start $PG
PG_PRE=$( brew --prefix $PG )/bin

for I in {1..$RETRIES}
do
  echo "$I: Create postgres user..."
  # Tests will create and drop "default_table" in this database:
  $PG_PRE/createdb $USER && break || echo 'Try again...'
  sleep 1
done


# MySQL:
MY='mysql@8.4'
brew install $MY
brew services start $MY
MY_PRE=$( brew --prefix $MY )/bin

for I in {1..$RETRIES}
do
  CMD="CREATE USER $USER"
  echo "$I: Create mysql user: $CMD"
  $MY_PRE/mysql -u root -e "$CMD" && break || echo 'Try again...'
  sleep 1
done
# Tests will create and drop "default_table" in this database:
CMD="CREATE DATABASE $USER"
echo "Create database: $CMD"
$MY_PRE/mysql -u root -e "$CMD"

CMD="GRANT ALL PRIVILEGES ON $USER."'*'" TO '$USER'@'%' WITH GRANT OPTION"
echo "Grant privs: $CMD"
$MY_PRE/mysql -u root -e "$CMD"
