#!/bin/bash

set -euo pipefail

WAIT=20

# PostgreSQL:
brew install postgresql@16
brew services start postgresql@16
[[ -n $CI ]] && echo 'export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"' >> /Users/runner/.bash_profile

for i in {1..10}
do
  # Tests will create and drop "default_table" in this database:
  createdb $USER && break || echo 'Try again...'
  sleep 1
done


# MySQL:
brew install mysql@8.4
brew services start mysql@8.4
[[ -n $CI ]] && echo 'export PATH="/opt/homebrew/opt/mysql@8.4/bin:$PATH"' >> /Users/runner/.bash_profile

for i in {1..10}
do
  mysql -u root -e "CREATE USER $USER" && break || echo 'Try again...'
  sleep 1
done
# Tests will create and drop "default_table" in this database:
mysql -u root -e "CREATE DATABASE $USER"
mysql -u root -e "GRANT ALL ON $USER.* TO '$USER'@'%'"
