#!/bin/bash

set -euo pipefail

# TODO?
# brew install openjdk@25
#
# Spark 4.2.0 (latest) should be happy with Java 25:
# https://spark.apache.org/docs/4.2.0
# > Spark runs on Java 17/21/25, Scala 2.13, Python 3.10+, and R 4.0+ (Deprecated).
# > Java 25 prior to version 25.0.3 support is deprecated as of Spark 4.2.0.
#
# "brew install openjdk" installs v26, which is ahead of the version required by Spark.

# $ export JAVA_HOME='/opt/homebrew/opt/openjdk@25'
# $ pyspark --master "local[2]" --verbose
# Python 3.10.19 (main, Oct  9 2025, 15:25:03) [Clang 16.0.0 (clang-1600.0.26.6)] on darwin
# Type "help", "copyright", "credits" or "license" for more information.
# WARNING: Using incubator modules: jdk.incubator.vector
# WARNING: package sun.security.action not in java.base
# Parsed arguments:
#   master                  local[2]
#   remote                  null
#   deployMode              null
#   executorMemory          null
#   executorCores           null
#   totalExecutorCores      null
#   propertiesFile          null
#   driverMemory            null
#   driverCores             null
#   driverExtraClassPath    null
#   driverExtraLibraryPath  null
#   driverExtraJavaOptions  null
#   supervise               false
#   queue                   null
#   numExecutors            null
#   files                   null
#   pyFiles                 null
#   archives                null
#   mainClass               null
#   primaryResource         pyspark-shell
#   name                    PySparkShell
#   childArgs               []
#   jars                    null
#   packages                null
#   packagesExclusions      null
#   repositories            null
#   verbose                 true

# Spark properties used, including those specified through
#  --conf and those from the properties file null:



# Main class:
# org.apache.spark.api.python.PythonGatewayServer
# Arguments:

# Spark config:
# (spark.app.name,PySparkShell)
# (spark.app.submitTime,1786999399533)
# (spark.master,local[2])
# (spark.submit.deployMode,client)
# (spark.submit.pyFiles,)
# (spark.ui.showConsoleProgress,true)
# Classpath elements:



# Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
# Setting default log level to "WARN".
# To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
# WARNING: A restricted method in java.lang.System has been called
# WARNING: java.lang.System::loadLibrary has been called by org.apache.hadoop.util.NativeCodeLoader in an unnamed module (file:/Users/chuckmccallum/github/opendp/polars-to-ibis/.venv/lib/python3.10/site-packages/pyspark/jars/hadoop-client-api-3.4.1.jar)
# WARNING: Use --enable-native-access=ALL-UNNAMED to avoid a warning for callers in this module
# WARNING: Restricted methods will be blocked in a future release unless native access is enabled

# 26/08/17 16:43:20 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
# 26/08/17 16:43:20 WARN SparkContext: Another SparkContext is being constructed (or threw an exception in its constructor). This may indicate an error, since only one SparkContext should be running in this JVM (see SPARK-2243). The other SparkContext was created at:
# org.apache.spark.api.java.JavaSparkContext.<init>(JavaSparkContext.scala:59)
# java.base/jdk.internal.reflect.DirectConstructorHandleAccessor.newInstance(DirectConstructorHandleAccessor.java:62)
# java.base/java.lang.reflect.Constructor.newInstanceWithCaller(Constructor.java:499)
# java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:483)
# py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:247)
# py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
# py4j.Gateway.invoke(Gateway.java:238)
# py4j.commands.ConstructorCommand.invokeConstructor(ConstructorCommand.java:80)
# py4j.commands.ConstructorCommand.execute(ConstructorCommand.java:69)
# py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:184)
# py4j.ClientServerConnection.run(ClientServerConnection.java:108)
# java.base/java.lang.Thread.run(Thread.java:1474)
# /Users/chuckmccallum/github/opendp/polars-to-ibis/.venv/lib/python3.10/site-packages/pyspark/python/pyspark/shell.py:94: UserWarning: Failed to initialize Spark session.
#   warnings.warn("Failed to initialize Spark session.")
# Traceback (most recent call last):
#   File "/Users/chuckmccallum/github/opendp/polars-to-ibis/.venv/lib/python3.10/site-packages/pyspark/python/pyspark/shell.py", line 89, in <module>
#     spark = SparkSession._create_shell_session()
#   File "/Users/chuckmccallum/github/opendp/polars-to-ibis/.venv/lib/python3.10/site-packages/pyspark/sql/session.py", line 1249, in _create_shell_session
#     return SparkSession._getActiveSessionOrCreate()
#   File "/Users/chuckmccallum/github/opendp/polars-to-ibis/.venv/lib/python3.10/site-packages/pyspark/sql/session.py", line 1265, in _getActiveSessionOrCreate
#     spark = builder.getOrCreate()
#   File "/Users/chuckmccallum/github/opendp/polars-to-ibis/.venv/lib/python3.10/site-packages/pyspark/sql/session.py", line 556, in getOrCreate
#     sc = SparkContext.getOrCreate(sparkConf)
#   File "/Users/chuckmccallum/github/opendp/polars-to-ibis/.venv/lib/python3.10/site-packages/pyspark/core/context.py", line 523, in getOrCreate
#     SparkContext(conf=conf or SparkConf())
#   File "/Users/chuckmccallum/github/opendp/polars-to-ibis/.venv/lib/python3.10/site-packages/pyspark/core/context.py", line 207, in __init__
#     self._do_init(
#   File "/Users/chuckmccallum/github/opendp/polars-to-ibis/.venv/lib/python3.10/site-packages/pyspark/core/context.py", line 300, in _do_init
#     self._jsc = jsc or self._initialize_context(self._conf._jconf)
#   File "/Users/chuckmccallum/github/opendp/polars-to-ibis/.venv/lib/python3.10/site-packages/pyspark/core/context.py", line 429, in _initialize_context
#     return self._jvm.JavaSparkContext(jconf)
#   File "/Users/chuckmccallum/github/opendp/polars-to-ibis/.venv/lib/python3.10/site-packages/pyspark/python/lib/py4j-0.10.9.9-src.zip/py4j/java_gateway.py", line 1627, in __call__
#     return_value = get_return_value(
#   File "/Users/chuckmccallum/github/opendp/polars-to-ibis/.venv/lib/python3.10/site-packages/pyspark/python/lib/py4j-0.10.9.9-src.zip/py4j/protocol.py", line 327, in get_return_value
#     raise Py4JJavaError(
# py4j.protocol.Py4JJavaError: An error occurred while calling None.org.apache.spark.api.java.JavaSparkContext.
# : java.lang.UnsupportedOperationException: getSubject is not supported
# 	at java.base/javax.security.auth.Subject.getSubject(Subject.java:277)
# 	at org.apache.hadoop.security.UserGroupInformation.getCurrentUser(UserGroupInformation.java:588)
# 	at org.apache.spark.util.Utils$.$anonfun$getCurrentUserName$1(Utils.scala:2446)
# 	at scala.Option.getOrElse(Option.scala:201)
# 	at org.apache.spark.util.Utils$.getCurrentUserName(Utils.scala:2446)
# 	at org.apache.spark.SparkContext.<init>(SparkContext.scala:339)
# 	at org.apache.spark.api.java.JavaSparkContext.<init>(JavaSparkContext.scala:59)
# 	at java.base/jdk.internal.reflect.DirectConstructorHandleAccessor.newInstance(DirectConstructorHandleAccessor.java:62)
# 	at java.base/java.lang.reflect.Constructor.newInstanceWithCaller(Constructor.java:499)
# 	at java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:483)
# 	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:247)
# 	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
# 	at py4j.Gateway.invoke(Gateway.java:238)
# 	at py4j.commands.ConstructorCommand.invokeConstructor(ConstructorCommand.java:80)
# 	at py4j.commands.ConstructorCommand.execute(ConstructorCommand.java:69)
# 	at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:184)
# 	at py4j.ClientServerConnection.run(ClientServerConnection.java:108)
# 	at java.base/java.lang.Thread.run(Thread.java:1474)


# PostgreSQL:
PG='postgresql@16'
brew install $PG
brew services stop $PG || echo "$PG not already running? Continue..."
brew services restart $PG
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
brew services restart $MY
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
