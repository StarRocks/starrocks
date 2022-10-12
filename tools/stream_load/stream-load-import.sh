# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#!/usr/bin/env bash

# java
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi
JAVA=$JAVA_HOME/bin/java
JAVAC=$JAVA_HOME/bin/javac

STREAM_LOAD_UTILS_PARAM="$STREAM_LOAD_UTILS_PARAM";

for i in "$*";
do
    STREAM_LOAD_UTILS_PARAM="$STREAM_LOAD_UTILS_PARAM $i"
done

# echo $STREAM_LOAD_UTILS_PARAM

$JAVAC StreamLoadImportUtils.java
$JAVA -Xmx512m -XX:+UseG1GC StreamLoadImportUtils  $STREAM_LOAD_UTILS_PARAM