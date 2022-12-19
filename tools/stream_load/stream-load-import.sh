# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
