#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -ex

BASE_DIR=`dirname "$0"`
BASE_DIR=`cd "$BASE_DIR"; pwd`

STARROCKS_HOME=${STARROCKS_HOME:-$BASE_DIR/..}

if [ -z "${STARROCKS_THIRDPARTY}" ]; then
  echo "ERR: Unknown STARROCKS_THIRDPARTY env."
  exit 1
fi

BUILD_TYPE=RELEASE

# build format-sdk
cd $STARROCKS_HOME/format-sdk

cmake -S src/main/cpp \
      -B target/build-jni/${BUILD_TYPE} \
      -DTARGET_NAME=starrocks_format_wrapper \
      -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
      -DWITH_STARCACHE=ON

cmake --build target/build-jni/${BUILD_TYPE} --config ${BUILD_TYPE}

mkdir -p target/classes/native && cp target/build-jni/${BUILD_TYPE}/*.so target/classes/native