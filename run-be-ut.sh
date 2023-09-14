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

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export STARROCKS_HOME=${ROOT}

. ${STARROCKS_HOME}/env.sh

PARALLEL=$[$(nproc)/4+1]

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --test  [TEST_NAME]            run specific test
     --dry-run                      dry-run unit tests
     --clean                        clean old unit tests before run
     --with-gcov                    enable to build with gcov
     --with-aws                     enable to test aws
     --with-bench                   enable to build with benchmark
     --module                       module to run uts
     --use-staros                   enable to build with staros
     -j                             build parallel

  Eg.
    $0                              run all unit tests
    $0 --test CompactionUtilsTest   run compaction test
    $0 --dry-run                    dry-run unit tests
    $0 --clean                      clean old unit tests before run
    $0 --help                       display usage
  "
  exit 1
}

# -l run and -l gtest_filter only used for compatibility
OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'test:' \
  -l 'dry-run' \
  -l 'clean' \
  -l 'with-gcov' \
  -l 'module:' \
  -l 'with-aws' \
  -l 'with-bench' \
  -l 'use-staros' \
  -o 'j:' \
  -l 'help' \
  -l 'run' \
  -l 'gtest_filter:' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

CLEAN=0
DRY_RUN=0
TEST_NAME=*
TEST_MODULE=".*"
HELP=0
WITH_AWS=OFF
USE_STAROS=OFF
WITH_GCOV=OFF
while true; do
    case "$1" in
        --clean) CLEAN=1 ; shift ;;
        --dry-run) DRY_RUN=1 ; shift ;;
        --run) shift ;; # Option only for compatibility
        --test) TEST_NAME=$2 ; shift 2;;
        --gtest_filter) TEST_NAME=$2 ; shift 2;; # Option only for compatibility
        --module) TEST_MODULE=$2; shift 2;;
        --help) HELP=1 ; shift ;;
        --with-aws) WITH_AWS=ON; shift ;;
        --with-gcov) WITH_GCOV=ON; shift ;;
        --use-staros) USE_STAROS=ON; shift ;;
        -j) PARALLEL=$2; shift 2 ;;
        --) shift ;  break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done

if [ ${HELP} -eq 1 ]; then
    usage
    exit 0
fi

CMAKE_BUILD_TYPE=${BUILD_TYPE:-ASAN}
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
if [[ -z ${USE_SSE4_2} ]]; then
    USE_SSE4_2=ON
fi
if [[ -z ${USE_AVX2} ]]; then
    USE_AVX2=ON
fi
if [[ -z ${USE_AVX512} ]]; then
    # Disable it by default
    USE_AVX512=OFF
fi
echo "Build Backend UT"

CMAKE_BUILD_DIR=${STARROCKS_HOME}/be/ut_build_${CMAKE_BUILD_TYPE}
if [ ${CLEAN} -eq 1 ]; then
    rm ${CMAKE_BUILD_DIR} -rf
    rm ${STARROCKS_HOME}/be/output/ -rf
fi

if [ ! -d ${CMAKE_BUILD_DIR} ]; then
    mkdir -p ${CMAKE_BUILD_DIR}
fi

# The `WITH_CACHELIB` just controls whether cachelib is compiled in, while starcache is controlled by "USE_STAROS".
# This option will soon be deprecated.
if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
    # force turn off cachelib on arm platform
    WITH_CACHELIB=OFF
elif [[ -z ${WITH_CACHELIB} ]]; then
    WITH_CACHELIB=OFF
fi

source ${STARROCKS_HOME}/bin/common.sh

cd ${CMAKE_BUILD_DIR}
if [ "${USE_STAROS}" == "ON"  ]; then
  if [ -z "$STARLET_INSTALL_DIR" ] ; then
    # assume starlet_thirdparty is installed to ${STARROCKS_THIRDPARTY}/installed/starlet/
    STARLET_INSTALL_DIR=${STARROCKS_THIRDPARTY}/installed/starlet
  fi
  export STARLET_INSTALL_DIR
fi

# Temporarily keep the default behavior same as before to avoid frequent thirdparty update.
# Once the starcache version is stable, we will turn on it by default.
if [[ -z ${WITH_STARCACHE} ]]; then
  WITH_STARCACHE=${USE_STAROS}
fi

${CMAKE_CMD}  -G "${CMAKE_GENERATOR}" \
            -DSTARROCKS_THIRDPARTY=${STARROCKS_THIRDPARTY}\
            -DSTARROCKS_HOME=${STARROCKS_HOME} \
            -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
            -DMAKE_TEST=ON -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
            -DUSE_AVX2=$USE_AVX2 -DUSE_AVX512=$USE_AVX512 -DUSE_SSE4_2=$USE_SSE4_2 \
            -DUSE_STAROS=${USE_STAROS} \
            -DSTARLET_INSTALL_DIR=${STARLET_INSTALL_DIR}          \
            -DWITH_GCOV=${WITH_GCOV} \
            -DWITH_CACHELIB=${WITH_CACHELIB} \
            -DWITH_STARCACHE=${WITH_STARCACHE} \
            -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ../

${BUILD_SYSTEM} -j${PARALLEL}

echo "*********************************"
echo "  Starting to Run BE Unit Tests  "
echo "*********************************"

cd ${STARROCKS_HOME}
export STARROCKS_TEST_BINARY_DIR=${CMAKE_BUILD_DIR}
export TERM=xterm
export UDF_RUNTIME_DIR=${STARROCKS_HOME}/lib/udf-runtime
export LOG_DIR=${STARROCKS_HOME}/log
export LSAN_OPTIONS=suppressions=${STARROCKS_HOME}/conf/asan_suppressions.conf
for i in `sed 's/ //g' $STARROCKS_HOME/conf/be_test.conf | egrep "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*="`; do
    eval "export $i";
done

mkdir -p $LOG_DIR
mkdir -p ${UDF_RUNTIME_DIR}
rm -f ${UDF_RUNTIME_DIR}/*

# ====================== configure JAVA/JVM ====================
# NOTE: JAVA_HOME must be configed if using hdfs scan, like hive external table
# this is only for starting be
jvm_arch="amd64"
if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
    jvm_arch="aarch64"
fi

if [ "$JAVA_HOME" = "" ]; then
    export LD_LIBRARY_PATH=$STARROCKS_HOME/lib/jvm/$jvm_arch/server:$STARROCKS_HOME/lib/jvm/$jvm_arch:$LD_LIBRARY_PATH
else
    java_version=$(jdk_version)
    if [[ $java_version -gt 8 ]]; then
        export LD_LIBRARY_PATH=$JAVA_HOME/lib/server:$JAVA_HOME/lib:$LD_LIBRARY_PATH
    # JAVA_HOME is jdk
    elif [[ -d "$JAVA_HOME/jre"  ]]; then
        export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/$jvm_arch/server:$JAVA_HOME/jre/lib/$jvm_arch:$LD_LIBRARY_PATH
    # JAVA_HOME is jre
    else
        export LD_LIBRARY_PATH=$JAVA_HOME/lib/$jvm_arch/server:$JAVA_HOME/lib/$jvm_arch:$LD_LIBRARY_PATH
    fi
fi

export LD_LIBRARY_PATH=$STARROCKS_HOME/lib/hadoop/native:$LD_LIBRARY_PATH
if [ "${WITH_CACHELIB}" == "ON"  ]; then
    CACHELIB_DIR=${STARROCKS_THIRDPARTY}/installed/cachelib
    export LD_LIBRARY_PATH=$CACHELIB_DIR/lib:$CACHELIB_DIR/lib64:$CACHELIB_DIR/deps/lib:$CACHELIB_DIR/deps/lib64:$LD_LIBRARY_PATH
fi

# HADOOP_CLASSPATH defined in $STARROCKS_HOME/conf/hadoop_env.sh
# put $STARROCKS_HOME/conf ahead of $HADOOP_CLASSPATH so that custom config can replace the config in $HADOOP_CLASSPATH
export CLASSPATH=$STARROCKS_HOME/conf:$HADOOP_CLASSPATH:$CLASSPATH

# ===========================================================

export STARROCKS_TEST_BINARY_DIR=${STARROCKS_TEST_BINARY_DIR}/test

if [ $WITH_AWS = "OFF" ]; then
    TEST_NAME="$TEST_NAME*:-*S3*"
fi

# prepare util test_data
if [ -d ${STARROCKS_TEST_BINARY_DIR}/util/test_data ]; then
    rm -rf ${STARROCKS_TEST_BINARY_DIR}/util/test_data
fi
cp -r ${STARROCKS_HOME}/be/test/util/test_data ${STARROCKS_TEST_BINARY_DIR}/util/

test_files=`find ${STARROCKS_TEST_BINARY_DIR} -type f -perm -111 -name "*test" \
    | grep -v starrocks_test \
    | grep -v bench_test \
    | grep -e "$TEST_MODULE" `

# run cases in starrocks_test in parallel if has gtest-parallel script.
# reference: https://github.com/google/gtest-parallel
if [[ $TEST_MODULE == '.*'  || $TEST_MODULE == 'starrocks_test' ]]; then
  echo "Run test: ${STARROCKS_TEST_BINARY_DIR}/starrocks_test"
  if [ ${DRY_RUN} -eq 0 ]; then
    if [ -x ${GTEST_PARALLEL} ]; then
        ${GTEST_PARALLEL} ${STARROCKS_TEST_BINARY_DIR}/starrocks_test \
            --gtest_filter=${TEST_NAME} \
            --serialize_test_cases ${GTEST_PARALLEL_OPTIONS}
    else
        ${STARROCKS_TEST_BINARY_DIR}/starrocks_test $GTEST_OPTIONS --gtest_filter=${TEST_NAME}
    fi
  fi
fi

for test in $test_files
do
    echo "Run test: $test"
    if [ ${DRY_RUN} -eq 0 ]; then
        file_name=${test##*/}
        if [ -z $RUN_FILE ] || [ $file_name == $RUN_FILE ]; then
            $test $GTEST_OPTIONS --gtest_filter=${TEST_NAME}
        fi
    fi
done
