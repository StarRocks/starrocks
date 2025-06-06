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
     --test TEST_NAME               run specific test
     --gtest_filter GTEST_FILTER    run test cases with gtest filters
     --dry-run                      dry-run unit tests
     --clean                        clean old unit tests before run
     --with-gcov                    enable to build with gcov
     --with-aws                     enable to test aws
     --with-bench                   enable to build with benchmark
     --excluding-test-suit          don't run cases of specific suit
     --module                       module to run uts
     --enable-shared-data           enable to build with shared-data feature support
     --without-starcache            build without starcache library
     --use-staros                   DEPRECATED. an alias of --enable-shared-data option
     --without-debug-symbol-split   split debug symbol out of the test binary to accelerate the speed
                                    of loading binary into memory and start execution.
     -j                             build parallel

  Eg.
    $0                              run all unit tests
    $0 --test CompactionUtilsTest   run compaction test
    $0 --dry-run                    dry-run unit tests
    $0 --clean                      clean old unit tests before run
    $0 --help                       display usage
    $0 --gtest_filter CompactionUtilsTest*:TabletUpdatesTest*   run the two test suites: CompactionUtilsTest and TabletUpdatesTest
  "
  exit 1
}

# Append negative cases to existing $TEST_NAME
# refer to https://github.com/google/googletest/blob/main/docs/advanced.md#running-a-subset-of-the-tests
# for detailed explaination of `--gtest_filter`
append_negative_case() {
    local exclude_case=$1
    case $TEST_NAME in
      *-*)
        # already has negative cases, just append the cases to the end
        TEST_NAME=${TEST_NAME}:$exclude_case
        ;;
      *)
        # doesn't have negative cases, start the negative session
        TEST_NAME=${TEST_NAME}-$exclude_case
        ;;
    esac
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
  -l 'excluding-test-suit:' \
  -l 'use-staros' \
  -l 'enable-shared-data' \
  -l 'without-starcache' \
  -l 'with-brpc-keepalive' \
  -l 'without-debug-symbol-split' \
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
EXCLUDING_TEST_SUIT=
HELP=0
WITH_AWS=OFF
USE_STAROS=OFF
WITH_GCOV=OFF
WITH_STARCACHE=ON
WITH_BRPC_KEEPALIVE=OFF
WITH_DEBUG_SYMBOL_SPLIT=ON
BUILD_JAVA_EXT=ON
while true; do
    case "$1" in
        --clean) CLEAN=1 ; shift ;;
        --dry-run) DRY_RUN=1 ; shift ;;
        --run) shift ;; # Option only for compatibility
        --test) TEST_NAME=${2}* ; shift 2;;
        --gtest_filter) TEST_NAME=$2 ; shift 2;;
        --module) TEST_MODULE=$2; shift 2;;
        --help) HELP=1 ; shift ;;
        --with-aws) WITH_AWS=ON; shift ;;
        --with-gcov) WITH_GCOV=ON; shift ;;
        --without-starcache) WITH_STARCACHE=OFF; shift ;;
        --with-brpc-keepalive) WITH_BRPC_KEEPALIVE=ON; shift ;;
        --excluding-test-suit) EXCLUDING_TEST_SUIT=$2; shift 2;;
        --enable-shared-data|--use-staros) USE_STAROS=ON; shift ;;
        --without-debug-symbol-split) WITH_DEBUG_SYMBOL_SPLIT=OFF; shift ;;
        --without-java-ext) BUILD_JAVA_EXT=OFF; shift ;;
        -j) PARALLEL=$2; shift 2 ;;
        --) shift ;  break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done

if [[ "${BUILD_TYPE}" == "ASAN" && "${WITH_GCOV}" == "ON" ]]; then
    echo "Error: ASAN and gcov cannot be enabled at the same time. Please disable one of them."
    exit 1
fi

if [ ${HELP} -eq 1 ]; then
    usage
    exit 0
fi

CMAKE_BUILD_TYPE=${BUILD_TYPE:-ASAN}
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
if [[ -z ${USE_SSE4_2} ]]; then
    USE_SSE4_2=ON
fi
if [[ -z ${USE_BMI_2} ]]; then
    USE_BMI_2=ON
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

source ${STARROCKS_HOME}/bin/common.sh

cd ${CMAKE_BUILD_DIR}
if [ "${USE_STAROS}" == "ON"  ]; then
  if [ -z "$STARLET_INSTALL_DIR" ] ; then
    # assume starlet_thirdparty is installed to ${STARROCKS_THIRDPARTY}/installed/starlet/
    STARLET_INSTALL_DIR=${STARROCKS_THIRDPARTY}/installed/starlet
  fi
  export STARLET_INSTALL_DIR
fi

# Build Java Extensions
if [ ${BUILD_JAVA_EXT} = "ON" ]; then
    echo "Build Java Extensions"
    cd ${STARROCKS_HOME}/java-extensions
    if [ ${CLEAN} -eq 1 ]; then
        ${MVN_CMD} clean
    fi
    ${MVN_CMD} $addon_mvn_opts package -DskipTests -T ${PARALLEL}
    cd ${STARROCKS_HOME}
else
    echo "Skip Building Java Extensions"
fi

cd ${CMAKE_BUILD_DIR}
${CMAKE_CMD}  -G "${CMAKE_GENERATOR}" \
            -DSTARROCKS_THIRDPARTY=${STARROCKS_THIRDPARTY}\
            -DSTARROCKS_HOME=${STARROCKS_HOME} \
            -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
            -DMAKE_TEST=ON -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
            -DUSE_AVX2=$USE_AVX2 -DUSE_AVX512=$USE_AVX512 -DUSE_SSE4_2=$USE_SSE4_2 -DUSE_BMI_2=$USE_BMI_2\
            -DUSE_STAROS=${USE_STAROS} \
            -DSTARLET_INSTALL_DIR=${STARLET_INSTALL_DIR}          \
            -DWITH_GCOV=${WITH_GCOV} \
            -DWITH_STARCACHE=${WITH_STARCACHE} \
            -DWITH_BRPC_KEEPALIVE=${WITH_BRPC_KEEPALIVE} \
            -DSTARROCKS_JIT_ENABLE=ON \
            -DWITH_RELATIVE_SRC_PATH=OFF \
            -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ../

${BUILD_SYSTEM} -j${PARALLEL}

cd ${STARROCKS_HOME}
export STARROCKS_TEST_BINARY_DIR=${CMAKE_BUILD_DIR}/test
TEST_BIN=starrocks_test
if [ "x$WITH_DEBUG_SYMBOL_SPLIT" = "xON" ] && test -f ${STARROCKS_TEST_BINARY_DIR}/$TEST_BIN ; then
    pushd ${STARROCKS_TEST_BINARY_DIR} >/dev/null 2>&1
    TEST_BIN_SYMBOL=starrocks_test.debuginfo
    echo -n "[INFO] Split $TEST_BIN debug symbol to $TEST_BIN_SYMBOL ..."
    objcopy --only-keep-debug $TEST_BIN $TEST_BIN_SYMBOL
    strip --strip-debug $TEST_BIN
    objcopy --add-gnu-debuglink=$TEST_BIN_SYMBOL $TEST_BIN
    # continue the echo output from the previous `echo -n`
    echo " split done."
    popd >/dev/null 2>&1
fi

echo "*********************************"
echo "  Starting to Run BE Unit Tests  "
echo "*********************************"

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

export LD_LIBRARY_PATH=${STARROCKS_THIRDPARTY}/installed/jemalloc/lib-shared/:$LD_LIBRARY_PATH

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
if [[ -n "$STARROCKS_GCC_HOME" ]] ; then
    # add gcc lib64 into LD_LIBRARY_PATH because of dynamic link libstdc++ and libgcc
    export LD_LIBRARY_PATH=$STARROCKS_GCC_HOME/lib64:$LD_LIBRARY_PATH
fi

THIRDPARTY_HADOOP_HOME=${STARROCKS_THIRDPARTY}/installed/hadoop/share/hadoop
if [[ -d ${THIRDPARTY_HADOOP_HOME} ]] ; then
    export HADOOP_CLASSPATH=${THIRDPARTY_HADOOP_HOME}/common/*:${THIRDPARTY_HADOOP_HOME}/common/lib/*:${THIRDPARTY_HADOOP_HOME}/hdfs/*:${THIRDPARTY_HADOOP_HOME}/hdfs/lib/*
    # get rid of StackOverflowError on the process reaper thread, which has a small stack size.
    # https://bugs.openjdk.org/browse/JDK-8153057
    export LIBHDFS_OPTS="$LIBHDFS_OPTS -Djdk.lang.processReaperUseDefaultStackSize=true"
else
    # exclude HdfsFileSystemTest related test case if no hadoop env found
    echo "[INFO] Can't find available HADOOP common lib, disable HdfsFileSystemTest related test!"
    append_negative_case "HdfsFileSystemTest*"
fi
# HADOOP_CLASSPATH defined in $STARROCKS_HOME/conf/hadoop_env.sh
# put $STARROCKS_HOME/conf ahead of $HADOOP_CLASSPATH so that custom config can replace the config in $HADOOP_CLASSPATH
export CLASSPATH=$STARROCKS_HOME/conf:$HADOOP_CLASSPATH:$CLASSPATH
export CLASSPATH=${STARROCKS_HOME}/java-extensions/udf-extensions/target/*:$CLASSPATH
export CLASSPATH=${STARROCKS_HOME}/java-extensions/java-utils/target/*:$CLASSPATH

# ===========================================================

export ASAN_OPTIONS="abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1:detect_stack_use_after_return=1"

if [ $WITH_AWS = "OFF" ]; then
    append_negative_case "*S3*"
fi

if [ -n "$EXCLUDING_TEST_SUIT" ]; then
    excluding_test_suit=$EXCLUDING_TEST_SUIT
    excluding_test_suit_array=("${excluding_test_suit//|/ }")
    for element in ${excluding_test_suit_array[*]}; do
        append_negative_case "*.${element}_*"
    done
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

echo "[INFO] gtest_filter: $TEST_NAME"
# run cases in starrocks_test in parallel if has gtest-parallel script.
# reference: https://github.com/google/gtest-parallel
if [[ $TEST_MODULE == '.*'  || $TEST_MODULE == 'starrocks_test' ]]; then
  echo "Run test: ${STARROCKS_TEST_BINARY_DIR}/starrocks_test"
  if [ ${DRY_RUN} -eq 0 ]; then
    if [ -x "${GTEST_PARALLEL}" ]; then
        ${GTEST_PARALLEL} ${STARROCKS_TEST_BINARY_DIR}/starrocks_test \
            --gtest_filter=${TEST_NAME} \
            --serialize_test_cases ${GTEST_PARALLEL_OPTIONS}
    else
        ${STARROCKS_TEST_BINARY_DIR}/starrocks_test $GTEST_OPTIONS --gtest_filter=${TEST_NAME}
    fi
  fi
fi

for test_bin in $test_files
do
    echo "Run test: $test_bin"
    if [ ${DRY_RUN} -eq 0 ]; then
        file_name=${test_bin##*/}
        if [ -z $RUN_FILE ] || [ $file_name == $RUN_FILE ]; then
            $test_bin $GTEST_OPTIONS --gtest_filter=${TEST_NAME}
        fi
    fi
done

