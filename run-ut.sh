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
     --clean                        clean and build ut
     --run                          build and run ut
     --gtest_filter                 specify test cases
     --with-aws                     enable to test aws
     --with-bench                   enable to build with benchmark
     --with-gcov                    enable to build with gcov
     --use-staros                   enable to build with staros
     -j                             build parallel

  Eg.
    $0                              build ut
    $0 --run                        build and run ut
    $0 --run --gtest_filter scan*   build and run ut of specified cases
    $0 --clean                      clean and build ut
    $0 --clean --run                clean, build and run ut
    $0 --clean --run --with-gcov    clean, build and run ut with gcov
    $0 --help                       display usage
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'run' \
  -l 'clean' \
  -l "gtest_filter:" \
  -l 'with-aws' \
  -l 'with-bench' \
  -l 'use-staros' \
  -l 'with-gcov' \
  -o 'j:' \
  -l 'help' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

CLEAN=0
RUN=0
TEST_FILTER=*
HELP=0
WITH_AWS=OFF
USE_STAROS=OFF
WITH_BLOCK_CACHE=OFF
WITH_GCOV=OFF
while true; do
    case "$1" in
        --clean) CLEAN=1 ; shift ;;
        --run) RUN=1 ; shift ;;
        --gtest_filter) TEST_FILTER=$2 ; shift 2;; 
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
echo "Build Backend UT"

CMAKE_BUILD_DIR=${STARROCKS_HOME}/be/ut_build_${CMAKE_BUILD_TYPE}
if [ ${CLEAN} -eq 1 ]; then
    rm ${CMAKE_BUILD_DIR} -rf
    rm ${STARROCKS_HOME}/be/output/ -rf
fi

if [ ! -d ${CMAKE_BUILD_DIR} ]; then
    mkdir -p ${CMAKE_BUILD_DIR}
fi

cd ${CMAKE_BUILD_DIR}

if [ "${USE_STAROS}" == "ON"  ]; then
  if [ -z "$STARLET_INSTALL_DIR" ] ; then
    # assume starlet_thirdparty is installed to ${STARROCKS_THIRDPARTY}/installed/starlet/
    STARLET_INSTALL_DIR=${STARROCKS_THIRDPARTY}/installed/starlet
  fi
  ${CMAKE_CMD}  -G "${CMAKE_GENERATOR}" \
              -DSTARROCKS_THIRDPARTY=${STARROCKS_THIRDPARTY}\
              -DSTARROCKS_HOME=${STARROCKS_HOME} \
              -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
              -DMAKE_TEST=ON -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
              -DUSE_AVX2=$USE_AVX2 -DUSE_SSE4_2=$USE_SSE4_2 \
              -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
              -DUSE_STAROS=${USE_STAROS} -DWITH_GCOV=${WITH_GCOV} \
              -DWITH_BLOCK_CACHE=${WITH_BLOCK_CACHE} \
              -Dprotobuf_DIR=${STARLET_INSTALL_DIR}/third_party/lib/cmake/protobuf \
              -Dabsl_DIR=${STARLET_INSTALL_DIR}/third_party/lib/cmake/absl \
              -DgRPC_DIR=${STARLET_INSTALL_DIR}/third_party/lib/cmake/grpc \
              -Dprometheus-cpp_DIR=${STARLET_INSTALL_DIR}/third_party/lib/cmake/prometheus-cpp \
              -Dstarlet_DIR=${STARLET_INSTALL_DIR}/starlet_install/lib64/cmake ..
else
  ${CMAKE_CMD}  -G "${CMAKE_GENERATOR}" \
              -DSTARROCKS_THIRDPARTY=${STARROCKS_THIRDPARTY}\
              -DSTARROCKS_HOME=${STARROCKS_HOME} \
              -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
              -DMAKE_TEST=ON -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
              -DUSE_AVX2=$USE_AVX2 -DUSE_SSE4_2=$USE_SSE4_2 \
              -DWITH_GCOV=${WITH_GCOV} \
              -DWITH_BLOCK_CACHE=${WITH_BLOCK_CACHE} \
              -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ../
fi
time ${BUILD_SYSTEM} -j${PARALLEL}

if [ ${RUN} -ne 1 ]; then
    echo "Finished"
    exit 0
fi

echo "******************************"
echo "    Running StarRocks BE Unittest    "
echo "******************************"

cd ${STARROCKS_HOME}
export STARROCKS_TEST_BINARY_DIR=${CMAKE_BUILD_DIR}
export TERM=xterm
export UDF_RUNTIME_DIR=${STARROCKS_HOME}/lib/udf-runtime
export LOG_DIR=${STARROCKS_HOME}/log
for i in `sed 's/ //g' $STARROCKS_HOME/conf/be.conf | egrep "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*="`; do
    eval "export $i";
done

mkdir -p $LOG_DIR
mkdir -p ${UDF_RUNTIME_DIR}
rm -f ${UDF_RUNTIME_DIR}/*

if [ ${RUN} -ne 1 ]; then
    echo "Finished"
    exit 0
fi

echo "******************************"
echo "    Running StarRocks BE Unittest    "
echo "******************************"

. ${STARROCKS_HOME}/bin/common.sh

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
if [ "${WITH_BLOCK_CACHE}" == "ON"  ]; then
    CACHELIB_DIR=${STARROCKS_THIRDPARTY}/installed/cachelib
    export LD_LIBRARY_PATH=$CACHELIB_DIR/lib:$CACHELIB_DIR/lib64:$CACHELIB_DIR/deps/lib:$CACHELIB_DIR/deps/lib64:$LD_LIBRARY_PATH
fi

# HADOOP_CLASSPATH defined in $STARROCKS_HOME/conf/hadoop_env.sh
# put $STARROCKS_HOME/conf ahead of $HADOOP_CLASSPATH so that custom config can replace the config in $HADOOP_CLASSPATH
export CLASSPATH=$STARROCKS_HOME/conf:$HADOOP_CLASSPATH:$CLASSPATH

# ===========================================================

export STARROCKS_TEST_BINARY_DIR=${STARROCKS_TEST_BINARY_DIR}/test/

if [ $WITH_AWS = "OFF" ]; then
    TEST_FILTER="$TEST_FILTER:-*S3*"
fi

# prepare util test_data
if [ -d ${STARROCKS_TEST_BINARY_DIR}/util/test_data ]; then
    rm -rf ${STARROCKS_TEST_BINARY_DIR}/util/test_data
fi
cp -r ${STARROCKS_HOME}/be/test/util/test_data ${STARROCKS_TEST_BINARY_DIR}/util/

test_files=`find ${STARROCKS_TEST_BINARY_DIR} -type f -perm -111 -name "*test" | grep -v starrocks_test | grep -v bench_test`

# run cases in starrocks_test in parallel if has gtest-parallel script.
# reference: https://github.com/google/gtest-parallel
if [ -x ${GTEST_PARALLEL} ]; then
    ${GTEST_PARALLEL} ${STARROCKS_TEST_BINARY_DIR}/starrocks_test --gtest_filter=${TEST_FILTER} --serialize_test_cases ${GTEST_PARALLEL_OPTIONS}
else
    ${STARROCKS_TEST_BINARY_DIR}/starrocks_test --gtest_filter=${TEST_FILTER}
fi

for test in ${test_files[@]}
do
    file_name=${test##*/}
    if [ -z $RUN_FILE ] || [ $file_name == $RUN_FILE ]; then
        echo "=== Run $file_name ==="
        $test --gtest_filter=${TEST_FILTER}
    fi
done
