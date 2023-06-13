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

##############################################################
# This script is used to compile StarRocks
# Usage: 
#    sh build.sh --help
# Eg:
#    sh build.sh                                      build all
#    sh build.sh  --be                                build Backend without clean
#    sh build.sh  --fe --clean                        clean and build Frontend and Spark Dpp application
#    sh build.sh  --fe --be --clean                   clean and build Frontend, Spark Dpp application and Backend
#    sh build.sh  --spark-dpp                         build Spark DPP application alone
#    BUILD_TYPE=build_type ./build.sh --be            build Backend is different mode (build_type could be Release, Debug, or Asan. Default value is Release. To build Backend in Debug mode, you can execute: BUILD_TYPE=Debug ./build.sh --be)
#
# You need to make sure all thirdparty libraries have been
# compiled and installed correctly.
##############################################################

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`
MACHINE_TYPE=$(uname -m)

export STARROCKS_HOME=${ROOT}

. ${STARROCKS_HOME}/env.sh

if [[ ! -f ${STARROCKS_THIRDPARTY}/installed/include/fast_float/fast_float.h ]]; then
    echo "Thirdparty libraries need to be build ..."
    ${STARROCKS_THIRDPARTY}/build-thirdparty.sh
fi

PARALLEL=$[$(nproc)/4+1]

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     --be               build Backend
     --fe               build Frontend and Spark Dpp application
     --spark-dpp        build Spark DPP application
     --clean            clean and build target
     --use-staros       build Backend with staros
     --with-gcov        build Backend with gcov, has an impact on performance
     --without-gcov     build Backend without gcov(default)
     --with-bench       build Backend with bench(default without bench)
     -j                 build Backend parallel

  Eg.
    $0                                           build all
    $0 --be                                      build Backend without clean
    $0 --fe --clean                              clean and build Frontend and Spark Dpp application
    $0 --fe --be --clean                         clean and build Frontend, Spark Dpp application and Backend
    $0 --spark-dpp                               build Spark DPP application alone
    BUILD_TYPE=build_type ./build.sh --be        build Backend is different mode (build_type could be Release, Debug, or Asan. Default value is Release. To build Backend in Debug mode, you can execute: BUILD_TYPE=Debug ./build.sh --be)
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -o 'h' \
  -l 'be' \
  -l 'fe' \
  -l 'spark-dpp' \
  -l 'clean' \
  -l 'with-gcov' \
  -l 'with-bench' \
  -l 'without-gcov' \
  -l 'use-staros' \
  -o 'j:' \
  -l 'help' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

BUILD_BE=
BUILD_FE=
BUILD_SPARK_DPP=
CLEAN=
RUN_UT=
WITH_GCOV=OFF
WITH_BENCH=OFF
USE_STAROS=OFF
if [[ -z ${USE_AVX2} ]]; then
    USE_AVX2=ON
fi
if [[ -z ${USE_SSE4_2} ]]; then
    USE_SSE4_2=ON
fi
# detect cpuinfo
if [[ -z $(grep -o 'avx[^ ]*' /proc/cpuinfo) ]]; then
    USE_AVX2=OFF
fi

if [[ -z $(grep -o 'sse[^ ]*' /proc/cpuinfo) ]]; then
    USE_SSE4_2=OFF
fi

if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
    # force turn off block cache on arm platform
    WITH_BLOCK_CACHE=OFF
elif [[ -z ${WITH_BLOCK_CACHE} ]]; then
    WITH_BLOCK_CACHE=ON
fi

if [[ "${WITH_BLOCK_CACHE}" == "ON" && ! -f ${STARROCKS_THIRDPARTY}/installed/cachelib/lib/libcachelib_allocator.a ]]; then
    echo "WITH_BLOCK_CACHE=ON but missing depdency libraries(cachelib)"
    exit 1
fi

if [[ -z ${ENABLE_QUERY_DEBUG_TRACE} ]]; then
	ENABLE_QUERY_DEBUG_TRACE=OFF
fi

if [[ -z ${USE_JEMALLOC} ]]; then
    USE_JEMALLOC=ON
fi

HELP=0
if [ $# == 1 ] ; then
    # default
    BUILD_BE=1
    BUILD_FE=1
    BUILD_SPARK_DPP=1
    CLEAN=0
    RUN_UT=0
elif [[ $OPTS =~ "-j" ]] && [ $# == 3 ]; then
    # default
    BUILD_BE=1
    BUILD_FE=1
    BUILD_SPARK_DPP=1
    CLEAN=0
    RUN_UT=0
    PARALLEL=$2
else
    BUILD_BE=0
    BUILD_FE=0
    BUILD_SPARK_DPP=0
    CLEAN=0
    RUN_UT=0
    while true; do
        case "$1" in
            --be) BUILD_BE=1 ; shift ;;
            --fe) BUILD_FE=1 ; shift ;;
            --spark-dpp) BUILD_SPARK_DPP=1 ; shift ;;
            --clean) CLEAN=1 ; shift ;;
            --ut) RUN_UT=1   ; shift ;;
            --with-gcov) WITH_GCOV=ON; shift ;;
            --without-gcov) WITH_GCOV=OFF; shift ;;
            --use-staros) USE_STAROS=ON; shift ;;
            --with-bench) WITH_BENCH=ON; shift ;;
            -h) HELP=1; shift ;;
            --help) HELP=1; shift ;;
            -j) PARALLEL=$2; shift 2 ;;
            --) shift ;  break ;;
            *) echo "Internal error" ; exit 1 ;;
        esac
    done
fi

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
fi

if [ ${CLEAN} -eq 1 -a ${BUILD_BE} -eq 0 -a ${BUILD_FE} -eq 0 -a ${BUILD_SPARK_DPP} -eq 0 ]; then
    echo "--clean can not be specified without --fe or --be or --spark-dpp"
    exit 1
fi

echo "Get params:
    BUILD_BE            -- $BUILD_BE
    BE_CMAKE_TYPE       -- $BUILD_TYPE
    BUILD_FE            -- $BUILD_FE
    BUILD_SPARK_DPP     -- $BUILD_SPARK_DPP
    CLEAN               -- $CLEAN
    RUN_UT              -- $RUN_UT
    WITH_GCOV           -- $WITH_GCOV
    WITH_BENCH          -- $WITH_BENCH
    USE_STAROS          -- $USE_STAROS
    USE_AVX2            -- $USE_AVX2
    PARALLEL            -- $PARALLEL
    ENABLE_QUERY_DEBUG_TRACE -- $ENABLE_QUERY_DEBUG_TRACE
    WITH_BLOCK_CACHE    -- $WITH_BLOCK_CACHE
    USE_JEMALLOC        -- $USE_JEMALLOC
"

# Clean and build generated code
echo "Build generated code"
cd ${STARROCKS_HOME}/gensrc
if [ ${CLEAN} -eq 1 ]; then
   make clean
   rm -rf ${STARROCKS_HOME}/fe/fe-core/target
fi
# DO NOT using parallel make(-j) for gensrc
make
cd ${STARROCKS_HOME}


if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
    export LIBRARY_PATH=${JAVA_HOME}/jre/lib/aarch64/server/
else
    export LIBRARY_PATH=${JAVA_HOME}/jre/lib/amd64/server/
fi

# Clean and build Backend
if [ ${BUILD_BE} -eq 1 ] ; then
    CMAKE_BUILD_TYPE=${BUILD_TYPE:-Release}
    echo "Build Backend: ${CMAKE_BUILD_TYPE}"
    CMAKE_BUILD_DIR=${STARROCKS_HOME}/be/build_${CMAKE_BUILD_TYPE}
    if [ "${WITH_GCOV}" = "ON" ]; then
        CMAKE_BUILD_DIR=${STARROCKS_HOME}/be/build_${CMAKE_BUILD_TYPE}_gcov
    fi
    if [ ${CLEAN} -eq 1 ]; then
        rm -rf $CMAKE_BUILD_DIR
        rm -rf ${STARROCKS_HOME}/be/output/
    fi
    mkdir -p ${CMAKE_BUILD_DIR}
    cd ${CMAKE_BUILD_DIR}
    if [ "${USE_STAROS}" == "ON"  ]; then
      if [ -z "$STARLET_INSTALL_DIR" ] ; then
        # assume starlet_thirdparty is installed to ${STARROCKS_THIRDPARTY}/installed/starlet/
        STARLET_INSTALL_DIR=${STARROCKS_THIRDPARTY}/installed/starlet
      fi
      ${CMAKE_CMD} -G "${CMAKE_GENERATOR}" \
                    -DSTARROCKS_THIRDPARTY=${STARROCKS_THIRDPARTY} \
                    -DSTARROCKS_HOME=${STARROCKS_HOME} \
                    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
                    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
                    -DMAKE_TEST=OFF -DWITH_GCOV=${WITH_GCOV}\
                    -DUSE_AVX2=$USE_AVX2 -DUSE_SSE4_2=$USE_SSE4_2 \
                    -DENABLE_QUERY_DEBUG_TRACE=$ENABLE_QUERY_DEBUG_TRACE \
                    -DUSE_JEMALLOC=$USE_JEMALLOC \
                    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
                    -DUSE_STAROS=${USE_STAROS} \
                    -DWITH_BENCH=${WITH_BENCH} \
                    -DWITH_BLOCK_CACHE=${WITH_BLOCK_CACHE} \
                    -Dprotobuf_DIR=${STARLET_INSTALL_DIR}/third_party/lib/cmake/protobuf \
                    -Dabsl_DIR=${STARLET_INSTALL_DIR}/third_party/lib/cmake/absl \
                    -DgRPC_DIR=${STARLET_INSTALL_DIR}/third_party/lib/cmake/grpc \
                    -Dprometheus-cpp_DIR=${STARLET_INSTALL_DIR}/third_party/lib/cmake/prometheus-cpp \
                    -Dstarlet_DIR=${STARLET_INSTALL_DIR}/starlet_install/lib64/cmake ..
    else
      ${CMAKE_CMD} -G "${CMAKE_GENERATOR}" \
                    -DSTARROCKS_THIRDPARTY=${STARROCKS_THIRDPARTY} \
                    -DSTARROCKS_HOME=${STARROCKS_HOME} \
                    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
                    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
                    -DMAKE_TEST=OFF -DWITH_GCOV=${WITH_GCOV}\
                    -DUSE_AVX2=$USE_AVX2 -DUSE_SSE4_2=$USE_SSE4_2 \
                    -DENABLE_QUERY_DEBUG_TRACE=$ENABLE_QUERY_DEBUG_TRACE \
                    -DUSE_JEMALLOC=$USE_JEMALLOC \
                    -DWITH_BENCH=${WITH_BENCH} \
                    -DWITH_BLOCK_CACHE=${WITH_BLOCK_CACHE} \
                    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON  ..
    fi
    time ${BUILD_SYSTEM} -j${PARALLEL}
    ${BUILD_SYSTEM} install

    # Build JDBC Bridge
    echo "Build Java Extensions"
    cd ${STARROCKS_HOME}/java-extensions
    if [ ${CLEAN} -eq 1 ]; then
        ${MVN_CMD} clean
    fi
    ${MVN_CMD} package -DskipTests
    cd ${STARROCKS_HOME}
fi

cd ${STARROCKS_HOME}

# Assesmble FE modules
FE_MODULES=
if [ ${BUILD_FE} -eq 1 -o ${BUILD_SPARK_DPP} -eq 1 ]; then
    if [ ${BUILD_SPARK_DPP} -eq 1 ]; then
        FE_MODULES="fe-common,spark-dpp"
    fi
    if [ ${BUILD_FE} -eq 1 ]; then
        FE_MODULES="fe-common,spark-dpp,fe-core"
    fi
fi

# Clean and build Frontend
if [ ${FE_MODULES}x != ""x ]; then
    echo "Build Frontend Modules: $FE_MODULES"
    cd ${STARROCKS_HOME}/fe
    if [ ${CLEAN} -eq 1 ]; then
        ${MVN_CMD} clean
    fi
    ${MVN_CMD} package -pl ${FE_MODULES} -DskipTests
    cd ${STARROCKS_HOME}
fi


# Clean and prepare output dir
STARROCKS_OUTPUT=${STARROCKS_HOME}/output/
mkdir -p ${STARROCKS_OUTPUT}

# Copy Frontend and Backend
if [ ${BUILD_FE} -eq 1 -o ${BUILD_SPARK_DPP} -eq 1 ]; then
    if [ ${BUILD_FE} -eq 1 ]; then
        install -d ${STARROCKS_OUTPUT}/fe/bin ${STARROCKS_OUTPUT}/fe/conf/ \
                   ${STARROCKS_OUTPUT}/fe/webroot/ ${STARROCKS_OUTPUT}/fe/lib/ \
                   ${STARROCKS_OUTPUT}/fe/spark-dpp/

        cp -r -p ${STARROCKS_HOME}/bin/*_fe.sh ${STARROCKS_OUTPUT}/fe/bin/
        cp -r -p ${STARROCKS_HOME}/bin/show_fe_version.sh ${STARROCKS_OUTPUT}/fe/bin/
        cp -r -p ${STARROCKS_HOME}/bin/common.sh ${STARROCKS_OUTPUT}/fe/bin/
        cp -r -p ${STARROCKS_HOME}/conf/fe.conf ${STARROCKS_OUTPUT}/fe/conf/
        cp -r -p ${STARROCKS_HOME}/conf/hadoop_env.sh ${STARROCKS_OUTPUT}/fe/conf/
        rm -rf ${STARROCKS_OUTPUT}/fe/lib/*
        cp -r -p ${STARROCKS_HOME}/fe/fe-core/target/lib/* ${STARROCKS_OUTPUT}/fe/lib/
        cp -r -p ${STARROCKS_HOME}/fe/fe-core/target/starrocks-fe.jar ${STARROCKS_OUTPUT}/fe/lib/
        cp -r -p ${STARROCKS_HOME}/webroot/* ${STARROCKS_OUTPUT}/fe/webroot/
        cp -r -p ${STARROCKS_HOME}/fe/spark-dpp/target/spark-dpp-*-jar-with-dependencies.jar ${STARROCKS_OUTPUT}/fe/spark-dpp/
        cp -r -p ${STARROCKS_THIRDPARTY}/installed/jindosdk/* ${STARROCKS_OUTPUT}/fe/lib/
        cp -r -p ${STARROCKS_THIRDPARTY}/installed/broker_thirdparty_jars/* ${STARROCKS_OUTPUT}/fe/lib/
        cp -r -p ${STARROCKS_THIRDPARTY}/installed/async-profiler/* ${STARROCKS_OUTPUT}/fe/bin/

    elif [ ${BUILD_SPARK_DPP} -eq 1 ]; then
        install -d ${STARROCKS_OUTPUT}/fe/spark-dpp/
        rm -rf ${STARROCKS_OUTPUT}/fe/spark-dpp/*
        cp -r -p ${STARROCKS_HOME}/fe/spark-dpp/target/spark-dpp-*-jar-with-dependencies.jar ${STARROCKS_OUTPUT}/fe/spark-dpp/
    fi
fi

if [ ${BUILD_BE} -eq 1 ]; then
    rm -rf ${STARROCKS_OUTPUT}/be/lib/*
    mkdir -p ${STARROCKS_OUTPUT}/be/lib/jni-packages

    install -d ${STARROCKS_OUTPUT}/be/bin  \
               ${STARROCKS_OUTPUT}/be/conf \
               ${STARROCKS_OUTPUT}/be/lib/hadoop \
               ${STARROCKS_OUTPUT}/be/lib/jvm \
               ${STARROCKS_OUTPUT}/be/www  \

    cp -r -p ${STARROCKS_HOME}/be/output/bin/* ${STARROCKS_OUTPUT}/be/bin/
    cp -r -p ${STARROCKS_HOME}/be/output/conf/be.conf ${STARROCKS_OUTPUT}/be/conf/
    cp -r -p ${STARROCKS_HOME}/be/output/conf/cn.conf ${STARROCKS_OUTPUT}/be/conf/
    cp -r -p ${STARROCKS_HOME}/be/output/conf/hadoop_env.sh ${STARROCKS_OUTPUT}/be/conf/
    cp -r -p ${STARROCKS_HOME}/be/output/conf/log4j.properties ${STARROCKS_OUTPUT}/be/conf/
    if [ "${BUILD_TYPE}" == "ASAN" ]; then
        cp -r -p ${STARROCKS_HOME}/be/output/conf/asan_suppressions.conf ${STARROCKS_OUTPUT}/be/conf/
    fi
    cp -r -p ${STARROCKS_HOME}/be/output/lib/* ${STARROCKS_OUTPUT}/be/lib/
    # format $BUILD_TYPE to lower case
    ibuildtype=`echo ${BUILD_TYPE} | tr 'A-Z' 'a-z'`
    if [ "${ibuildtype}" == "release" ] ; then
        pushd ${STARROCKS_OUTPUT}/be/lib/ &>/dev/null
        BE_BIN=starrocks_be
        BE_BIN_DEBUGINFO=starrocks_be.debuginfo
        echo "Split $BE_BIN debug symbol to $BE_BIN_DEBUGINFO ..."
        # strip be binary
        # if eu-strip is available, can replace following three lines into `eu-strip -g -f starrocks_be.debuginfo starrocks_be`
        objcopy --only-keep-debug $BE_BIN $BE_BIN_DEBUGINFO
        strip --strip-debug $BE_BIN
        objcopy --add-gnu-debuglink=$BE_BIN_DEBUGINFO $BE_BIN
        popd &>/dev/null
    fi
    cp -r -p ${STARROCKS_HOME}/be/output/www/* ${STARROCKS_OUTPUT}/be/www/
    cp -r -p ${STARROCKS_HOME}/java-extensions/jdbc-bridge/target/starrocks-jdbc-bridge-jar-with-dependencies.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
    cp -r -p ${STARROCKS_HOME}/java-extensions/udf-extensions/target/udf-extensions-jar-with-dependencies.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
    cp -r -p ${STARROCKS_HOME}/java-extensions/java-utils/target/starrocks-java-utils.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
    cp -r -p ${STARROCKS_HOME}/java-extensions/jni-connector/target/starrocks-jni-connector.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
    cp -r -p ${STARROCKS_HOME}/java-extensions/hudi-reader/target/hudi-reader-lib ${STARROCKS_OUTPUT}/be/lib/
    cp -r -p ${STARROCKS_HOME}/java-extensions/hudi-reader/target/starrocks-hudi-reader.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
    cp -r -p ${STARROCKS_HOME}/java-extensions/hudi-reader/target/starrocks-hudi-reader.jar ${STARROCKS_OUTPUT}/be/lib/hudi-reader-lib
    cp -r -p ${STARROCKS_HOME}/java-extensions/paimon-reader/target/paimon-reader-lib ${STARROCKS_OUTPUT}/be/lib/
    cp -r -p ${STARROCKS_HOME}/java-extensions/paimon-reader/target/starrocks-paimon-reader.jar ${STARROCKS_OUTPUT}/be/lib/jni-packages
    cp -r -p ${STARROCKS_HOME}/java-extensions/paimon-reader/target/starrocks-paimon-reader.jar ${STARROCKS_OUTPUT}/be/lib/paimon-reader-lib
    cp -r -p ${STARROCKS_THIRDPARTY}/installed/hadoop/share/hadoop/common ${STARROCKS_OUTPUT}/be/lib/hadoop/
    cp -r -p ${STARROCKS_THIRDPARTY}/installed/hadoop/share/hadoop/hdfs ${STARROCKS_OUTPUT}/be/lib/hadoop/
    cp -r -p ${STARROCKS_THIRDPARTY}/installed/hadoop/lib/native ${STARROCKS_OUTPUT}/be/lib/hadoop/

    rm -f ${STARROCKS_OUTPUT}/be/lib/hadoop/common/lib/log4j-1.2.17.jar
    rm -f ${STARROCKS_OUTPUT}/be/lib/hadoop/hdfs/lib/log4j-1.2.17.jar

    if [ "${WITH_BLOCK_CACHE}" == "ON"  ]; then
        mkdir -p ${STARROCKS_OUTPUT}/be/lib/cachelib
        cp -r -p ${CACHELIB_DIR}/deps/lib64 ${STARROCKS_OUTPUT}/be/lib/cachelib/
    fi

    # note: do not use oracle jdk to avoid commercial dispute
    if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
        cp -r -p ${STARROCKS_THIRDPARTY}/installed/open_jdk/jre/lib/aarch64 ${STARROCKS_OUTPUT}/be/lib/jvm/
    else
        cp -r -p ${STARROCKS_THIRDPARTY}/installed/open_jdk/jre/lib/amd64 ${STARROCKS_OUTPUT}/be/lib/jvm/
    fi
    cp -r -p ${STARROCKS_THIRDPARTY}/installed/jindosdk/* ${STARROCKS_OUTPUT}/be/lib/hadoop/hdfs/
    cp -r -p ${STARROCKS_THIRDPARTY}/installed/jindosdk/* ${STARROCKS_OUTPUT}/be/lib/hudi-reader-lib/
    cp -r -p ${STARROCKS_THIRDPARTY}/installed/jindosdk/* ${STARROCKS_OUTPUT}/be/lib/paimon-reader-lib/
    cp -r -p ${STARROCKS_THIRDPARTY}/installed/broker_thirdparty_jars/* ${STARROCKS_OUTPUT}/be/lib/hadoop/hdfs/
    cp -r -p ${STARROCKS_THIRDPARTY}/installed/broker_thirdparty_jars/* ${STARROCKS_OUTPUT}/be/lib/hudi-reader-lib/
    cp -r -p ${STARROCKS_THIRDPARTY}/installed/broker_thirdparty_jars/* ${STARROCKS_OUTPUT}/be/lib/paimon-reader-lib/
fi



cp -r -p "${STARROCKS_HOME}/LICENSE.txt" "${STARROCKS_OUTPUT}/LICENSE.txt"
build-support/gen_notice.py "${STARROCKS_HOME}/licenses,${STARROCKS_HOME}/licenses-binary" "${STARROCKS_OUTPUT}/NOTICE.txt" all

echo "***************************************"
echo "Successfully build StarRocks"
echo "***************************************"

if [[ ! -z ${STARROCKS_POST_BUILD_HOOK} ]]; then
    eval ${STARROCKS_POST_BUILD_HOOK}
fi

exit 0
