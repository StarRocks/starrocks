#!/bin/bash

set -e
set -o pipefail

OLD_CWD=`pwd`

CWD=`dirname $0`
mkdir -p $CWD/build
cd $CWD/build

CMAKE_CMD=${CMAKE_CMD:-"cmake"}
STARROCKS_THIRDPARTY=${STARROCKS_THIRDPARTY:-"/home/disk1/doris-deps/thirdparty"}
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-"Debug"}
unset STARROCKS_HOME

${CMAKE_CMD} .. \
    -DSNAPPY_HOME=${STARROCKS_THIRDPARTY}/installed \
    -DZLIB_HOME=${STARROCKS_THIRDPARTY}/installed \
    -DGTEST_HOME=${STARROCKS_THIRDPARTY}/installed \
    -DPROTOBUF_HOME=${STARROCKS_THIRDPARTY}/installed \
    -DZSTD_HOME=${STARROCKS_THIRDPARTY}/installed \
    -DZSTD_INCLUDE_DIR=${STARROCKS_THIRDPARTY}/installed/include/ \
    -DZSTD_LIBRARY=${STARROCKS_THIRDPARTY}/installed/lib/libzstd.a \
    -DLZ4_HOME=${STARROCKS_THIRDPARTY}/installed \
    -DLZ4_INCLUDE_DIR=${STARROCKS_THIRDPARTY}/installed/include/ \
    -DLZ4_LIBRARY=${STARROCKS_THIRDPARTY}/installed/lib/liblz4.a \
    -DBUILD_JAVA=OFF \
    -DBUILD_LIBHDFSPP=OFF \
    -DBUILD_TOOLS=ON \
    -DBUILD_CPP_TESTS=ON \
    -DSTOP_BUILD_ON_WARNING=OFF \
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}

make -j ${PARALLEL}

cd $OLD_CWD
