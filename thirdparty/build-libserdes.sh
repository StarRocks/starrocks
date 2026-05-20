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

#################################################################################
# This script will
# 1. Check prerequisite libraries. Including:
#    cmake byacc flex automake libtool binutils-dev libiberty-dev bison
# 2. Compile and install all thirdparties which are downloaded
#    using *download-thirdparty.sh*.
#
# This script will run *download-thirdparty.sh* once again
# to check if all thirdparties have been downloaded, unpacked and patched.
#################################################################################
set -e

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`

export STARROCKS_HOME=${STARROCKS_HOME:-$curdir/..}
export TP_DIR=$curdir

# include custom environment variables
if [[ -f ${STARROCKS_HOME}/env.sh ]]; then
    . ${STARROCKS_HOME}/env.sh
fi

if [[ ! -f ${TP_DIR}/download-thirdparty.sh ]]; then
    echo "Download thirdparty script is missing".
    exit 1
fi

if [ ! -f ${TP_DIR}/vars.sh ]; then
    echo "vars.sh is missing".
    exit 1
fi
. ${TP_DIR}/vars.sh

cd $TP_DIR

# Download thirdparties.
${TP_DIR}/download-thirdparty.sh

# set COMPILER
if [[ ! -z ${STARROCKS_GCC_HOME} ]]; then
    export CC=${STARROCKS_GCC_HOME}/bin/gcc
    export CPP=${STARROCKS_GCC_HOME}/bin/cpp
    export CXX=${STARROCKS_GCC_HOME}/bin/g++
    export PATH=${STARROCKS_GCC_HOME}/bin:$PATH
else
    echo "STARROCKS_GCC_HOME environment variable is not set"
    exit 1
fi

# prepare installed prefix
mkdir -p ${TP_DIR}/installed

check_prerequest() {
    local CMD=$1
    local NAME=$2
    if ! $CMD; then
        echo $NAME is missing
        exit 1
    else
        echo $NAME is found
    fi
}

# sudo apt-get install cmake
# sudo yum install cmake
check_prerequest "${CMAKE_CMD} --version" "cmake"

# sudo apt-get install byacc
# sudo yum install byacc
check_prerequest "byacc -V" "byacc"

# sudo apt-get install flex
# sudo yum install flex
check_prerequest "flex -V" "flex"

# sudo apt-get install automake
# sudo yum install automake
check_prerequest "automake --version" "automake"

# sudo apt-get install libtool
# sudo yum install libtool
check_prerequest "libtoolize --version" "libtool"

BUILD_SYSTEM=${BUILD_SYSTEM:-make}

####################################
# build libserdes & its dependencies
####################################

# Name of cmake build directory in each thirdpary project.
# Do not use `build`, because many projects contained a file named `BUILD`
# and if the filesystem is not case sensitive, `mkdir` will fail.
BUILD_DIR=starrocks_build
MACHINE_TYPE=$(uname -m)

# handle mac m1 platform, change arm64 to aarch64
if [[ "${MACHINE_TYPE}" == "arm64" ]]; then
    MACHINE_TYPE="aarch64"
fi

echo "machine type : $MACHINE_TYPE"

if [[ -z ${THIRD_PARTY_BUILD_WITH_AVX2} ]]; then
    THIRD_PARTY_BUILD_WITH_AVX2=ON
fi

if [ -e /proc/cpuinfo ] ; then
    # detect cpuinfo
    if [[ -z $(grep -o 'avx[^ ]\+' /proc/cpuinfo) ]]; then
        THIRD_PARTY_BUILD_WITH_AVX2=OFF
    fi
fi

check_if_source_exist() {
    if [ -z $1 ]; then
        echo "dir should specified to check if exist."
        exit 1
    fi

    if [ ! -d $TP_SOURCE_DIR/$1 ];then
        echo "$TP_SOURCE_DIR/$1 does not exist."
        exit 1
    fi
    echo "===== begin build $1"
}

check_if_archieve_exist() {
    if [ -z $1 ]; then
        echo "archieve should specified to check if exist."
        exit 1
    fi

    if [ ! -f $TP_SOURCE_DIR/$1 ];then
        echo "$TP_SOURCE_DIR/$1 does not exist."
        exit 1
    fi
}

build_jansson() {
    check_if_source_exist $JANSSON_SOURCE
    cd $TP_SOURCE_DIR/$JANSSON_SOURCE/
    mkdir -p build
    cd build
    $CMAKE_CMD .. -DCMAKE_INSTALL_PREFIX=${TP_INSTALL_DIR} -DCMAKE_INSTALL_LIBDIR=lib
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}

# avro-c
build_avro_c() {
    check_if_source_exist $AVRO_SOURCE
    cd $TP_SOURCE_DIR/$AVRO_SOURCE/lang/c
    mkdir -p build
    cd build
    $CMAKE_CMD .. -DCMAKE_INSTALL_PREFIX=${TP_INSTALL_DIR} -DCMAKE_INSTALL_LIBDIR=lib64 -DCMAKE_BUILD_TYPE=Release
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
    rm ${TP_INSTALL_DIR}/lib64/libavro.so*
}

build_serdes() {
    export CFLAGS="-O3 -fno-omit-frame-pointer -fPIC -g"
    check_if_source_exist $SERDES_SOURCE
    cd $TP_SOURCE_DIR/$SERDES_SOURCE
    export LIBS="-lrt -lpthread -lcurl -ljansson -lrdkafka -lrdkafka++ -lavro -lssl -lcrypto -ldl"
    ./configure --prefix=${TP_INSTALL_DIR} \
                --libdir=${TP_INSTALL_DIR}/lib \
                --CFLAGS="-I ${TP_INSTALL_DIR}/include -I ${STARROCKS_THIRDPARTY}/installed/include"  \
                --CXXFLAGS="-I ${TP_INSTALL_DIR}/include -I ${STARROCKS_THIRDPARTY}/installed/include" \
                --LDFLAGS="-L ${TP_INSTALL_DIR}/lib -L ${TP_INSTALL_DIR}/lib64 -L ${STARROCKS_THIRDPARTY}/installed/lib"\
                --enable-static \
                --disable-shared

    make -j$PARALLEL
    make install
    rm ${TP_INSTALL_DIR}/lib/libserdes.so*
    # these symbols also be definition in librdkafka, change these symbols to be local.
    objcopy --localize-symbol=cnd_timedwait ${TP_INSTALL_DIR}/lib/libserdes.a
    objcopy --localize-symbol=cnd_timedwait_ms ${TP_INSTALL_DIR}/lib/libserdes.a
    objcopy --localize-symbol=thrd_is_current ${TP_INSTALL_DIR}/lib/libserdes.a
    unset LIBS
    restore_compile_flags
}

# restore cxxflags/cppflags/cflags to default one
restore_compile_flags() {
    # c preprocessor flags
    export CPPFLAGS=$GLOBAL_CPPFLAGS
    # c flags
    export CFLAGS=$GLOBAL_CFLAGS
    # c++ flags
    export CXXFLAGS=$GLOBAL_CXXFLAGS
}

strip_binary() {
    # strip binary tools and ignore any errors
    echo "Strip binaries in $TP_INSTALL_DIR/bin/ ..."
    strip $TP_INSTALL_DIR/bin/* 2>/dev/null || true
}

# strip `$TP_SOURCE_DIR` and `$TP_INSTALL_DIR` from source code file path
export FILE_PREFIX_MAP_OPTION="-ffile-prefix-map=${TP_SOURCE_DIR}=. -ffile-prefix-map=${TP_INSTALL_DIR}=."
# set GLOBAL_C*FLAGS for easy restore in each sub build process
export GLOBAL_CPPFLAGS="-I ${TP_INCLUDE_DIR}"
# https://stackoverflow.com/questions/42597685/storage-size-of-timespec-isnt-known
export GLOBAL_CFLAGS="-O3 -fno-omit-frame-pointer -std=c99 -fPIC -g -D_POSIX_C_SOURCE=200112L -gz=zlib ${FILE_PREFIX_MAP_OPTION}"
export GLOBAL_CXXFLAGS="-O3 -fno-omit-frame-pointer -Wno-class-memaccess -fPIC -g -gz=zlib ${FILE_PREFIX_MAP_OPTION}"

# set those GLOBAL_*FLAGS to the CFLAGS/CXXFLAGS/CPPFLAGS
export CPPFLAGS=$GLOBAL_CPPFLAGS
export CXXFLAGS=$GLOBAL_CXXFLAGS
export CFLAGS=$GLOBAL_CFLAGS

build_jansson
build_avro_c
build_serdes

cp -r ${TP_DIR}/installed/include/libserdes ${STARROCKS_THIRDPARTY}/installed/include
cp ${TP_INSTALL_DIR}/lib/libserdes.a ${STARROCKS_THIRDPARTY}/installed/lib

# strip unnecessary debug symbol for binaries in thirdparty
strip_binary

echo "Finished to build all thirdparties"
