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

# sudo apt-get install binutils-dev
# sudo yum install binutils-devel
#check_prerequest "locate libbfd.a" "binutils-dev"

# sudo apt-get install libiberty-dev
# no need in centos 7.1
#check_prerequest "locate libiberty.a" "libiberty-dev"

# sudo apt-get install bison
# sudo yum install bison
#check_prerequest "bison --version" "bison"

#########################
# build all thirdparties
#########################


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

# libevent
build_libevent() {
    check_if_source_exist $LIBEVENT_SOURCE
    cd $TP_SOURCE_DIR/$LIBEVENT_SOURCE
    if [ ! -f configure ]; then
        ./autogen.sh
    fi

    LDFLAGS="-L${TP_LIB_DIR}" \
    ./configure --prefix=$TP_INSTALL_DIR --enable-shared=no --disable-samples --disable-libevent-regress
    make -j$PARALLEL
    make install
}

build_openssl() {
    OPENSSL_PLATFORM="linux-x86_64"
    if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
        OPENSSL_PLATFORM="linux-aarch64"
    fi

    check_if_source_exist $OPENSSL_SOURCE
    cd $TP_SOURCE_DIR/$OPENSSL_SOURCE

    # use customized CFLAGS/CPPFLAGS/CXXFLAGS/LDFLAGS
    unset CXXFLAGS
    unset CPPFLAGS
    export CFLAGS="-O3 -fno-omit-frame-pointer -fPIC"

    LDFLAGS="-L${TP_LIB_DIR}" \
    LIBDIR="lib" \
    ./Configure --prefix=$TP_INSTALL_DIR -zlib no-shared no-tests ${OPENSSL_PLATFORM}
    make -j$PARALLEL
    make install_sw

    restore_compile_flags
}

# thrift
build_thrift() {
    check_if_source_exist $THRIFT_SOURCE
    cd $TP_SOURCE_DIR/$THRIFT_SOURCE

    if [ ! -f configure ]; then
        ./bootstrap.sh
    fi

    echo ${TP_LIB_DIR}
    ./configure LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" LIBS="-lssl -lcrypto -ldl" \
    --prefix=$TP_INSTALL_DIR --docdir=$TP_INSTALL_DIR/doc --enable-static --disable-shared --disable-tests \
    --disable-tutorial --without-qt4 --without-qt5 --without-csharp --without-erlang --without-nodejs \
    --without-lua --without-perl --without-php --without-php_extension --without-dart --without-ruby \
    --without-haskell --without-go --without-haxe --without-d --without-python -without-java --with-cpp \
    --with-libevent=$TP_INSTALL_DIR --with-boost=$TP_INSTALL_DIR --with-openssl=$TP_INSTALL_DIR

    if [ -f compiler/cpp/thrifty.hh ];then
        mv compiler/cpp/thrifty.hh compiler/cpp/thrifty.h
    fi

    make -j$PARALLEL
    make install
}

# llvm
build_llvm() {
    LLVM_TARGET="X86"
    if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
        LLVM_TARGET="AArch64"
    fi

    check_if_source_exist $LLVM_SOURCE
    check_if_source_exist $CLANG_SOURCE
    check_if_source_exist $COMPILER_RT_SOURCE

    if [ ! -d $TP_SOURCE_DIR/$LLVM_SOURCE/tools/clang ]; then
        cp -rf $TP_SOURCE_DIR/$CLANG_SOURCE $TP_SOURCE_DIR/$LLVM_SOURCE/tools/clang
    fi

    if [ ! -d $TP_SOURCE_DIR/$LLVM_SOURCE/projects/compiler-rt ]; then
        cp -rf $TP_SOURCE_DIR/$COMPILER_RT_SOURCE $TP_SOURCE_DIR/$LLVM_SOURCE/projects/compiler-rt
    fi

    cd $TP_SOURCE_DIR
    mkdir -p llvm-build
    cd llvm-build
    rm -rf CMakeCache.txt CMakeFiles/
    LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
    $CMAKE_CMD -G "${CMAKE_GENERATOR}" -DLLVM_REQUIRES_RTTI:Bool=True -DLLVM_TARGETS_TO_BUILD=${LLVM_TARGET} -DLLVM_ENABLE_TERMINFO=OFF LLVM_BUILD_LLVM_DYLIB:BOOL=OFF -DLLVM_ENABLE_PIC=true -DBUILD_SHARED_LIBS=OFF -DCMAKE_BUILD_TYPE="RELEASE" -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR/llvm ../$LLVM_SOURCE
    ${BUILD_SYSTEM} -j$PARALLEL REQUIRES_RTTI=1
    ${BUILD_SYSTEM} install
}

# protobuf
build_protobuf() {
    check_if_source_exist $PROTOBUF_SOURCE
    cd $TP_SOURCE_DIR/$PROTOBUF_SOURCE
    rm -fr gmock
    mkdir gmock
    cd gmock
    tar xf ${TP_SOURCE_DIR}/$GTEST_NAME
    mv $GTEST_SOURCE gtest
    cd $TP_SOURCE_DIR/$PROTOBUF_SOURCE
    ./autogen.sh
    LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc -pthread -Wl,--whole-archive -lpthread -Wl,--no-whole-archive" \
    ./configure --prefix=${TP_INSTALL_DIR} --disable-shared --enable-static --with-zlib --with-zlib-include=${TP_INSTALL_DIR}/include
    make -j$PARALLEL
    make install
}

# gflags
build_gflags() {
    check_if_source_exist $GFLAGS_SOURCE

    cd $TP_SOURCE_DIR/$GFLAGS_SOURCE
    mkdir -p $BUILD_DIR
    cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    $CMAKE_CMD -G "${CMAKE_GENERATOR}" -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_POSITION_INDEPENDENT_CODE=On ../
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}

# glog
build_glog() {
    check_if_source_exist $GLOG_SOURCE
    cd $TP_SOURCE_DIR/$GLOG_SOURCE

    # to generate config.guess and config.sub to support aarch64
    rm -rf config.*
    autoreconf -i

    LDFLAGS="-L${TP_LIB_DIR}" \
    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
    ./configure --prefix=$TP_INSTALL_DIR --enable-frame-pointers --disable-shared --enable-static
    make -j$PARALLEL
    make install
}

# gtest
build_gtest() {
    check_if_source_exist $GTEST_SOURCE

    cd $TP_SOURCE_DIR/$GTEST_SOURCE
    mkdir -p $BUILD_DIR
    cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    $CMAKE_CMD -G "${CMAKE_GENERATOR}" -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_POSITION_INDEPENDENT_CODE=On ../
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}

# rapidjson
build_rapidjson() {
    check_if_source_exist $RAPIDJSON_SOURCE

    rm -rf $TP_INSTALL_DIR/rapidjson
    cp -r $TP_SOURCE_DIR/$RAPIDJSON_SOURCE/include/rapidjson $TP_INCLUDE_DIR/
}

# simdjson
build_simdjson() {
    check_if_source_exist $SIMDJSON_SOURCE
    cd $TP_SOURCE_DIR/$SIMDJSON_SOURCE

    #ref: https://github.com/simdjson/simdjson/blob/master/HACKING.md
    mkdir -p $BUILD_DIR
    cd $BUILD_DIR
    $CMAKE_CMD -G "${CMAKE_GENERATOR}" -DCMAKE_CXX_FLAGS="-O3" -DCMAKE_C_FLAGS="-O3" -DSIMDJSON_AVX512_ALLOWED=OFF ..
    $CMAKE_CMD --build .
    mkdir -p $TP_INSTALL_DIR/lib

    cp $TP_SOURCE_DIR/$SIMDJSON_SOURCE/$BUILD_DIR/libsimdjson.a $TP_INSTALL_DIR/lib
    cp -r $TP_SOURCE_DIR/$SIMDJSON_SOURCE/include/* $TP_INCLUDE_DIR/
}

# snappy
build_snappy() {
    check_if_source_exist $SNAPPY_SOURCE
    cd $TP_SOURCE_DIR/$SNAPPY_SOURCE

    mkdir -p $BUILD_DIR
    cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    $CMAKE_CMD -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -G "${CMAKE_GENERATOR}" \
    -DCMAKE_INSTALL_LIBDIR=lib64 \
    -DCMAKE_POSITION_INDEPENDENT_CODE=On \
    -DCMAKE_INSTALL_INCLUDEDIR=$TP_INCLUDE_DIR/snappy \
    -DSNAPPY_BUILD_TESTS=0 ../
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
    if [ -f $TP_INSTALL_DIR/lib64/libsnappy.a ]; then
        mkdir -p $TP_INSTALL_DIR/lib
        cp $TP_INSTALL_DIR/lib64/libsnappy.a $TP_INSTALL_DIR/lib/libsnappy.a
    fi

    #build for libarrow.a
    cp $TP_INCLUDE_DIR/snappy/snappy-c.h  $TP_INCLUDE_DIR/snappy-c.h
    cp $TP_INCLUDE_DIR/snappy/snappy-sinksource.h  $TP_INCLUDE_DIR/snappy-sinksource.h
    cp $TP_INCLUDE_DIR/snappy/snappy-stubs-public.h  $TP_INCLUDE_DIR/snappy-stubs-public.h
    cp $TP_INCLUDE_DIR/snappy/snappy.h  $TP_INCLUDE_DIR/snappy.h
    cp $TP_INSTALL_DIR/lib/libsnappy.a $TP_INSTALL_DIR/libsnappy.a
}

# gperftools
build_gperftools() {
    check_if_source_exist $GPERFTOOLS_SOURCE
    cd $TP_SOURCE_DIR/$GPERFTOOLS_SOURCE

    if [ ! -f configure ]; then
        ./autogen.sh
    fi

    LDFLAGS="-L${TP_LIB_DIR}" \
    CFLAGS="-O3 -fno-omit-frame-pointer -fPIC -g" \
    ./configure --prefix=$TP_INSTALL_DIR/gperftools --disable-shared --enable-static --disable-libunwind --with-pic --enable-frame-pointers
    make -j$PARALLEL
    make install
}

# zlib
build_zlib() {
    check_if_source_exist $ZLIB_SOURCE
    cd $TP_SOURCE_DIR/$ZLIB_SOURCE

    LDFLAGS="-L${TP_LIB_DIR}" \
    ./configure --prefix=$TP_INSTALL_DIR --static
    make -j$PARALLEL
    make install

    # build minizip
    cd $TP_SOURCE_DIR/$ZLIB_SOURCE/contrib/minizip
    autoreconf --force --install
    ./configure --prefix=$TP_INSTALL_DIR --enable-static=yes --enable-shared=no
    make -j$PARALLEL
    make install
}

# lz4
build_lz4() {
    check_if_source_exist $LZ4_SOURCE
    cd $TP_SOURCE_DIR/$LZ4_SOURCE

    make -C lib -j$PARALLEL install PREFIX=$TP_INSTALL_DIR \
    INCLUDEDIR=$TP_INCLUDE_DIR/lz4/ BUILD_SHARED=no
}

# lzo
build_lzo2() {
    check_if_source_exist $LZO2_SOURCE
    cd $TP_SOURCE_DIR/$LZO2_SOURCE

    CPPFLAGS="-I${TP_INCLUDE_DIR}" \
        LDFLAGS="-L${TP_LIB_DIR}" \
        ./configure --prefix="${TP_INSTALL_DIR}" --disable-shared --enable-static

    make -j "${PARALLEL}"
    make install
}

# bzip
build_bzip() {
    check_if_source_exist $BZIP_SOURCE
    cd $TP_SOURCE_DIR/$BZIP_SOURCE

    make -j$PARALLEL install PREFIX=$TP_INSTALL_DIR
}

# curl
build_curl() {
    check_if_source_exist $CURL_SOURCE
    cd $TP_SOURCE_DIR/$CURL_SOURCE

    LDFLAGS="-L${TP_LIB_DIR}" LIBS="-lssl -lcrypto -ldl" \
    ./configure --prefix=$TP_INSTALL_DIR --disable-shared --enable-static \
    --without-librtmp --with-ssl=${TP_INSTALL_DIR} --without-libidn2 --without-libgsasl --disable-ldap --enable-ipv6
    make -j$PARALLEL
    make install
}

# re2
build_re2() {
    check_if_source_exist $RE2_SOURCE
    cd $TP_SOURCE_DIR/$RE2_SOURCE

    $CMAKE_CMD -G "${CMAKE_GENERATOR}" -DCMAKE_BUILD_TYPE=Release \
	    -DBUILD_SHARED_LIBS=0 -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR -DCMAKE_INSTALL_LIBDIR=lib
    ${BUILD_SYSTEM} -j$PARALLEL install
}

# boost
build_boost() {
    check_if_source_exist $BOOST_SOURCE
    cd $TP_SOURCE_DIR/$BOOST_SOURCE

    # It is difficult to generate static linked b2, so we use LD_LIBRARY_PATH instead
    ./bootstrap.sh --prefix=$TP_INSTALL_DIR
    LD_LIBRARY_PATH=${STARROCKS_GCC_HOME}/lib:${STARROCKS_GCC_HOME}/lib64:${LD_LIBRARY_PATH} \
    ./b2 link=static runtime-link=static -j $PARALLEL --without-test --without-mpi --without-graph --without-graph_parallel --without-python cxxflags="-std=c++11 -g -fPIC -I$TP_INCLUDE_DIR -L$TP_LIB_DIR" install
}

#leveldb
build_leveldb() {
    check_if_source_exist $LEVELDB_SOURCE
    cd $TP_SOURCE_DIR/$LEVELDB_SOURCE
    LDFLAGS="-L ${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
    make -j$PARALLEL
    cp out-static/libleveldb.a $TP_LIB_DIR/libleveldb.a
    cp -r include/leveldb $TP_INCLUDE_DIR
}

# brpc
build_brpc() {
    check_if_source_exist $BRPC_SOURCE

    cd $TP_SOURCE_DIR/$BRPC_SOURCE
    CMAKE_GENERATOR="Unix Makefiles"
    BUILD_SYSTEM='make'
    ./config_brpc.sh --headers="$TP_INSTALL_DIR/include /usr/include" --libs="$TP_INSTALL_DIR/bin $TP_INSTALL_DIR/lib /usr/lib" --with-glog
    make -j$PARALLEL
    cp -rf output/* ${TP_INSTALL_DIR}/
    if [ -f $TP_INSTALL_DIR/lib/libbrpc.a ]; then
        mkdir -p $TP_INSTALL_DIR/lib64
        cp $TP_SOURCE_DIR/$BRPC_SOURCE/output/lib/libbrpc.a $TP_INSTALL_DIR/lib64/
    fi
}

# rocksdb
build_rocksdb() {
    check_if_source_exist $ROCKSDB_SOURCE

    cd $TP_SOURCE_DIR/$ROCKSDB_SOURCE
    make clean

    CFLAGS= \
    EXTRA_CFLAGS="-I ${TP_INCLUDE_DIR} -I ${TP_INCLUDE_DIR}/snappy -I ${TP_INCLUDE_DIR}/lz4 -L${TP_LIB_DIR}" \
    EXTRA_CXXFLAGS="-fPIC -Wno-deprecated-copy -Wno-stringop-truncation -Wno-pessimizing-move -I ${TP_INCLUDE_DIR} -I ${TP_INCLUDE_DIR}/snappy" \
    EXTRA_LDFLAGS="-static-libstdc++ -static-libgcc" \
    PORTABLE=1 make USE_RTTI=1 -j$PARALLEL static_lib

    cp librocksdb.a $TP_LIB_DIR/librocksdb.a
    cp -r include/rocksdb $TP_INCLUDE_DIR
}

# librdkafka
build_librdkafka() {
    check_if_source_exist $LIBRDKAFKA_SOURCE

    cd $TP_SOURCE_DIR/$LIBRDKAFKA_SOURCE

    $CMAKE_CMD -DCMAKE_LIBRARY_PATH=$TP_INSTALL_DIR/lib -DCMAKE_INCLUDE_PATH=$TP_INSTALL_DIR/include \
        -DBUILD_SHARED_LIBS=0 -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR -DRDKAFKA_BUILD_STATIC=ON -DWITH_SASL=OFF -DWITH_SASL_SCRAM=ON \
        -DRDKAFKA_BUILD_EXAMPLES=OFF -DRDKAFKA_BUILD_TESTS=OFF -DWITH_SSL=ON -DCMAKE_INSTALL_LIBDIR=lib

    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}

# pulsar
build_pulsar() {
    check_if_source_exist $PULSAR_SOURCE

    cd $TP_SOURCE_DIR/$PULSAR_SOURCE/pulsar-client-cpp

    $CMAKE_CMD -DCMAKE_LIBRARY_PATH=$TP_INSTALL_DIR/lib -DCMAKE_INCLUDE_PATH=$TP_INSTALL_DIR/include \
        -DPROTOC_PATH=$TP_INSTALL_DIR/bin/protoc -DBUILD_TESTS=OFF -DBUILD_PYTHON_WRAPPER=OFF -DBUILD_DYNAMIC_LIB=OFF .
    ${BUILD_SYSTEM} -j$PARALLEL

    cp lib/libpulsar.a $TP_INSTALL_DIR/lib/
    cp -r include/pulsar $TP_INSTALL_DIR/include/
}

# flatbuffers
build_flatbuffers() {
  check_if_source_exist $FLATBUFFERS_SOURCE
  cd $TP_SOURCE_DIR/$FLATBUFFERS_SOURCE
  mkdir -p $BUILD_DIR
  cd $BUILD_DIR
  rm -rf CMakeCache.txt CMakeFiles/
  LDFLAGS="-static-libstdc++ -static-libgcc" \
  ${CMAKE_CMD} .. -G "${CMAKE_GENERATOR}" -DFLATBUFFERS_BUILD_TESTS=OFF
  ${BUILD_SYSTEM} -j$PARALLEL
  cp flatc  $TP_INSTALL_DIR/bin/flatc
  cp -r ../include/flatbuffers  $TP_INCLUDE_DIR/flatbuffers
  cp libflatbuffers.a $TP_LIB_DIR/libflatbuffers.a
}

# arrow
build_arrow() {
    export CXXFLAGS="-O3 -fno-omit-frame-pointer -fPIC -g"
    export CFLAGS="-O3 -fno-omit-frame-pointer -fPIC -g"
    export CPPFLAGS=$CXXFLAGS

    check_if_source_exist $ARROW_SOURCE
    cd $TP_SOURCE_DIR/$ARROW_SOURCE/cpp
    mkdir -p release
    cd release
    export ARROW_BROTLI_URL=${TP_SOURCE_DIR}/${BROTLI_NAME}
    export ARROW_GLOG_URL=${TP_SOURCE_DIR}/${GLOG_NAME}
    export ARROW_LZ4_URL=${TP_SOURCE_DIR}/${LZ4_NAME}
    export ARROW_SNAPPY_URL=${TP_SOURCE_DIR}/${SNAPPY_NAME}
    export ARROW_ZLIB_URL=${TP_SOURCE_DIR}/${ZLIB_NAME}
    export ARROW_FLATBUFFERS_URL=${TP_SOURCE_DIR}/${FLATBUFFERS_NAME}
    export ARROW_ZSTD_URL=${TP_SOURCE_DIR}/${ZSTD_NAME}
    export LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc"

    # https://github.com/apache/arrow/blob/apache-arrow-5.0.0/cpp/src/arrow/memory_pool.cc#L286
    #
    # JemallocAllocator use mallocx and rallocx to allocate new memory, but mallocx and rallocx are Non-standard APIs,
    # and can not be hooked in BE, the memory used by arrow can not be counted by BE,
    # so disable jemalloc here and use SystemAllocator.
    #
    # Currently, the standard APIs are hooked in BE, so the jemalloc standard APIs will actually be used.
    ${CMAKE_CMD} -DARROW_PARQUET=ON -DARROW_JSON=ON -DARROW_IPC=ON -DARROW_USE_GLOG=OFF -DARROW_BUILD_SHARED=OFF \
    -DARROW_WITH_BROTLI=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZLIB=ON -DARROW_WITH_ZSTD=ON \
    -DARROW_WITH_UTF8PROC=OFF -DARROW_WITH_RE2=OFF \
    -DARROW_JEMALLOC=OFF -DARROW_MIMALLOC=OFF \
    -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_INSTALL_LIBDIR=lib64 \
    -DARROW_BOOST_USE_SHARED=OFF -DARROW_GFLAGS_USE_SHARED=OFF -DBoost_NO_BOOST_CMAKE=ON -DBOOST_ROOT=$TP_INSTALL_DIR \
    -DJEMALLOC_HOME=$TP_INSTALL_DIR \
    -Dzstd_SOURCE=BUNDLED \
    -Dgflags_ROOT=$TP_INSTALL_DIR/ \
    -DSnappy_ROOT=$TP_INSTALL_DIR/ \
    -DGLOG_ROOT=$TP_INSTALL_DIR/ \
    -DLZ4_ROOT=$TP_INSTALL_DIR/ \
    -G "${CMAKE_GENERATOR}" \
    -DThrift_ROOT=$TP_INSTALL_DIR/ ..

    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
    #copy dep libs
    cp -rf ./brotli_ep/src/brotli_ep-install/lib/libbrotlienc-static.a $TP_INSTALL_DIR/lib64/libbrotlienc.a
    cp -rf ./brotli_ep/src/brotli_ep-install/lib/libbrotlidec-static.a $TP_INSTALL_DIR/lib64/libbrotlidec.a
    cp -rf ./brotli_ep/src/brotli_ep-install/lib/libbrotlicommon-static.a $TP_INSTALL_DIR/lib64/libbrotlicommon.a
    if [ -f ./zstd_ep-install/lib64/libzstd.a ]; then
        cp -rf ./zstd_ep-install/lib64/libzstd.a $TP_INSTALL_DIR/lib64/libzstd.a
    else
        cp -rf ./zstd_ep-install/lib/libzstd.a $TP_INSTALL_DIR/lib64/libzstd.a
    fi
    # copy zstd headers
    mkdir -p ${TP_INSTALL_DIR}/include/zstd
    cp ./zstd_ep-install/include/* ${TP_INSTALL_DIR}/include/zstd

    restore_compile_flags
}

# s2
build_s2() {
    check_if_source_exist $S2_SOURCE
    cd $TP_SOURCE_DIR/$S2_SOURCE
    mkdir -p $BUILD_DIR
    cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
    $CMAKE_CMD -G "${CMAKE_GENERATOR}" -DBUILD_SHARED_LIBS=0 -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_INCLUDE_PATH="$TP_INSTALL_DIR/include" \
    -DBUILD_SHARED_LIBS=OFF \
    -DGFLAGS_ROOT_DIR="$TP_INSTALL_DIR/include" \
    -DWITH_GFLAGS=ON \
    -DGLOG_ROOT_DIR="$TP_INSTALL_DIR/include" \
    -DWITH_GLOG=ON \
    -DCMAKE_LIBRARY_PATH="$TP_INSTALL_DIR/lib;$TP_INSTALL_DIR/lib64" ..
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}

# bitshuffle
build_bitshuffle() {
    check_if_source_exist $BITSHUFFLE_SOURCE
    cd $TP_SOURCE_DIR/$BITSHUFFLE_SOURCE
    PREFIX=$TP_INSTALL_DIR

    # This library has significant optimizations when built with -mavx2. However,
    # we still need to support non-AVX2-capable hardware. So, we build it twice,
    # once with the flag and once without, and use some linker tricks to
    # suffix the AVX2 symbols with '_avx2'.
    arches="default avx2 avx512"
    # Becuase aarch64 don't support avx2, disable it.
    if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
        arches="default"
    fi

    to_link=""
    for arch in $arches ; do
        arch_flag=""
        if [ "$arch" == "avx2" ]; then
            arch_flag="-mavx2"
        elif [ "$arch" == "avx512" ]; then
            arch_flag="-march=icelake-server"
        fi
        tmp_obj=bitshuffle_${arch}_tmp.o
        dst_obj=bitshuffle_${arch}.o
        ${CC:-gcc} $EXTRA_CFLAGS $arch_flag -std=c99 -I$PREFIX/include/lz4/ -O3 -DNDEBUG -fPIC -c \
            "src/bitshuffle_core.c" \
            "src/bitshuffle.c" \
            "src/iochain.c"
        # Merge the object files together to produce a combined .o file.
        ld -r -o $tmp_obj bitshuffle_core.o bitshuffle.o iochain.o
        # For the AVX2 symbols, suffix them.
        if [ "$arch" == "avx2" ]; then
            # Create a mapping file with '<old_sym> <suffixed_sym>' on each line.
            nm --defined-only --extern-only $tmp_obj | while read addr type sym ; do
              echo ${sym} ${sym}_${arch}
            done > renames.txt
            objcopy --redefine-syms=renames.txt $tmp_obj $dst_obj
        elif [ "$arch" == "avx512" ]; then
            # Create a mapping file with '<old_sym> <suffixed_sym>' on each line.
            nm --defined-only --extern-only $tmp_obj | while read addr type sym ; do
              echo ${sym} ${sym}_${arch}
            done > renames.txt
            objcopy --redefine-syms=renames.txt $tmp_obj $dst_obj
        else
            mv $tmp_obj $dst_obj
        fi
        to_link="$to_link $dst_obj"
    done
    rm -f libbitshuffle.a
    ar rs libbitshuffle.a $to_link
    mkdir -p $PREFIX/include/bitshuffle
    cp libbitshuffle.a $PREFIX/lib/
    cp $TP_SOURCE_DIR/$BITSHUFFLE_SOURCE/src/bitshuffle.h $PREFIX/include/bitshuffle/bitshuffle.h
    cp $TP_SOURCE_DIR/$BITSHUFFLE_SOURCE/src/bitshuffle_core.h $PREFIX/include/bitshuffle/bitshuffle_core.h
}

# croaring bitmap
# If open AVX512 default, current version will be compiled failed on some machine, so close AVX512 default,
# When this problem is solved, a switch will be added to control.
build_croaringbitmap() {
    FORCE_AVX=ON
    # avx2 is not supported by aarch64.
    if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
        FORCE_AVX=FALSE
    fi
    if [[ `cat /proc/cpuinfo |grep avx|wc -l` == "0" ]]; then
        FORCE_AVX=FALSE
    fi
    check_if_source_exist $CROARINGBITMAP_SOURCE
    cd $TP_SOURCE_DIR/$CROARINGBITMAP_SOURCE
    mkdir -p $BUILD_DIR
    cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    LDFLAGS="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc" \
    $CMAKE_CMD -G "${CMAKE_GENERATOR}" -DROARING_BUILD_STATIC=ON -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
    -DCMAKE_INCLUDE_PATH="$TP_INSTALL_DIR/include" \
    -DENABLE_ROARING_TESTS=OFF \
    -DROARING_DISABLE_NATIVE=ON \
    -DFORCE_AVX=$FORCE_AVX \
    -DROARING_DISABLE_AVX512=ON \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_LIBRARY_PATH="$TP_INSTALL_DIR/lib;$TP_INSTALL_DIR/lib64" ..
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}
#orc
build_orc() {
    check_if_source_exist $ORC_SOURCE
    cd $TP_SOURCE_DIR/$ORC_SOURCE
    mkdir -p $BUILD_DIR
    cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    $CMAKE_CMD ../ -DBUILD_JAVA=OFF \
    -G "${CMAKE_GENERATOR}" \
    -DPROTOBUF_HOME=$TP_INSTALL_DIR \
    -DSNAPPY_HOME=$TP_INSTALL_DIR \
    -DGTEST_HOME=$TP_INSTALL_DIR \
    -DLZ4_HOME=$TP_INSTALL_DIR \
    -DLZ4_INCLUDE_DIR=$TP_INSTALL_DIR/include/lz4 \
    -DZLIB_HOME=$TP_INSTALL_DIR\
    -DBUILD_LIBHDFSPP=OFF \
    -DBUILD_CPP_TESTS=OFF \
    -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR

    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}

#cctz
build_cctz() {
    check_if_source_exist $CCTZ_SOURCE
    cd $TP_SOURCE_DIR/$CCTZ_SOURCE

    make -j$PARALLEL
    PREFIX=${TP_INSTALL_DIR} make install
}

#fmt
build_fmt() {
    check_if_source_exist $FMT_SOURCE
    cd $TP_SOURCE_DIR/$FMT_SOURCE
    mkdir -p build
    cd build
    $CMAKE_CMD -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${TP_INSTALL_DIR} ../ \
            -DCMAKE_INSTALL_LIBDIR=lib64 -G "${CMAKE_GENERATOR}" -DFMT_TEST=OFF
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}

#ryu
build_ryu() {
    check_if_source_exist $RYU_SOURCE
    cd $TP_SOURCE_DIR/$RYU_SOURCE/ryu
    make -j$PARALLEL
    make install DESTDIR=${TP_INSTALL_DIR}
    mkdir -p $TP_INSTALL_DIR/include/ryu
    mv $TP_INSTALL_DIR/include/ryu.h $TP_INSTALL_DIR/include/ryu
    # copy to 64 to compatable with current CMake
    cp -f ${TP_INSTALL_DIR}/lib/libryu.a ${TP_INSTALL_DIR}/lib64/libryu.a
}

#break_pad
build_breakpad() {
    check_if_source_exist $BREAK_PAD_SOURCE
    cd $TP_SOURCE_DIR/$BREAK_PAD_SOURCE
    mkdir -p src/third_party/lss
    cp $TP_PATCH_DIR/linux_syscall_support.h src/third_party/lss
    CFLAGS= ./configure --prefix=$TP_INSTALL_DIR --enable-shared=no --disable-samples --disable-libevent-regress
    make -j$PARALLEL
    make install
}

#hadoop
build_hadoop() {
    check_if_source_exist $HADOOP_SOURCE
    cp -r $TP_SOURCE_DIR/$HADOOP_SOURCE $TP_INSTALL_DIR/hadoop
    # remove unnecessary doc and logs
    rm -rf $TP_INSTALL_DIR/hadoop/logs/* $TP_INSTALL_DIR/hadoop/share/doc/hadoop
    mkdir -p $TP_INSTALL_DIR/include/hdfs
    cp $TP_SOURCE_DIR/$HADOOP_SOURCE/include/hdfs.h $TP_INSTALL_DIR/include/hdfs
    cp $TP_SOURCE_DIR/$HADOOP_SOURCE/lib/native/libhdfs.a $TP_INSTALL_DIR/lib
}

#jdk
build_jdk() {
    check_if_source_exist $JDK_SOURCE
    rm -rf $TP_INSTALL_DIR/open_jdk && cp -r $TP_SOURCE_DIR/$JDK_SOURCE $TP_INSTALL_DIR/open_jdk
}

# ragel
# ragel-6.9+ is used by hypercan, so we build it first.
build_ragel() {
    check_if_source_exist $RAGEL_SOURCE
    cd $TP_SOURCE_DIR/$RAGEL_SOURCE
    # generage a static linked ragel, hyperscan will depend on it
    LDFLAGS=" -static-libstdc++ -static-libgcc" \
    ./configure --prefix=$TP_INSTALL_DIR --disable-shared --enable-static
    make -j$PARALLEL
    make install
}

#hyperscan
build_hyperscan() {
    check_if_source_exist $HYPERSCAN_SOURCE
    cd $TP_SOURCE_DIR/$HYPERSCAN_SOURCE
    export PATH=$TP_INSTALL_DIR/bin:$PATH
    $CMAKE_CMD -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${TP_INSTALL_DIR} -DBOOST_ROOT=$STARROCKS_THIRDPARTY/installed/include \
          -DCMAKE_CXX_COMPILER=$STARROCKS_GCC_HOME/bin/g++ -DCMAKE_C_COMPILER=$STARROCKS_GCC_HOME/bin/gcc  -DCMAKE_INSTALL_LIBDIR=lib \
          -DBUILD_EXAMPLES=OFF -DBUILD_UNIT=OFF
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}

#mariadb-connector-c
build_mariadb() {
    OLD_CMAKE_GENERATOR=${CMAKE_GENERATOR}
    OLD_BUILD_SYSTEM=${BUILD_SYSTEM}

    unset CXXFLAGS
    unset CPPFLAGS
    export CFLAGS="-O3 -fno-omit-frame-pointer -fPIC"

    # force use make build system, since ninja doesn't support install only headers
    CMAKE_GENERATOR="Unix Makefiles"
    BUILD_SYSTEM='make'

    check_if_source_exist $MARIADB_SOURCE
    cd $TP_SOURCE_DIR/$MARIADB_SOURCE
    mkdir -p build && cd build

    $CMAKE_CMD .. -G "${CMAKE_GENERATOR}" -DCMAKE_BUILD_TYPE=Release    \
                  -DWITH_UNIT_TESTS=OFF                                 \
                  -DBUILD_SHARED_LIBS=OFF                               \
                  -DOPENSSL_ROOT_DIR=${TP_INSTALL_DIR}                  \
                  -DOPENSSL_USE_STATIC_LIBS=TRUE                        \
                  -DCMAKE_INSTALL_PREFIX=${TP_INSTALL_DIR}
    # we only need build libmariadbclient and headers
    ${BUILD_SYSTEM} -j$PARALLEL mariadbclient
    cd $TP_SOURCE_DIR/$MARIADB_SOURCE/build/libmariadb
    mkdir -p $TP_INSTALL_DIR/lib/mariadb/
    cp libmariadbclient.a $TP_INSTALL_DIR/lib/mariadb/
    # install mariadb headers
    cd $TP_SOURCE_DIR/$MARIADB_SOURCE/build/include
    ${BUILD_SYSTEM} install

    restore_compile_flags
    export CMAKE_GENERATOR=$OLD_CMAKE_GENERATOR
    export BUILD_SYSTEM=$OLD_BUILD_SYSTEM
}

# jindosdk for Aliyun OSS
build_aliyun_jindosdk() {
    check_if_source_exist $JINDOSDK_SOURCE
    mkdir -p $TP_INSTALL_DIR/jindosdk
    cp -r $TP_SOURCE_DIR/$JINDOSDK_SOURCE/lib/*.jar $TP_INSTALL_DIR/jindosdk
}

build_gcs_connector() {
    check_if_source_exist $GCS_CONNECTOR_SOURCE
    mkdir -p $TP_INSTALL_DIR/gcs_connector
    cp -r $TP_SOURCE_DIR/$GCS_CONNECTOR_SOURCE/*.jar $TP_INSTALL_DIR/gcs_connector
}

build_broker_thirdparty_jars() {
    check_if_source_exist $BROKER_THIRDPARTY_JARS_SOURCE
    mkdir -p $TP_INSTALL_DIR/$BROKER_THIRDPARTY_JARS_SOURCE
    cp -r $TP_SOURCE_DIR/$BROKER_THIRDPARTY_JARS_SOURCE/* $TP_INSTALL_DIR/$BROKER_THIRDPARTY_JARS_SOURCE
    rm $TP_INSTALL_DIR/$BROKER_THIRDPARTY_JARS_SOURCE/hadoop-aliyun-2.7.2.jar
    # ensure read permission is granted for all users
    chmod -R +r $TP_INSTALL_DIR/$BROKER_THIRDPARTY_JARS_SOURCE/
}

build_aws_cpp_sdk() {
    export CFLAGS="-O3 -fno-omit-frame-pointer -std=c99 -fPIC -D_POSIX_C_SOURCE=200112L"

    check_if_source_exist $AWS_SDK_CPP_SOURCE
    cd $TP_SOURCE_DIR/$AWS_SDK_CPP_SOURCE
    # only build s3, s3-crt, transfer manager, identity-management and sts, you can add more components if you want.
    $CMAKE_CMD -Bbuild -DBUILD_ONLY="core;s3;s3-crt;transfer;identity-management;sts" -DCMAKE_BUILD_TYPE=RelWithDebInfo \
               -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_PREFIX=${TP_INSTALL_DIR} -DENABLE_TESTING=OFF \
               -DENABLE_CURL_LOGGING=OFF \
               -G "${CMAKE_GENERATOR}" \
               -D_POSIX_C_SOURCE=200112L \
               -DCURL_LIBRARY_RELEASE=${TP_INSTALL_DIR}/lib/libcurl.a   \
               -DZLIB_LIBRARY_RELEASE=${TP_INSTALL_DIR}/lib/libz.a      \
               -DOPENSSL_ROOT_DIR=${TP_INSTALL_DIR}                     \
               -DOPENSSL_USE_STATIC_LIBS=TRUE                           \
               -Dcrypto_LIBRARY=${TP_INSTALL_DIR}/lib/libcrypto.a

    cd build
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install

    restore_compile_flags
}

# velocypack
build_vpack() {
    check_if_source_exist $VPACK_SOURCE
    cd $TP_SOURCE_DIR/$VPACK_SOURCE
    mkdir -p build
    cd build
    $CMAKE_CMD .. \
        -DCMAKE_CXX_STANDARD="17" \
        -G "${CMAKE_GENERATOR}" \
        -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${TP_INSTALL_DIR} \
        -DCMAKE_CXX_COMPILER=$STARROCKS_GCC_HOME/bin/g++ -DCMAKE_C_COMPILER=$STARROCKS_GCC_HOME/bin/gcc

    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}

# opentelemetry
build_opentelemetry() {
    check_if_source_exist $OPENTELEMETRY_SOURCE

    cd $TP_SOURCE_DIR/$OPENTELEMETRY_SOURCE
    mkdir -p $BUILD_DIR
    cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    $CMAKE_CMD .. \
        -DCMAKE_CXX_STANDARD="17" \
        -G "${CMAKE_GENERATOR}" \
        -DCMAKE_INSTALL_PREFIX=${TP_INSTALL_DIR} \
        -DBUILD_TESTING=OFF -DWITH_EXAMPLES=OFF \
        -DCMAKE_INSTALL_LIBDIR=lib64 \
        -DWITH_STL=OFF -DWITH_JAEGER=ON
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}

# jemalloc
build_jemalloc() {
    check_if_source_exist $JEMALLOC_SOURCE

    cd $TP_SOURCE_DIR/$JEMALLOC_SOURCE
    # jemalloc supports a runtime page size that's smaller or equal to the build
    # time one, but aborts on a larger one. If not defined, it falls back to the
    # the build system's _SC_PAGESIZE, which in many architectures can vary. Set
    # this to 64K (2^16) for arm architecture, and default 4K on x86 for performance.
    local addition_opts=" --with-lg-page=12"
    if [[ $MACHINE_TYPE == "aarch64" ]] ; then
        # change to 64K for arm architecture
        addition_opts=" --with-lg-page=16"
    fi
    CFLAGS="-O3 -fno-omit-frame-pointer -fPIC -g" \
    ./configure --prefix=${TP_INSTALL_DIR} --with-jemalloc-prefix=je --enable-prof --disable-cxx --disable-libdl --disable-shared $addition_opts
    make -j$PARALLEL
    make install
}

# google benchmark
build_benchmark() {
    check_if_source_exist $BENCHMARK_SOURCE
    cd $TP_SOURCE_DIR/$BENCHMARK_SOURCE
    mkdir -p $BUILD_DIR
    cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/
    cmake -DBENCHMARK_DOWNLOAD_DEPENDENCIES=off \
          -DBENCHMARK_ENABLE_GTEST_TESTS=off \
          -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
          -DCMAKE_INSTALL_LIBDIR=lib64 \
          -DCMAKE_BUILD_TYPE=Release ../
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}

# fast float
build_fast_float() {
    check_if_source_exist $FAST_FLOAT_SOURCE
    cd $TP_SOURCE_DIR/$FAST_FLOAT_SOURCE
    cp -r $TP_SOURCE_DIR/$FAST_FLOAT_SOURCE/include $TP_INSTALL_DIR
}

build_cachelib() {
    check_if_source_exist $CACHELIB_SOURCE
    rm -rf $TP_INSTALL_DIR/$CACHELIB_SOURCE && mv $TP_SOURCE_DIR/$CACHELIB_SOURCE $TP_INSTALL_DIR/
}

build_starcache() {
    check_if_source_exist $STARCACHE_SOURCE
    cp -r $TP_SOURCE_DIR/$STARCACHE_SOURCE/include/* $TP_INCLUDE_DIR/
    cp -r $TP_SOURCE_DIR/$STARCACHE_SOURCE/lib/* $TP_LIB_DIR/
}

# streamvbyte
build_streamvbyte() {
    check_if_source_exist $STREAMVBYTE_SOURCE

    cd $TP_SOURCE_DIR/$STREAMVBYTE_SOURCE/

    mkdir -p $BUILD_DIR
    cd $BUILD_DIR
    rm -rf CMakeCache.txt CMakeFiles/

    CMAKE_GENERATOR="Unix Makefiles"
    BUILD_SYSTEM='make'
    $CMAKE_CMD .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX:PATH=$TP_INSTALL_DIR/

    make -j$PARALLEL
    make install
}

# jansson
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

# serders
build_serdes() {
    export CFLAGS="-O3 -fno-omit-frame-pointer -fPIC -g"
    check_if_source_exist $SERDES_SOURCE
    cd $TP_SOURCE_DIR/$SERDES_SOURCE
    export LIBS="-lrt -lpthread -lcurl -ljansson -lrdkafka -lrdkafka++ -lavro -lssl -lcrypto -ldl"
    ./configure --prefix=${TP_INSTALL_DIR} \
                --libdir=${TP_INSTALL_DIR}/lib \
                --CFLAGS="-I ${TP_INSTALL_DIR}/include"  \
                --CXXFLAGS="-I ${TP_INSTALL_DIR}/include" \
                --LDFLAGS="-L ${TP_INSTALL_DIR}/lib -L ${TP_INSTALL_DIR}/lib64" \
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

# async-profiler
build_async_profiler() {
    check_if_source_exist $ASYNC_PROFILER_SOURCE
    mkdir -p $TP_INSTALL_DIR/async-profiler
    cp -r $TP_SOURCE_DIR/$ASYNC_PROFILER_SOURCE/build $TP_INSTALL_DIR/async-profiler
    cp -r $TP_SOURCE_DIR/$ASYNC_PROFILER_SOURCE/profiler.sh $TP_INSTALL_DIR/async-profiler
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


# set GLOBAL_C*FLAGS for easy restore in each sub build process
export GLOBAL_CPPFLAGS="-I ${TP_INCLUDE_DIR}"
# https://stackoverflow.com/questions/42597685/storage-size-of-timespec-isnt-known
export GLOBAL_CFLAGS="-O3 -fno-omit-frame-pointer -std=c99 -fPIC -g -D_POSIX_C_SOURCE=199309L"
export GLOBAL_CXXFLAGS="-O3 -fno-omit-frame-pointer -Wno-class-memaccess -fPIC -g"

# set those GLOBAL_*FLAGS to the CFLAGS/CXXFLAGS/CPPFLAGS
export CPPFLAGS=$GLOBAL_CPPFLAGS
export CXXFLAGS=$GLOBAL_CXXFLAGS
export CFLAGS=$GLOBAL_CFLAGS

build_libevent
build_zlib
build_lz4
build_lzo2
build_bzip
build_openssl
build_boost # must before thrift
build_protobuf
build_gflags
build_gtest
build_glog
build_rapidjson
build_simdjson
build_snappy
build_gperftools
build_curl
build_re2
build_thrift
build_leveldb
build_brpc
build_rocksdb
build_librdkafka
build_flatbuffers
# must build before arrow
build_jemalloc
build_arrow
build_pulsar
build_s2
build_bitshuffle
build_croaringbitmap
build_cctz
build_fmt
build_ryu
build_hadoop
build_jdk
build_ragel
build_hyperscan
build_mariadb
build_aliyun_jindosdk
build_gcs_connector
build_broker_thirdparty_jars
build_aws_cpp_sdk
build_vpack
build_opentelemetry
build_benchmark
build_fast_float
build_cachelib
build_starcache
build_streamvbyte
build_jansson
build_avro_c
build_serdes
build_async_profiler

if [[ "${MACHINE_TYPE}" != "aarch64" ]]; then
    build_breakpad
fi

# strip unnecessary debug symbol for binaries in thirdparty
strip_binary

echo "Finished to build all thirdparties"
