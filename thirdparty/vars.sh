#!/bin/bash
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

############################################################
# You may have to set variables bellow,
# which are used for compiling thirdparties and starrocks itself.
############################################################

# --job param for *make*
PARALLEL=$[$(nproc)/4+1]

###################################################
# DO NOT change variables bellow unless you known
# what you are doing.
###################################################

# thirdparties will be downloaded and unpacked here
export TP_SOURCE_DIR=$TP_DIR/src

# thirdparties will be installed to here
export TP_INSTALL_DIR=$TP_DIR/installed

# patches for all thirdparties
export TP_PATCH_DIR=$TP_DIR/patches

# header files of all thirdparties will be intalled to here
export TP_INCLUDE_DIR=$TP_INSTALL_DIR/include

# libraries of all thirdparties will be intalled to here
export TP_LIB_DIR=$TP_INSTALL_DIR/lib

# all java libraries will be unpacked to here
export TP_JAR_DIR=$TP_INSTALL_DIR/lib/jar

#####################################################
# Download url, filename and unpacked filename
# of all thirdparties
#####################################################

# Definitions for architecture-related thirdparty
MACHINE_TYPE=$(uname -m)
VARS_TARGET=vars-${MACHINE_TYPE}.sh

if [ ! -f ${TP_DIR}/${VARS_TARGET} ]; then
    echo "${VARS_TARGET} is missing".
    exit 1
fi
. ${TP_DIR}/${VARS_TARGET}

# libevent
# the last release version of libevent is 2.1.8, which was released on 26 Jan 2017, that is too old.
# so we use the master version of libevent, which is downloaded on 22 Jun 2018, with commit 24236aed01798303745470e6c498bf606e88724a
LIBEVENT_DOWNLOAD="https://github.com/libevent/libevent/archive/24236ae.zip"
LIBEVENT_NAME=libevent-24236aed01798303745470e6c498bf606e88724a.zip
LIBEVENT_SOURCE=libevent-24236aed01798303745470e6c498bf606e88724a
LIBEVENT_MD5SUM="c6c4e7614f03754b8c67a17f68177649"

# openssl
OPENSSL_DOWNLOAD="https://github.com/openssl/openssl/archive/OpenSSL_1_1_1m.tar.gz"
OPENSSL_NAME=openssl-OpenSSL_1_1_1m.tar.gz
OPENSSL_SOURCE=openssl-OpenSSL_1_1_1m
OPENSSL_MD5SUM="710c2368d28f1a25ab92e25b5b9b11ec"

# thrift
THRIFT_DOWNLOAD="http://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.tar.gz"
THRIFT_NAME=thrift-0.13.0.tar.gz
THRIFT_SOURCE=thrift-0.13.0
THRIFT_MD5SUM="38a27d391a2b03214b444cb13d5664f1"

# protobuf
PROTOBUF_DOWNLOAD="https://github.com/google/protobuf/archive/v3.14.0.tar.gz"
PROTOBUF_NAME=protobuf-3.14.0.tar.gz
PROTOBUF_SOURCE=protobuf-3.14.0
PROTOBUF_MD5SUM="0c9d2a96f3656ba7ef3b23b533fb6170"

# gflags
GFLAGS_DOWNLOAD="https://github.com/gflags/gflags/archive/v2.2.2.tar.gz"
GFLAGS_NAME=gflags-2.2.2.tar.gz
GFLAGS_SOURCE=gflags-2.2.2
GFLAGS_MD5SUM="1a865b93bacfa963201af3f75b7bd64c"

# glog
GLOG_DOWNLOAD="https://github.com/google/glog/archive/v0.4.0.tar.gz"
GLOG_NAME=glog-0.4.0.tar.gz
GLOG_SOURCE=glog-0.4.0
GLOG_MD5SUM="0daea8785e6df922d7887755c3d100d0"

# gtest
GTEST_DOWNLOAD="https://github.com/google/googletest/archive/release-1.10.0.tar.gz"
GTEST_NAME=googletest-release-1.10.0.tar.gz
GTEST_SOURCE=googletest-release-1.10.0
GTEST_MD5SUM="ecd1fa65e7de707cd5c00bdac56022cd"

# snappy
SNAPPY_DOWNLOAD="https://github.com/google/snappy/archive/1.1.8.tar.gz"
SNAPPY_NAME=snappy-1.1.8.tar.gz
SNAPPY_SOURCE=snappy-1.1.8
SNAPPY_MD5SUM="70e48cba7fecf289153d009791c9977f"

# gperftools
GPERFTOOLS_DOWNLOAD="https://github.com/gperftools/gperftools/archive/gperftools-2.7.tar.gz"
GPERFTOOLS_NAME=gperftools-2.7.tar.gz
GPERFTOOLS_SOURCE=gperftools-gperftools-2.7
GPERFTOOLS_MD5SUM="797e7b7f6663288e2b90ab664861c61a"

# zlib
ZLIB_DOWNLOAD="https://github.com/madler/zlib/archive/refs/tags/v1.2.11.tar.gz"
ZLIB_NAME=zlib-1.2.11.tar.gz
ZLIB_SOURCE=zlib-1.2.11
ZLIB_MD5SUM="0095d2d2d1f3442ce1318336637b695f"

# lz4
LZ4_DOWNLOAD="https://github.com/lz4/lz4/archive/v1.9.3.tar.gz"
LZ4_NAME=lz4-1.9.3.tar.gz
LZ4_SOURCE=lz4-1.9.3
LZ4_MD5SUM="3a1ab1684e14fc1afc66228ce61b2db3"

# bzip
BZIP_DOWNLOAD="https://fossies.org/linux/misc/bzip2-1.0.8.tar.gz"
BZIP_NAME=bzip2-1.0.8.tar.gz
BZIP_SOURCE=bzip2-1.0.8
BZIP_MD5SUM="67e051268d0c475ea773822f7500d0e5"

# rapidjson
RAPIDJSON_DOWNLOAD="https://github.com/miloyip/rapidjson/archive/v1.1.0.tar.gz"
RAPIDJSON_NAME=rapidjson-1.1.0.tar.gz
RAPIDJSON_SOURCE=rapidjson-1.1.0
RAPIDJSON_MD5SUM="badd12c511e081fec6c89c43a7027bce"

# simdjson
SIMDJSON_DOWNLOAD="https://github.com/simdjson/simdjson/archive/refs/tags/v1.0.2.tar.gz"
SIMDJSON_NAME=simdjson-v1.0.2.tar.gz
SIMDJSON_SOURCE=simdjson-1.0.2
SIMDJSON_MD5SUM="5bb34cca7087a99c450dbdfe406bdc7d"

# curl
CURL_DOWNLOAD="https://curl.se/download/curl-7.79.0.tar.gz"
CURL_NAME=curl-7.79.0.tar.gz
CURL_SOURCE=curl-7.79.0
CURL_MD5SUM="b40e4dc4bbc9e109c330556cd58c8ec8"

# RE2
RE2_DOWNLOAD="https://github.com/google/re2/archive/2017-05-01.tar.gz"
RE2_NAME=re2-2017-05-01.tar.gz
RE2_SOURCE=re2-2017-05-01
RE2_MD5SUM="4aa65a0b22edacb7ddcd7e4aec038dcf"

# boost
BOOST_DOWNLOAD="http://sourceforge.net/projects/boost/files/boost/1.75.0/boost_1_75_0.tar.gz"
BOOST_NAME=boost_1_75_0.tar.gz
BOOST_SOURCE=boost_1_75_0
BOOST_MD5SUM="38813f6feb40387dfe90160debd71251"

# leveldb
LEVELDB_DOWNLOAD="https://github.com/google/leveldb/archive/v1.20.tar.gz"
LEVELDB_NAME=leveldb-1.20.tar.gz
LEVELDB_SOURCE=leveldb-1.20
LEVELDB_MD5SUM="298b5bddf12c675d6345784261302252"

# brpc
BRPC_DOWNLOAD="https://github.com/apache/brpc/archive/0.9.7.tar.gz"
BRPC_NAME=brpc-0.9.7.tar.gz
BRPC_SOURCE=brpc-0.9.7
BRPC_MD5SUM="98efa987b7328476e26519be73727fa1"

# rocksdb
ROCKSDB_DOWNLOAD="https://github.com/facebook/rocksdb/archive/refs/tags/v6.22.1.zip"
ROCKSDB_NAME=rocksdb-6.22.1.zip
ROCKSDB_SOURCE=rocksdb-6.22.1
ROCKSDB_MD5SUM="02727e52cdb94fa6a9dbbd68d157e619"

# librdkafka
LIBRDKAFKA_DOWNLOAD="https://github.com/edenhill/librdkafka/archive/v1.7.0.tar.gz"
LIBRDKAFKA_NAME=librdkafka-1.7.0.tar.gz
LIBRDKAFKA_SOURCE=librdkafka-1.7.0
LIBRDKAFKA_MD5SUM="fe3c45deb182bd9c644b6bc6375bffc3"

# zstd
ZSTD_DOWNLOAD="https://github.com/facebook/zstd/archive/v1.5.0.tar.gz"
ZSTD_NAME=zstd-1.5.0.tar.gz
ZSTD_SOURCE=zstd-1.5.0
ZSTD_MD5SUM="d5ac89d5df9e81243ce40d0c6a66691d"

# brotli
BROTLI_DOWNLOAD="https://github.com/google/brotli/archive/v1.0.9.tar.gz"
BROTLI_NAME="brotli-1.0.9.tar.gz"
BROTLI_SOURCE="brotli-1.0.9"
BROTLI_MD5SUM="c2274f0c7af8470ad514637c35bcee7d"

# flatbuffers
FLATBUFFERS_DOWNLOAD="https://github.com/google/flatbuffers/archive/v1.10.0.tar.gz"
FLATBUFFERS_NAME=flatbuffers-v1.10.0.tar.gz
FLATBUFFERS_SOURCE=flatbuffers-1.10.0
FLATBUFFERS_MD5SUM="f7d19a3f021d93422b0bc287d7148cd2"

# arrow
ARROW_DOWNLOAD="https://github.com/apache/arrow/archive/apache-arrow-5.0.0.tar.gz"
ARROW_NAME="arrow-apache-arrow-5.0.0.tar.gz"
ARROW_SOURCE="arrow-apache-arrow-5.0.0"
ARROW_MD5SUM="9caf5dbd36ef4972c3a591bcfeaf59c8"

# S2
S2_DOWNLOAD="https://github.com/google/s2geometry/archive/v0.9.0.tar.gz"
S2_NAME=s2geometry-0.9.0.tar.gz
S2_SOURCE=s2geometry-0.9.0
S2_MD5SUM="293552c7646193b8b4a01556808fe155"

# BITSHUFFLE
BITSHUFFLE_DOWNLOAD="https://github.com/kiyo-masui/bitshuffle/archive/0.3.5.tar.gz"
BITSHUFFLE_NAME=bitshuffle-0.3.5.tar.gz
BITSHUFFLE_SOURCE=bitshuffle-0.3.5
BITSHUFFLE_MD5SUM="2648ec7ccd0b896595c6636d926fc867"

# CROARINGBITMAP
CROARINGBITMAP_DOWNLOAD="https://github.com/RoaringBitmap/CRoaring/archive/v0.2.60.tar.gz"
CROARINGBITMAP_NAME=CRoaring-0.2.60.tar.gz
CROARINGBITMAP_SOURCE=CRoaring-0.2.60
CROARINGBITMAP_MD5SUM="29602918e6890ffdeed84cb171857046"

# jemalloc
JEMALLOC_DOWNLOAD="https://github.com/jemalloc/jemalloc/releases/download/5.2.1/jemalloc-5.2.1.tar.bz2"
JEMALLOC_NAME="jemalloc-5.2.1.tar.bz2"
JEMALLOC_SOURCE="jemalloc-5.2.1"
JEMALLOC_MD5SUM="3d41fbf006e6ebffd489bdb304d009ae"

# CCTZ
CCTZ_DOWNLOAD="https://github.com/google/cctz/archive/v2.3.tar.gz"
CCTZ_NAME="cctz-2.3.tar.gz"
CCTZ_SOURCE="cctz-2.3"
CCTZ_MD5SUM="209348e50b24dbbdec6d961059c2fc92"

# FMT
FMT_DOWNLOAD="https://github.com/fmtlib/fmt/releases/download/7.0.3/fmt-7.0.3.zip"
FMT_NAME="fmt-7.0.3.zip"
FMT_SOURCE="fmt-7.0.3"
FMT_MD5SUM="60c8803eb36a6ff81a4afde33c0f621a"

# RYU
RYU_DOWNLOAD="https://github.com/ulfjack/ryu/archive/aa31ca9361d21b1a00ee054aac49c87d07e74abc.zip"
RYU_NAME="ryu-aa31ca9361d21b1a00ee054aac49c87d07e74abc.zip"
RYU_SOURCE="ryu-aa31ca9361d21b1a00ee054aac49c87d07e74abc"
RYU_MD5SUM="cb82b6da904d919470fe3f5a01ca30ff"

# breakpad
BREAK_PAD_DOWNLOAD="https://github.com/google/breakpad/archive/d6a6f52.zip"
BREAK_PAD_NAME="breakpad-d6a6f52606529111b9f0ade9a0e0d9040fa97c1f.zip"
BREAK_PAD_SOURCE="breakpad-d6a6f52606529111b9f0ade9a0e0d9040fa97c1f"
BREAK_PAD_MD5SUM="53e8e9ee2d5e4f842a0cb4d651e74af6"

# RAGEL
# ragel-6.9+ is used by hyperscan, so we build it first
RAGEL_DOWNLOAD="https://www.colm.net/files/ragel/ragel-6.10.tar.gz"
RAGEL_NAME="ragel-6.10.tar.gz"
RAGEL_SOURCE="ragel-6.10"
RAGEL_MD5SUM="748cae8b50cffe9efcaa5acebc6abf0d"

# mariadb-connector-c
MARIADB_DOWNLOAD="https://github.com/mariadb-corporation/mariadb-connector-c/archive/refs/tags/v3.1.14.tar.gz"
MARIADB_NAME="mariadb-connector-c-3.1.14.tar.gz"
MARIADB_SOURCE="mariadb-connector-c-3.1.14"
MARIADB_MD5SUM="86c4052adeb8447900bf33b4e2ddd1f9"

# aliyun_oss_jars
ALIYUN_OSS_JARS_DOWNLOAD="http://cdn-thirdparty.starrocks.com/aliyun-oss-sdk-3.7.2.tar.gz"
ALIYUN_OSS_JARS_NAME="aliyun-oss-sdk-3.7.2.tar.gz"
ALIYUN_OSS_JARS_SOURCE="aliyun-oss-sdk-3.7.2"
ALIYUN_OSS_JARS_MD5SUM="1e37382831598f4ed049eb276b8e8b29"

AWS_SDK_CPP_DOWNLOAD="https://github.com/aws/aws-sdk-cpp/archive/refs/tags/1.9.179.tar.gz"
AWS_SDK_CPP_NAME="aws-sdk-cpp-1.9.179.tar.gz"
AWS_SDK_CPP_SOURCE="aws-sdk-cpp-1.9.179"
AWS_SDK_CPP_MD5SUM="3a4e2703eaeeded588814ee9e61a3342"

# velocypack: A fast and compact format for serialization and storage
VPACK_DOWNLOAD="https://github.com/arangodb/velocypack/archive/refs/tags/XYZ1.0.tar.gz"
VPACK_NAME="velocypack-XYZ1.0.tar.gz"
VPACK_SOURCE="velocypack-XYZ1.0"
VPACK_MD5SUM="161cbf4c347f6daadacfb749c31842f8"

# open-telemetry
OPENTELEMETRY_DOWNLOAD="https://github.com/open-telemetry/opentelemetry-cpp/archive/refs/tags/v1.2.0.tar.gz"
OPENTELEMETRY_NAME=opentelemetry-cpp-v1.2.0.tar.gz
OPENTELEMETRY_SOURCE=opentelemetry-cpp-1.2.0
OPENTELEMETRY_MD5SUM="c084abc742c6b3cd4c9c3684e559d4e1"

# all thirdparties which need to be downloaded is set in array TP_ARCHIVES
TP_ARCHIVES="LIBEVENT OPENSSL THRIFT PROTOBUF GFLAGS GLOG GTEST RAPIDJSON SIMDJSON SNAPPY GPERFTOOLS ZLIB LZ4 BZIP CURL \
            RE2 BOOST LEVELDB BRPC ROCKSDB LIBRDKAFKA FLATBUFFERS ARROW BROTLI ZSTD S2 BITSHUFFLE CROARINGBITMAP JEMALLOC \
            CCTZ FMT RYU BREAK_PAD HADOOP JDK RAGEL HYPERSCAN MARIADB ALIYUN_OSS_JARS AWS_SDK_CPP VPACK OPENTELEMETRY"
