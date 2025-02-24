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
# support macos
if [[ $(uname) == "Darwin" ]]; then
    default_parallel=$[$(sysctl -n hw.physicalcpu)/4+1]
else
    default_parallel=$[$(nproc)/4+1]
fi

# use the value if $PARALEL is already set, otherwise use $default_parallel
PARALLEL=${PARALLEL:-$default_parallel}

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
# handle mac m1 platform, change arm64 to aarch64
if [[ "${MACHINE_TYPE}" == "arm64" ]]; then
    MACHINE_TYPE="aarch64"
fi

VARS_TARGET=vars-${MACHINE_TYPE}.sh

if [ ! -f ${TP_DIR}/${VARS_TARGET} ]; then
    echo "${VARS_TARGET} is missing".
    exit 1
fi
. ${TP_DIR}/${VARS_TARGET}

if [ -f /etc/lsb-release ]; then
    source /etc/lsb-release
    if [[ $DISTRIB_ID = "Ubuntu" && $DISTRIB_RELEASE =~ 22.* && -f ${TP_DIR}/vars-ubuntu22-${MACHINE_TYPE}.sh ]]; then
        . ${TP_DIR}/vars-ubuntu22-${MACHINE_TYPE}.sh
    fi
fi

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
THRIFT_DOWNLOAD="http://archive.apache.org/dist/thrift/0.20.0/thrift-0.20.0.tar.gz"
THRIFT_NAME=thrift-0.20.0.tar.gz
THRIFT_SOURCE=thrift-0.20.0
THRIFT_MD5SUM="aadebde599e1f5235acd3c730721b873"

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
GLOG_DOWNLOAD="https://github.com/google/glog/archive/v0.7.1.tar.gz"
GLOG_NAME=glog-0.7.1.tar.gz
GLOG_SOURCE=glog-0.7.1
GLOG_MD5SUM="128e2995cc33d794ff24f785a3060346"

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
LZ4_DOWNLOAD="https://github.com/lz4/lz4/archive/v1.9.4.tar.gz"
LZ4_NAME=lz4-1.9.4.tar.gz
LZ4_SOURCE=lz4-1.9.4
LZ4_MD5SUM="e9286adb64040071c5e23498bf753261"

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
SIMDJSON_DOWNLOAD="https://github.com/simdjson/simdjson/archive/refs/tags/v3.9.4.tar.gz"
SIMDJSON_NAME=simdjson-v3.9.4.tar.gz
SIMDJSON_SOURCE=simdjson-3.9.4
SIMDJSON_MD5SUM="bdc1dfcb2a89dc0c09e8370808a946f5"

# curl
CURL_DOWNLOAD="https://curl.se/download/curl-8.4.0.tar.gz"
CURL_NAME=curl-8.4.0.tar.gz
CURL_SOURCE=curl-8.4.0
CURL_MD5SUM="533e8a3b1228d5945a6a512537bea4c7"

# RE2
RE2_DOWNLOAD="https://github.com/google/re2/archive/refs/tags/2022-12-01.tar.gz"
RE2_NAME=re2-2022-12-01.tar.gz
RE2_SOURCE=re2-2022-12-01
RE2_MD5SUM="f25d7b06a3e7747ecbb2f12d48be61cd"

# boost
BOOST_DOWNLOAD="https://archives.boost.io/release/1.80.0/source/boost_1_80_0.tar.gz"
BOOST_NAME=boost_1_80_0.tar.gz
BOOST_SOURCE=boost_1_80_0
BOOST_MD5SUM="077f074743ea7b0cb49c6ed43953ae95"

# leveldb
LEVELDB_DOWNLOAD="https://github.com/google/leveldb/archive/v1.20.tar.gz"
LEVELDB_NAME=leveldb-1.20.tar.gz
LEVELDB_SOURCE=leveldb-1.20
LEVELDB_MD5SUM="298b5bddf12c675d6345784261302252"

# brpc
BRPC_DOWNLOAD="https://github.com/apache/brpc/archive/refs/tags/1.9.0.tar.gz"
BRPC_NAME=brpc-1.9.0.tar.gz
BRPC_SOURCE=brpc-1.9.0
BRPC_MD5SUM="a2b626d96a5b017f2a6701ffa594530c"

# rocksdb
ROCKSDB_DOWNLOAD="https://github.com/facebook/rocksdb/archive/refs/tags/v6.22.1.zip"
ROCKSDB_NAME=rocksdb-6.22.1.zip
ROCKSDB_SOURCE=rocksdb-6.22.1
ROCKSDB_MD5SUM="02727e52cdb94fa6a9dbbd68d157e619"

# libsasl
SASL_DOWNLOAD="https://github.com/cyrusimap/cyrus-sasl/archive/refs/tags/cyrus-sasl-2.1.28.tar.gz"
SASL_NAME=cyrus-sasl-2.1.28.tar.gz
SASL_SOURCE=cyrus-sasl-2.1.28
SASL_MD5SUM="7dcf3919b3085a1d09576438171bda91"

# kerberos MIT
KRB5_DOWNLOAD="https://kerberos.org/dist/krb5/1.19/krb5-1.19.4.tar.gz"
KRB5_NAME=krb5-1.19.4.tar.gz
KRB5_SOURCE=krb5-1.19.4
KRB5_MD5SUM="ef76083e58f8c49066180642d7c2814a"

# librdkafka
LIBRDKAFKA_DOWNLOAD="https://github.com/confluentinc/librdkafka/archive/refs/tags/v2.0.2.tar.gz"
LIBRDKAFKA_NAME=librdkafka-2.0.2.tar.gz
LIBRDKAFKA_SOURCE=librdkafka-2.0.2
LIBRDKAFKA_MD5SUM="c0120dc32acc129bfb4656fe17568da1"

# pulsar
PULSAR_DOWNLOAD="https://github.com/apache/pulsar-client-cpp/archive/refs/tags/v3.3.0.tar.gz"
PULSAR_NAME=pulsar-client-3.3.0.tar.gz
PULSAR_SOURCE=pulsar-client-cpp-3.3.0
PULSAR_MD5SUM="348b7e5ec39e50547668520d13a417a1"

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
ARROW_DOWNLOAD="https://github.com/apache/arrow/archive/refs/tags/apache-arrow-16.1.0.tar.gz"
ARROW_NAME="arrow-apache-arrow-16.1.0.tar.gz"
ARROW_SOURCE="arrow-apache-arrow-16.1.0"
ARROW_MD5SUM="d9d019aaef586bd1a7493416d78954b9"

# S2
S2_DOWNLOAD="https://github.com/google/s2geometry/archive/v0.9.0.tar.gz"
S2_NAME=s2geometry-0.9.0.tar.gz
S2_SOURCE=s2geometry-0.9.0
S2_MD5SUM="293552c7646193b8b4a01556808fe155"

# BITSHUFFLE
BITSHUFFLE_DOWNLOAD="https://github.com/kiyo-masui/bitshuffle/archive/0.5.1.tar.gz"
BITSHUFFLE_NAME=bitshuffle-0.5.1.tar.gz
BITSHUFFLE_SOURCE=bitshuffle-0.5.1
BITSHUFFLE_MD5SUM="b3bf6a9838927f7eb62214981c138e2f"

# CROARINGBITMAP
CROARINGBITMAP_DOWNLOAD="https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v4.2.1.tar.gz"
CROARINGBITMAP_NAME=CRoaring-4.2.1.tar.gz
CROARINGBITMAP_SOURCE=CRoaring-4.2.1
CROARINGBITMAP_MD5SUM="00667266a60709978368cf867fb3a3aa"

# jemalloc
JEMALLOC_DOWNLOAD="https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2"
JEMALLOC_NAME="jemalloc-5.3.0.tar.bz2"
JEMALLOC_SOURCE="jemalloc-5.3.0"
JEMALLOC_MD5SUM="09a8328574dab22a7df848eae6dbbf53"

# CCTZ
CCTZ_DOWNLOAD="https://github.com/google/cctz/archive/v2.3.tar.gz"
CCTZ_NAME="cctz-2.3.tar.gz"
CCTZ_SOURCE="cctz-2.3"
CCTZ_MD5SUM="209348e50b24dbbdec6d961059c2fc92"

# FMT
FMT_DOWNLOAD="https://github.com/fmtlib/fmt/releases/download/8.1.1/fmt-8.1.1.zip"
FMT_NAME="fmt-8.1.1.zip"
FMT_SOURCE="fmt-8.1.1"
FMT_MD5SUM="16dcd48ecc166f10162450bb28aabc87"

# RYU
RYU_DOWNLOAD="https://github.com/ulfjack/ryu/archive/aa31ca9361d21b1a00ee054aac49c87d07e74abc.zip"
RYU_NAME="ryu-aa31ca9361d21b1a00ee054aac49c87d07e74abc.zip"
RYU_SOURCE="ryu-aa31ca9361d21b1a00ee054aac49c87d07e74abc"
RYU_MD5SUM="cb82b6da904d919470fe3f5a01ca30ff"

# breakpad
BREAK_PAD_DOWNLOAD="https://github.com/google/breakpad/archive/refs/tags/v2022.07.12.tar.gz"
BREAK_PAD_NAME="breakpad-2022.07.12.tar.gz"
BREAK_PAD_SOURCE="breakpad-2022.07.12"
BREAK_PAD_MD5SUM="d5bcfd3f7b361ef5bda96123c3abdd0a"

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

# Google Cloud Storage, gcs-connector
GCS_CONNECTOR_DOWNLOAD="https://cdn-thirdparty.starrocks.com/gcs-connector-hadoop3-2.2.11-shaded.zip"
GCS_CONNECTOR_NAME="gcs-connector-hadoop3-2.2.11-shaded.zip"
GCS_CONNECTOR_SOURCE="gcs-connector-hadoop3-2.2.11-shaded"
GCS_CONNECTOR_MD5SUM="51fd0eb5cb913a84e4ad8a5ed2069e21"

# aws-sdk-cpp
AWS_SDK_CPP_DOWNLOAD="https://github.com/aws/aws-sdk-cpp/archive/refs/tags/1.11.267.tar.gz"
AWS_SDK_CPP_NAME="aws-sdk-cpp-1.11.267.tar.gz"
AWS_SDK_CPP_SOURCE="aws-sdk-cpp-1.11.267"
AWS_SDK_CPP_MD5SUM="fdf43e7262f9d08968eb34f9ad18b8e7"

# poco
POCO_DOWNLOAD="https://github.com/pocoproject/poco/archive/refs/tags/poco-1.12.5-release.tar.gz"
POCO_NAME="poco-1.12.5-release.tar.gz"
POCO_SOURCE="poco-1.12.5-release"
POCO_MD5SUM="282e54a68911f516b15d07136c78592b"

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

# benchmark
BENCHMARK_DOWNLOAD="https://github.com/google/benchmark/archive/refs/tags/v1.5.5.tar.gz"
BENCHMARK_NAME=google_benchmark-1.5.5.tar.gz
BENCHMARK_SOURCE=benchmark-1.5.5
BENCHMARK_MD5SUM="6f852815d48db788f5bb87e2e561dc5e"

# fast-float
FAST_FLOAT_DOWNLOAD="https://github.com/fastfloat/fast_float/archive/refs/tags/v3.5.1.tar.gz"
FAST_FLOAT_NAME="fast-float-3.5.1.tar.gz"
FAST_FLOAT_SOURCE="fast-float-3.5.1"
FAST_FLOAT_MD5SUM="adb3789b99f47e0cd971b4d90727d4d0"

# streamvbyte
STREAMVBYTE_DOWNLOAD="https://github.com/lemire/streamvbyte/archive/refs/tags/v0.5.1.tar.gz"
STREAMVBYTE_NAME="streamvbyte-0.5.1.tar.gz"
STREAMVBYTE_SOURCE="streamvbyte-0.5.1"
STREAMVBYTE_MD5SUM="251d9200d27dda9120653b4928a23a86"

# jansson
JANSSON_DOWNLOAD="https://github.com/akheron/jansson/releases/download/v2.14/jansson-2.14.tar.gz"
JANSSON_NAME="jansson-2.14.tar.gz"
JANSSON_SOURCE="jansson-2.14"
JANSSON_MD5SUM="6cbfc54c2ab3b4d7284e188e185c2b0b"

# avro
AVRO_DOWNLOAD="https://github.com/apache/avro/archive/refs/tags/release-1.10.2.tar.gz"
AVRO_NAME="avro-release-1.10.2.tar.gz"
AVRO_SOURCE="avro-release-1.10.2"
AVRO_MD5SUM="55b9c200976366fd62f1201231f3a5eb"

# serdes
SERDES_DOWNLOAD="https://github.com/confluentinc/libserdes/archive/refs/tags/v7.3.1.tar.gz"
SERDES_NAME="libserdes-7.3.1.tar.gz"
SERDES_SOURCE="libserdes-7.3.1"
SERDES_MD5SUM="61012487a8845f37540710ac4ac2f7ab"

# lzo
LZO2_DOWNLOAD="http://www.oberhumer.com/opensource/lzo/download/lzo-2.10.tar.gz"
LZO2_NAME=lzo-2.10.tar.gz
LZO2_SOURCE=lzo-2.10
LZO2_MD5SUM="39d3f3f9c55c87b1e5d6888e1420f4b5"

# datasketches-cpp
DATASKETCHES_DOWNLOAD="https://github.com/apache/datasketches-cpp/archive/refs/tags/4.0.0.tar.gz"
DATASKETCHES_NAME="datasketches-cpp-4.0.0.tar.gz"
DATASKETCHES_SOURCE="datasketches-cpp-4.0.0"
DATASKETCHES_MD5SUM="724cd1df9735de2b8939d298f0d95ea2"

# libfiu
FIU_DOWNLOAD="https://blitiri.com.ar/p/libfiu/files/1.1/libfiu-1.1.tar.gz"
FIU_NAME="libfiu-1.1.tar.gz"
FIU_SOURCE="libfiu-1.1"
FIU_MD5SUM="51092dcb7801efb511b7b962388d9ff4"

# libdeflate
LIBDEFLATE_DOWNLOAD="https://github.com/ebiggers/libdeflate/archive/refs/tags/v1.18.zip"
LIBDEFLATE_NAME="libdeflate-1.18.zip"
LIBDEFLATE_SOURCE="libdeflate-1.18"
LIBDEFLATE_MD5SUM="1ec42dfe7d777929ade295281560d750"

# llvm
LLVM_DOWNLOAD="https://github.com/llvm/llvm-project/releases/download/llvmorg-16.0.6/llvm-project-16.0.6.src.tar.xz"
LLVM_NAME="llvm-project-16.0.6.src.tar.xz"
LLVM_SOURCE="llvm-project-16.0.6.src"
LLVM_MD5SUM="dc13938a604f70379d3b38d09031de98"

#clucene
CLUCENE_DOWNLOAD="https://github.com/StarRocks/clucene/archive/refs/tags/starrocks-2024.06.03.tar.gz"
CLUCENE_NAME="starrocks-clucene-2024.06.03.tar.gz"
CLUCENE_SOURCE="starrocks-clucene-2024.06.03"
CLUCENE_MD5SUM="c218eb0fbbfe7f295e81ab1c8a9317cb"


#absl
ABSL_DOWNLOAD="https://github.com/abseil/abseil-cpp/archive/refs/tags/20220623.0.tar.gz"
ABSL_NAME="abseil-cpp-20220623.0.tar.gz"
ABSL_SOURCE="abseil-cpp-20220623.0"
ABSL_MD5SUM="955b6faedf32ec2ce1b7725561d15618"

# cares - grpc dependency
CARES_DOWNLOAD="https://github.com/c-ares/c-ares/archive/tags/cares-1_19_1.tar.gz"
CARES_NAME=cares-1_19_1.tar.gz
CARES_SOURCE=cares-1_19_1
CARES_MD5SUM="ae2177836c9dbbacb8f303d167fe700f"

# grpc
GRPC_DOWNLOAD="https://github.com/grpc/grpc/archive/refs/tags/v1.43.0.tar.gz"
GRPC_NAME="grpc-1.43.0.tar.gz"
GRPC_SOURCE="grpc-1.43.0"
GRPC_MD5SUM="92559743e7b5d3f67486c4c0de2f5cbe"

# simdutf
SIMDUTF_DOWNLOAD="https://github.com/simdutf/simdutf/archive/refs/tags/v5.2.8.tar.gz"
SIMDUTF_NAME="simdutf-5.2.8.tar.gz"
SIMDUTF_SOURCE="simdutf-5.2.8"
SIMDUTF_MD5SUM="731c78ab5a10c6073942dc93d5c4b04c"

# tenann
TENANN_DOWNLOAD="https://github.com/StarRocks/tenann/releases/download/v0.4.2-RELEASE/tenann-v0.4.2-RELEASE.tar.gz"
TENANN_NAME="tenann-v0.4.2-RELEASE.tar.gz"
TENANN_SOURCE="tenann-v0.4.2-RELEASE"
TENANN_MD5SUM="40a00643d953982845901ae60766aad4"

# icu
ICU_DOWNLOAD="https://github.com/unicode-org/icu/releases/download/release-76-1/icu4c-76_1-src.zip"
ICU_NAME="icu4c-76_1-src.zip"
ICU_SOURCE="icu"
ICU_MD5SUM="f5f5c827d94af8445766c7023aca7f6b"

# all thirdparties which need to be downloaded is set in array TP_ARCHIVES
TP_ARCHIVES="CLUCENE LIBEVENT OPENSSL THRIFT PROTOBUF GFLAGS GLOG GTEST RAPIDJSON SIMDJSON SNAPPY GPERFTOOLS ZLIB LZ4 BZIP CURL \
            RE2 BOOST LEVELDB BRPC ROCKSDB KRB5 SASL LIBRDKAFKA PULSAR FLATBUFFERS ARROW BROTLI ZSTD S2 BITSHUFFLE CROARINGBITMAP \
            JEMALLOC CCTZ FMT RYU BREAK_PAD HADOOP JDK RAGEL HYPERSCAN MARIADB JINDOSDK AWS_SDK_CPP VPACK OPENTELEMETRY \
            BENCHMARK FAST_FLOAT STARCACHE STREAMVBYTE JANSSON AVRO SERDES GCS_CONNECTOR LZO2 DATASKETCHES \
            ASYNC_PROFILER FIU LIBDEFLATE LLVM ABSL CARES GRPC SIMDUTF TENANN POCO ICU"