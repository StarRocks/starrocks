#!/usr/bin/env bash
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

starrocks_set_default_packages() {
    local machine_type="$1"

    STARROCKS_THIRDPARTY_ALL_PACKAGES=(
        libevent
        zlib
        lz4
        lzo2
        bzip
        openssl
        boost
        protobuf
        gflags
        gtest
        glog
        rapidjson
        simdjson
        snappy
        gperftools
        curl
        re2
        thrift
        leveldb
        brpc
        rocksdb
        kerberos
        sasl
        absl
        grpc
        flatbuffers
        jemalloc
        brotli
        arrow
        librdkafka
        pulsar
        s2
        bitshuffle
        croaringbitmap
        cctz
        fmt
        fmt_shared
        ryu
        hadoop_src
        jdk
        ragel
        hyperscan
        mariadb
        aliyun_jindosdk
        gcs_connector
        aws_cpp_sdk
        vpack
        opentelemetry
        benchmark
        fast_float
        starcache
        streamvbyte
        jansson
        avro_c
        avro_cpp
        serdes
        datasketches
        fiu
        llvm
        clucene
        simdutf
        poco
        icu
        xsimd
        libxml2
        azure
        libdivide
        flamegraph
        tenann
        xxhash
        pprof
        benchgen
    )

    if [[ "${machine_type}" != "aarch64" ]]; then
        STARROCKS_THIRDPARTY_ALL_PACKAGES+=(breakpad libdeflate)
    fi
}
