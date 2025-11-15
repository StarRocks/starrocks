# Copyright 2021-present StarRocks, Inc. All rights reserved.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_STATIC_RUNTIME ON)

# Compile generated source if necessary
message(STATUS "build gensrc if necessary")
execute_process(COMMAND make -C ${BASE_DIR}/../gensrc/
                RESULT_VARIABLE MAKE_GENSRC_RESULT)
if(NOT ${MAKE_GENSRC_RESULT} EQUAL 0 AND NOT APPLE)
    message(FATAL_ERROR "Failed to build ${BASE_DIR}/../gensrc/")
endif()

#
set(BUILD_VERSION_CC ${CMAKE_BINARY_DIR}/build_version.cc)
configure_file(${SRC_DIR}/common/build_version.cc.in ${BUILD_VERSION_CC} @ONLY)
add_library(build_version OBJECT ${BUILD_VERSION_CC})
target_include_directories(build_version PRIVATE ${SRC_DIR}/common)

# Add common cmake prefix path and link library path
list(APPEND CMAKE_PREFIX_PATH ${THIRDPARTY_DIR}/lib/cmake)
list(APPEND CMAKE_PREFIX_PATH ${THIRDPARTY_DIR}/lib64/cmake)
link_directories(${THIRDPARTY_DIR}/lib ${THIRDPARTY_DIR}/lib64)

# Set Boost
set(Boost_DEBUG FALSE)
set(Boost_USE_MULTITHREADED ON)
set(Boost_NO_BOOST_CMAKE ON)
set(BOOST_ROOT ${THIRDPARTY_DIR})
# boost suppress warning is supported on cmake 3.20
# https://cmake.org/cmake/help/latest/module/FindBoost.html
set(Boost_NO_WARN_NEW_VERSIONS ON)

if (NOT APPLE)
    find_package(Boost 1.80.0 REQUIRED COMPONENTS thread regex program_options filesystem context iostreams)
else()
    find_package(Boost 1.80.0 COMPONENTS thread regex program_options filesystem context iostreams)
endif()
include_directories(${Boost_INCLUDE_DIRS})
message(STATUS ${Boost_LIBRARIES})

set(GPERFTOOLS_HOME "${THIRDPARTY_DIR}/gperftools")
set(JEMALLOC_HOME "${THIRDPARTY_DIR}/jemalloc")

# Set all libraries

add_library(clucene-core STATIC IMPORTED)
set_target_properties(clucene-core PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libclucene-core-static.a)

add_library(clucene-shared STATIC IMPORTED)
set_target_properties(clucene-shared PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libclucene-shared-static.a)

add_library(clucene-contribs-lib STATIC IMPORTED)
set_target_properties(clucene-contribs-lib PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libclucene-contribs-lib.a)

add_library(gflags STATIC IMPORTED GLOBAL)
set_target_properties(gflags PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgflags.a)

add_library(glog STATIC IMPORTED GLOBAL)
set_target_properties(glog PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libglog.a)

add_library(re2 STATIC IMPORTED GLOBAL)
set_target_properties(re2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libre2.a)

add_library(pprof STATIC IMPORTED)
set_target_properties(pprof PROPERTIES IMPORTED_LOCATION
    ${GPERFTOOLS_HOME}/lib/libprofiler.a)

add_library(tcmalloc STATIC IMPORTED)
set_target_properties(tcmalloc PROPERTIES IMPORTED_LOCATION
    ${GPERFTOOLS_HOME}/lib/libtcmalloc.a)

add_library(protobuf STATIC IMPORTED GLOBAL)
set_target_properties(protobuf PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libprotobuf.a)

add_library(protoc STATIC IMPORTED)
set_target_properties(protoc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libprotoc.a)

add_library(gtest STATIC IMPORTED)
set_target_properties(gtest PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgtest.a)

add_library(gmock STATIC IMPORTED)
set_target_properties(gmock PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgmock.a)

add_library(snappy STATIC IMPORTED GLOBAL)
set_target_properties(snappy PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libsnappy.a)

set(CURL_INCLUDE_DIR "${THIRDPARTY_DIR}/include" CACHE PATH "curl include path")
set(CURL_LIBRARY "${THIRDPARTY_DIR}/lib/libcurl.a" CACHE FILEPATH "curl static library")
add_library(curl STATIC IMPORTED GLOBAL)
set_target_properties(curl PROPERTIES IMPORTED_LOCATION ${CURL_LIBRARY})

add_library(lz4 STATIC IMPORTED)
set_target_properties(lz4 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/liblz4.a)

add_library(thrift STATIC IMPORTED)
set_target_properties(thrift PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libthrift.a)

add_library(thriftnb STATIC IMPORTED)
set_target_properties(thriftnb PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libthriftnb.a)

add_library(mysql STATIC IMPORTED)
set_target_properties(mysql PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/mariadb/libmariadbclient.a)

add_library(hdfs STATIC IMPORTED GLOBAL)
set_target_properties(hdfs PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libhdfs.a)

add_library(fiu STATIC IMPORTED)
set_target_properties(fiu PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libfiu.a)

add_library(icuuc STATIC IMPORTED)
set_target_properties(icuuc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libicuuc.a)

add_library(icui18n STATIC IMPORTED)
set_target_properties(icui18n PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libicui18n.a)

add_library(icudata STATIC IMPORTED)
set_target_properties(icudata PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libicudata.a)


# Allow FindOpenSSL() to find correct static libraries
set(OPENSSL_ROOT_DIR ${THIRDPARTY_DIR} CACHE STRING "root directory of an OpenSSL installation")
message(STATUS "Using OpenSSL Root Dir: ${OPENSSL_ROOT_DIR}")
add_library(crypto STATIC IMPORTED GLOBAL)
set_target_properties(crypto PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libcrypto.a)

add_library(AWS::crypto ALIAS crypto)
set(AWSSDK_ROOT_DIR ${THIRDPARTY_DIR})
set(AWSSDK_COMMON_RUNTIME_LIBS "aws-crt-cpp;aws-c-auth;aws-c-cal;aws-c-common;aws-c-compression;aws-c-event-stream;aws-c-http;aws-c-io;aws-c-mqtt;aws-c-s3;aws-checksums;s2n;aws-c-sdkutils")
foreach(lib IN ITEMS ${AWSSDK_COMMON_RUNTIME_LIBS})
    list(APPEND CMAKE_PREFIX_PATH ${THIRDPARTY_DIR}/lib/${lib}/cmake)
    list(APPEND CMAKE_PREFIX_PATH ${THIRDPARTY_DIR}/lib64/${lib}/cmake)
endforeach()
find_package(AWSSDK REQUIRED COMPONENTS core s3 s3-crt transfer identity-management sts)
include_directories(${AWSSDK_INCLUDE_DIRS})

set(Poco_ROOT_DIR ${THIRDPARTY_DIR})
find_package(Poco REQUIRED COMPONENTS Net NetSSL)
include_directories(${Poco_INCLUDE_DIRS})

add_library(libevent STATIC IMPORTED)
set_target_properties(libevent PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libevent.a)

add_library(openssl STATIC IMPORTED)
set_target_properties(openssl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libssl.a)

add_library(leveldb STATIC IMPORTED GLOBAL)
set_target_properties(leveldb PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libleveldb.a)

if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR "${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    add_library(jemalloc SHARED IMPORTED)
    set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION ${JEMALLOC_HOME}/lib-shared/libjemalloc.so)
else()
    add_library(jemalloc STATIC IMPORTED)
    set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION ${JEMALLOC_HOME}/lib-static/libjemalloc.a)
endif()

add_library(brotlicommon STATIC IMPORTED)
set_target_properties(brotlicommon PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbrotlicommon.a)

add_library(brotlidec STATIC IMPORTED)
set_target_properties(brotlidec PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbrotlidec.a)

add_library(brotlienc STATIC IMPORTED)
set_target_properties(brotlienc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbrotlienc.a)

add_library(zstd STATIC IMPORTED)
set_target_properties(zstd PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libzstd.a)

add_library(streamvbyte STATIC IMPORTED)
set_target_properties(streamvbyte PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libstreamvbyte_static.a)

add_library(arrow STATIC IMPORTED)
set_target_properties(arrow PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libarrow.a)

add_library(parquet STATIC IMPORTED)
set_target_properties(parquet PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libparquet.a)

add_library(brpc STATIC IMPORTED GLOBAL)
set_target_properties(brpc PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbrpc.a)

add_library(rocksdb STATIC IMPORTED GLOBAL)
set_target_properties(rocksdb PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/librocksdb.a)

add_library(krb5support STATIC IMPORTED)
set_target_properties(krb5support PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libkrb5support.a)

add_library(krb5 STATIC IMPORTED)
set_target_properties(krb5 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libkrb5.a)

add_library(com_err STATIC IMPORTED)
set_target_properties(com_err PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libcom_err.a)

add_library(k5crypto STATIC IMPORTED)
set_target_properties(k5crypto PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libk5crypto.a)

add_library(gssapi_krb5 STATIC IMPORTED)
set_target_properties(gssapi_krb5 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgssapi_krb5.a)

add_library(sasl STATIC IMPORTED GLOBAL)
set_target_properties(sasl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libsasl2.a)

add_library(librdkafka_cpp STATIC IMPORTED)
set_target_properties(librdkafka_cpp PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/librdkafka++.a)

add_library(librdkafka STATIC IMPORTED)
set_target_properties(librdkafka PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/librdkafka.a)

add_library(libpulsar STATIC IMPORTED)
set_target_properties(libpulsar PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libpulsar.a)

add_library(libs2 STATIC IMPORTED)
set_target_properties(libs2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libs2.a)

add_library(bitshuffle STATIC IMPORTED)
set_target_properties(bitshuffle PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libbitshuffle.a)

add_library(roaring STATIC IMPORTED)
set_target_properties(roaring PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libroaring.a)

add_library(cctz STATIC IMPORTED)
set_target_properties(cctz PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libcctz.a)

add_library(benchmark STATIC IMPORTED)
set_target_properties(benchmark PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbenchmark.a)

add_library(benchmark_main STATIC IMPORTED)
set_target_properties(benchmark_main PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libbenchmark_main.a)

if (ENABLE_MULTI_DYNAMIC_LIBS)
    add_library(fmt SHARED IMPORTED)
    set_target_properties(fmt PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libfmt.so.8)
    file(GLOB FMT_SO_FILES "${THIRDPARTY_DIR}/lib64/libfmt.so*")
    install(FILES ${FMT_SO_FILES} DESTINATION ${OUTPUT_DIR}/lib)
else()
    add_library(fmt STATIC IMPORTED)
    set_target_properties(fmt PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libfmt.a)
endif()

add_library(ryu STATIC IMPORTED)
set_target_properties(ryu PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libryu.a)

add_library(libz STATIC IMPORTED GLOBAL)
set_target_properties(libz PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libz.a)

add_library(libbz2 STATIC IMPORTED)
set_target_properties(libbz2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libbz2.a)

# Disable minidump on aarch64, hence breakpad is not needed.
if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    add_library(breakpad STATIC IMPORTED)
    set_target_properties(breakpad PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libbreakpad_client.a)
endif()

add_library(hyperscan STATIC IMPORTED)
set_target_properties(hyperscan PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libhs.a)

add_library(simdjson STATIC IMPORTED)
set_target_properties(simdjson PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libsimdjson.a)

add_library(simdutf STATIC IMPORTED)
set_target_properties(simdutf PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libsimdutf.a)

add_library(velocypack STATIC IMPORTED)
set_target_properties(velocypack PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libvelocypack.a)

find_program(THRIFT_COMPILER thrift ${CMAKE_SOURCE_DIR}/bin)

add_library(http_client_curl STATIC IMPORTED GLOBAL)
set_target_properties(http_client_curl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libhttp_client_curl.a)

add_library(opentelemetry_common STATIC IMPORTED GLOBAL)
set_target_properties(opentelemetry_common PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_common.a)

add_library(opentelemetry_trace STATIC IMPORTED GLOBAL)
set_target_properties(opentelemetry_trace PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_trace.a)

add_library(opentelemetry_resources STATIC IMPORTED GLOBAL)
set_target_properties(opentelemetry_resources PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_resources.a)

add_library(jansson STATIC IMPORTED GLOBAL)
set_target_properties(jansson PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libjansson.a)

add_library(avro STATIC IMPORTED GLOBAL)
set_target_properties(avro PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libavro.a)

add_library(avrocpp STATIC IMPORTED GLOBAL)
set_target_properties(avrocpp PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libavrocpp_s.a)

add_library(serdes STATIC IMPORTED GLOBAL)
set_target_properties(serdes PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libserdes.a)

add_library(opentelemetry_exporter_jaeger_trace STATIC IMPORTED GLOBAL)
set_target_properties(opentelemetry_exporter_jaeger_trace PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libopentelemetry_exporter_jaeger_trace.a)

add_library(libxml2 STATIC IMPORTED GLOBAL)
set_target_properties(libxml2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libxml2.a)

add_library(azure-core STATIC IMPORTED GLOBAL)
set_target_properties(azure-core PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libazure-core.a)

add_library(azure-identity STATIC IMPORTED GLOBAL)
set_target_properties(azure-identity PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libazure-identity.a)

add_library(azure-storage-common STATIC IMPORTED GLOBAL)
set_target_properties(azure-storage-common PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libazure-storage-common.a)

add_library(azure-storage-blobs STATIC IMPORTED GLOBAL)
set_target_properties(azure-storage-blobs PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libazure-storage-blobs.a)

add_library(azure-storage-files-datalake STATIC IMPORTED GLOBAL)
set_target_properties(azure-storage-files-datalake PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libazure-storage-files-datalake.a)

set(absl_DIR "${THIRDPARTY_DIR}/lib/cmake/absl" CACHE PATH "absl search path" FORCE)
find_package(absl CONFIG REQUIRED)
set(gRPC_DIR "${THIRDPARTY_DIR}/lib/cmake/grpc" CACHE PATH "grpc search path")
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")
get_target_property(gRPC_INCLUDE_DIR gRPC::grpc INTERFACE_INCLUDE_DIRECTORIES)
include_directories(SYSTEM ${gRPC_INCLUDE_DIR})
add_library(protobuf::libprotobuf ALIAS protobuf)
add_library(ZLIB::ZLIB ALIAS libz)

# Disable libdeflate on aarch64
if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    add_library(libdeflate STATIC IMPORTED GLOBAL)
    set_target_properties(libdeflate PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libdeflate.a)
endif()

if (${WITH_TENANN} STREQUAL "ON")
    add_library(tenann STATIC IMPORTED GLOBAL)
    if(${USE_AVX2})
        set_target_properties(tenann PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libtenann-bundle-avx2.a)
    else()
        set_target_properties(tenann PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libtenann-bundle.a)
    endif()
endif()

set(JAVA_HOME ${THIRDPARTY_DIR}/open_jdk/)
add_library(jvm SHARED IMPORTED)
FILE(GLOB_RECURSE LIB_JVM ${JAVA_HOME}/lib/*/libjvm.so)
set_target_properties(jvm PROPERTIES IMPORTED_LOCATION ${LIB_JVM})
include_directories(${JAVA_HOME}/include)
include_directories(${JAVA_HOME}/include/linux)
