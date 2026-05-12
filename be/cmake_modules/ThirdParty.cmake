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

set(STARROCKS_PROTOC_EXECUTABLE "${THIRDPARTY_DIR}/bin/protoc")
if(NOT EXISTS "${STARROCKS_PROTOC_EXECUTABLE}")
    find_program(STARROCKS_PROTOC_EXECUTABLE NAMES protoc REQUIRED)
endif()

set(STARROCKS_THRIFT_EXECUTABLE "${THIRDPARTY_DIR}/bin/thrift")
if(NOT EXISTS "${STARROCKS_THRIFT_EXECUTABLE}")
    find_program(STARROCKS_THRIFT_EXECUTABLE NAMES thrift REQUIRED)
endif()

set(THRIFT_COMPILER "${STARROCKS_THRIFT_EXECUTABLE}")

message(STATUS "Using protoc compiler: ${STARROCKS_PROTOC_EXECUTABLE}")
message(STATUS "Using thrift compiler: ${STARROCKS_THRIFT_EXECUTABLE}")

#
set(BUILD_VERSION_CC ${CMAKE_BINARY_DIR}/build_version.cc)
configure_file(${SRC_DIR}/common/build_version.cc.in ${BUILD_VERSION_CC} @ONLY)
set(BUILD_VERSION_CPP ${GENSRC_DIR}/gen_cpp/version.cpp)
set_source_files_properties(${BUILD_VERSION_CPP} PROPERTIES GENERATED TRUE)
add_library(build_version OBJECT ${BUILD_VERSION_CC} ${BUILD_VERSION_CPP})
target_include_directories(build_version PRIVATE ${SRC_DIR}/common)

# Add common cmake prefix path and link library path
list(APPEND CMAKE_PREFIX_PATH ${THIRDPARTY_DIR}/lib/cmake)
list(APPEND CMAKE_PREFIX_PATH ${THIRDPARTY_DIR}/lib64/cmake)
link_directories(${THIRDPARTY_DIR}/lib ${THIRDPARTY_DIR}/lib64)

function(starrocks_resolve_thirdparty_library out_var file_name)
    if (APPLE)
        set(search_dirs "${THIRDPARTY_DIR}/lib" "${THIRDPARTY_DIR}/lib64")
    else()
        set(search_dirs "${THIRDPARTY_DIR}/lib64" "${THIRDPARTY_DIR}/lib")
    endif()

    foreach(search_dir IN LISTS search_dirs)
        if (EXISTS "${search_dir}/${file_name}")
            set(${out_var} "${search_dir}/${file_name}" PARENT_SCOPE)
            return()
        endif()
    endforeach()

    list(GET search_dirs 0 default_search_dir)
    set(${out_var} "${default_search_dir}/${file_name}" PARENT_SCOPE)
endfunction()

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
    set(BOOST_APPLE_LIBRARIES "")
    foreach(BOOST_LIBRARY IN LISTS Boost_LIBRARIES)
        get_filename_component(BOOST_LIBRARY_NAME "${BOOST_LIBRARY}" NAME)
        if (BOOST_LIBRARY_NAME MATCHES "^libboost_.*\\.(a|dylib)$")
            starrocks_resolve_thirdparty_library(BOOST_LIBRARY_RESOLVED "${BOOST_LIBRARY_NAME}")
            list(APPEND BOOST_APPLE_LIBRARIES "${BOOST_LIBRARY_RESOLVED}")
        else()
            list(APPEND BOOST_APPLE_LIBRARIES "${BOOST_LIBRARY}")
        endif()
    endforeach()
    set(Boost_LIBRARIES "${BOOST_APPLE_LIBRARIES}")
endif()
include_directories(${Boost_INCLUDE_DIRS})
message(STATUS ${Boost_LIBRARIES})

set(GPERFTOOLS_HOME "${THIRDPARTY_DIR}/gperftools")
set(JEMALLOC_HOME "${THIRDPARTY_DIR}/jemalloc")

# Set all libraries

starrocks_resolve_thirdparty_library(CLUCENE_CORE_LIBRARY libclucene-core-static.a)
add_library(clucene-core STATIC IMPORTED)
set_target_properties(clucene-core PROPERTIES IMPORTED_LOCATION ${CLUCENE_CORE_LIBRARY})

starrocks_resolve_thirdparty_library(CLUCENE_SHARED_LIBRARY libclucene-shared-static.a)
add_library(clucene-shared STATIC IMPORTED)
set_target_properties(clucene-shared PROPERTIES IMPORTED_LOCATION ${CLUCENE_SHARED_LIBRARY})

starrocks_resolve_thirdparty_library(CLUCENE_CONTRIBS_LIBRARY libclucene-contribs-lib.a)
add_library(clucene-contribs-lib STATIC IMPORTED)
set_target_properties(clucene-contribs-lib PROPERTIES IMPORTED_LOCATION ${CLUCENE_CONTRIBS_LIBRARY})

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


# Allow FindOpenSSL() to find correct static libraries from the prepared thirdparty root.
set(OPENSSL_ROOT_DIR ${THIRDPARTY_DIR} CACHE PATH "root directory of an OpenSSL installation" FORCE)
set(OPENSSL_USE_STATIC_LIBS TRUE CACHE BOOL "Prefer static OpenSSL from the prepared thirdparty root" FORCE)
if (EXISTS "${THIRDPARTY_DIR}/include/openssl/ssl.h")
    set(OPENSSL_INCLUDE_DIR "${THIRDPARTY_DIR}/include" CACHE PATH "OpenSSL include directory" FORCE)
endif()
if (EXISTS "${THIRDPARTY_DIR}/lib/libcrypto.a")
    set(OPENSSL_CRYPTO_LIBRARY "${THIRDPARTY_DIR}/lib/libcrypto.a" CACHE FILEPATH "OpenSSL crypto library" FORCE)
endif()
if (EXISTS "${THIRDPARTY_DIR}/lib/libssl.a")
    set(OPENSSL_SSL_LIBRARY "${THIRDPARTY_DIR}/lib/libssl.a" CACHE FILEPATH "OpenSSL ssl library" FORCE)
endif()
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

set(Poco_ROOT "${THIRDPARTY_DIR}" CACHE PATH "Poco search path" FORCE)
set(Poco_ROOT_DIR "${THIRDPARTY_DIR}" CACHE PATH "Poco search path" FORCE)
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
    if (APPLE)
        file(GLOB JEMALLOC_SHARED_LIBS
             "${JEMALLOC_HOME}/lib-shared/libjemalloc*.dylib"
             "${JEMALLOC_HOME}/lib-shared/libjemalloc*.dylib.*")
        list(LENGTH JEMALLOC_SHARED_LIBS JEMALLOC_SHARED_LIBS_COUNT)
        if (JEMALLOC_SHARED_LIBS_COUNT EQUAL 0)
            message(FATAL_ERROR "jemalloc shared library not found under ${JEMALLOC_HOME}/lib-shared")
        endif()
        list(GET JEMALLOC_SHARED_LIBS 0 JEMALLOC_SHARED_LIB)
    else()
        set(JEMALLOC_SHARED_LIB "${JEMALLOC_HOME}/lib-shared/libjemalloc.so")
    endif()
    set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION "${JEMALLOC_SHARED_LIB}")
else()
    add_library(jemalloc STATIC IMPORTED)
    set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION ${JEMALLOC_HOME}/lib-static/libjemalloc.a)
endif()

starrocks_resolve_thirdparty_library(BROTLICOMMON_LIBRARY libbrotlicommon.a)
add_library(brotlicommon STATIC IMPORTED)
set_target_properties(brotlicommon PROPERTIES IMPORTED_LOCATION ${BROTLICOMMON_LIBRARY})

starrocks_resolve_thirdparty_library(BROTLIDEC_LIBRARY libbrotlidec.a)
add_library(brotlidec STATIC IMPORTED)
set_target_properties(brotlidec PROPERTIES IMPORTED_LOCATION ${BROTLIDEC_LIBRARY})

starrocks_resolve_thirdparty_library(BROTLIENC_LIBRARY libbrotlienc.a)
add_library(brotlienc STATIC IMPORTED)
set_target_properties(brotlienc PROPERTIES IMPORTED_LOCATION ${BROTLIENC_LIBRARY})

starrocks_resolve_thirdparty_library(ZSTD_LIBRARY libzstd.a)
add_library(zstd STATIC IMPORTED)
set_target_properties(zstd PROPERTIES IMPORTED_LOCATION ${ZSTD_LIBRARY})

add_library(streamvbyte STATIC IMPORTED)
set_target_properties(streamvbyte PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libstreamvbyte_static.a)

starrocks_resolve_thirdparty_library(ARROW_LIBRARY libarrow.a)
add_library(arrow STATIC IMPORTED)
set_target_properties(arrow PROPERTIES IMPORTED_LOCATION ${ARROW_LIBRARY})

starrocks_resolve_thirdparty_library(PARQUET_LIBRARY libparquet.a)
add_library(parquet STATIC IMPORTED)
set_target_properties(parquet PROPERTIES IMPORTED_LOCATION ${PARQUET_LIBRARY})

starrocks_resolve_thirdparty_library(BRPC_LIBRARY libbrpc.a)
add_library(brpc STATIC IMPORTED GLOBAL)
set_target_properties(brpc PROPERTIES IMPORTED_LOCATION ${BRPC_LIBRARY})

add_library(rocksdb STATIC IMPORTED GLOBAL)
set_target_properties(rocksdb PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/librocksdb.a)

if (APPLE)
    add_library(krb5support SHARED IMPORTED)
    set_target_properties(krb5support PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libkrb5support.dylib)

    add_library(krb5 SHARED IMPORTED)
    set_target_properties(krb5 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libkrb5.dylib)

    add_library(com_err SHARED IMPORTED)
    set_target_properties(com_err PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libcom_err.dylib)

    add_library(k5crypto SHARED IMPORTED)
    set_target_properties(k5crypto PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libk5crypto.dylib)

    add_library(gssapi_krb5 SHARED IMPORTED)
    set_target_properties(gssapi_krb5 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libgssapi_krb5.dylib)

    add_library(sasl SHARED IMPORTED GLOBAL)
    set_target_properties(sasl PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libsasl2.dylib)
else()
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
endif()

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

starrocks_resolve_thirdparty_library(BENCHMARK_LIBRARY libbenchmark.a)
add_library(benchmark STATIC IMPORTED)
set_target_properties(benchmark PROPERTIES IMPORTED_LOCATION ${BENCHMARK_LIBRARY})

starrocks_resolve_thirdparty_library(BENCHMARK_MAIN_LIBRARY libbenchmark_main.a)
add_library(benchmark_main STATIC IMPORTED)
set_target_properties(benchmark_main PROPERTIES IMPORTED_LOCATION ${BENCHMARK_MAIN_LIBRARY})

if (ENABLE_MULTI_DYNAMIC_LIBS)
    add_library(fmt SHARED IMPORTED)
    if (APPLE)
        if (EXISTS "${THIRDPARTY_DIR}/lib64/libfmt.dylib" OR EXISTS "${THIRDPARTY_DIR}/lib64/libfmt.12.dylib")
            set(FMT_SHARED_DIR "${THIRDPARTY_DIR}/lib64")
        else()
            set(FMT_SHARED_DIR "${THIRDPARTY_DIR}/lib")
        endif()
        find_library(FMT_SHARED_LIBRARY NAMES fmt PATHS ${FMT_SHARED_DIR} NO_DEFAULT_PATH)
        if (NOT FMT_SHARED_LIBRARY)
            message(FATAL_ERROR "fmt shared library not found under ${FMT_SHARED_DIR}")
        endif()
        set_target_properties(fmt PROPERTIES IMPORTED_LOCATION ${FMT_SHARED_LIBRARY})
        file(GLOB FMT_SHARED_FILES "${FMT_SHARED_DIR}/libfmt*.dylib")
    else()
        set_target_properties(fmt PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib64/libfmt.so.8)
        file(GLOB FMT_SHARED_FILES "${THIRDPARTY_DIR}/lib64/libfmt.so*")
    endif()
    install(FILES ${FMT_SHARED_FILES} DESTINATION ${OUTPUT_DIR}/lib)
else()
    starrocks_resolve_thirdparty_library(FMT_STATIC_LIBRARY libfmt.a)
    add_library(fmt STATIC IMPORTED)
    set_target_properties(fmt PROPERTIES IMPORTED_LOCATION ${FMT_STATIC_LIBRARY})
endif()

starrocks_resolve_thirdparty_library(RYU_LIBRARY libryu.a)
add_library(ryu STATIC IMPORTED)
set_target_properties(ryu PROPERTIES IMPORTED_LOCATION ${RYU_LIBRARY})

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

starrocks_resolve_thirdparty_library(HTTP_CLIENT_CURL_LIBRARY libhttp_client_curl.a)
add_library(http_client_curl STATIC IMPORTED GLOBAL)
set_target_properties(http_client_curl PROPERTIES IMPORTED_LOCATION ${HTTP_CLIENT_CURL_LIBRARY})

starrocks_resolve_thirdparty_library(OPENTELEMETRY_COMMON_LIBRARY libopentelemetry_common.a)
add_library(opentelemetry_common STATIC IMPORTED GLOBAL)
set_target_properties(opentelemetry_common PROPERTIES IMPORTED_LOCATION ${OPENTELEMETRY_COMMON_LIBRARY})

starrocks_resolve_thirdparty_library(OPENTELEMETRY_TRACE_LIBRARY libopentelemetry_trace.a)
add_library(opentelemetry_trace STATIC IMPORTED GLOBAL)
set_target_properties(opentelemetry_trace PROPERTIES IMPORTED_LOCATION ${OPENTELEMETRY_TRACE_LIBRARY})

starrocks_resolve_thirdparty_library(OPENTELEMETRY_RESOURCES_LIBRARY libopentelemetry_resources.a)
add_library(opentelemetry_resources STATIC IMPORTED GLOBAL)
set_target_properties(opentelemetry_resources PROPERTIES IMPORTED_LOCATION ${OPENTELEMETRY_RESOURCES_LIBRARY})

add_library(jansson STATIC IMPORTED GLOBAL)
set_target_properties(jansson PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libjansson.a)

starrocks_resolve_thirdparty_library(AVRO_LIBRARY libavro.a)
add_library(avro STATIC IMPORTED GLOBAL)
set_target_properties(avro PROPERTIES IMPORTED_LOCATION ${AVRO_LIBRARY})

starrocks_resolve_thirdparty_library(AVROCPP_LIBRARY libavrocpp_s.a)
add_library(avrocpp STATIC IMPORTED GLOBAL)
set_target_properties(avrocpp PROPERTIES IMPORTED_LOCATION ${AVROCPP_LIBRARY})

add_library(serdes STATIC IMPORTED GLOBAL)
set_target_properties(serdes PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libserdes.a)

starrocks_resolve_thirdparty_library(OPENTELEMETRY_JAEGER_LIBRARY libopentelemetry_exporter_jaeger_trace.a)
add_library(opentelemetry_exporter_jaeger_trace STATIC IMPORTED GLOBAL)
set_target_properties(opentelemetry_exporter_jaeger_trace PROPERTIES IMPORTED_LOCATION ${OPENTELEMETRY_JAEGER_LIBRARY})

if (APPLE)
    add_library(libxml2 SHARED IMPORTED GLOBAL)
    set_target_properties(libxml2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libxml2.dylib)
else()
    add_library(libxml2 STATIC IMPORTED GLOBAL)
    set_target_properties(libxml2 PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libxml2.a)
endif()
include_directories(${THIRDPARTY_DIR}/include/libxml2)

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

add_library(benchgen STATIC IMPORTED GLOBAL)
set_target_properties(benchgen PROPERTIES IMPORTED_LOCATION ${THIRDPARTY_DIR}/lib/libbenchgen.a)

set(absl_DIR "${THIRDPARTY_DIR}/lib/cmake/absl" CACHE PATH "absl search path" FORCE)
find_package(absl CONFIG REQUIRED)

# Pre-declare protobuf::libprotobuf + Protobuf_FOUND so gRPCConfig.cmake's
# guarded find_package(Protobuf) is skipped. TP ships no ProtobufConfig.cmake
# for 3.14; without this, module-mode FindProtobuf can bind to a host
# protobuf (e.g. Homebrew 33.x) whose imported target then collides with
# the ALIAS below.
add_library(protobuf::libprotobuf ALIAS protobuf)
set(Protobuf_FOUND TRUE)
set(PROTOBUF_FOUND TRUE)
set(Protobuf_INCLUDE_DIR "${THIRDPARTY_DIR}/include")
set(Protobuf_INCLUDE_DIRS "${THIRDPARTY_DIR}/include")
set(Protobuf_LIBRARIES protobuf::libprotobuf)

set(gRPC_DIR "${THIRDPARTY_DIR}/lib/cmake/grpc" CACHE PATH "grpc search path")
find_package(gRPC CONFIG REQUIRED)
get_target_property(gRPC_INCLUDE_DIR gRPC::grpc INTERFACE_INCLUDE_DIRECTORIES)
message(STATUS "Using gRPC ${gRPC_VERSION}")
include_directories(SYSTEM ${gRPC_INCLUDE_DIR})
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

set(BUNDLED_JAVA_HOME ${THIRDPARTY_DIR}/open_jdk)
if (DEFINED ENV{JAVA_HOME} AND NOT "$ENV{JAVA_HOME}" STREQUAL "")
    set(JAVA_HOME_CANDIDATE "$ENV{JAVA_HOME}")
elseif (EXISTS "${BUNDLED_JAVA_HOME}/include/jni.h" OR
        EXISTS "${BUNDLED_JAVA_HOME}/Contents/Home/include/jni.h" OR
        EXISTS "${BUNDLED_JAVA_HOME}/libexec/openjdk.jdk/Contents/Home/include/jni.h")
    set(JAVA_HOME_CANDIDATE "${BUNDLED_JAVA_HOME}")
else()
    message(FATAL_ERROR "No bundled OpenJDK found under ${BUNDLED_JAVA_HOME} and JAVA_HOME is not set")
endif()
if (EXISTS "${JAVA_HOME_CANDIDATE}/libexec/openjdk.jdk/Contents/Home/include/jni.h")
    set(JAVA_HOME "${JAVA_HOME_CANDIDATE}/libexec/openjdk.jdk/Contents/Home")
elseif (EXISTS "${JAVA_HOME_CANDIDATE}/Contents/Home/include/jni.h")
    set(JAVA_HOME "${JAVA_HOME_CANDIDATE}/Contents/Home")
elseif (EXISTS "${JAVA_HOME_CANDIDATE}/include/jni.h")
    set(JAVA_HOME "${JAVA_HOME_CANDIDATE}")
else()
    message(FATAL_ERROR "JNI headers not found under ${JAVA_HOME_CANDIDATE}")
endif()
message(STATUS "Using JAVA_HOME for BE: ${JAVA_HOME}")

add_library(jvm SHARED IMPORTED)
if (APPLE)
    file(GLOB_RECURSE LIB_JVM "${JAVA_HOME}/lib/*/libjvm.dylib")
    set(JAVA_PLATFORM_INCLUDE_DIR "${JAVA_HOME}/include/darwin")
else()
    file(GLOB_RECURSE LIB_JVM "${JAVA_HOME}/lib/*/libjvm.so")
    set(JAVA_PLATFORM_INCLUDE_DIR "${JAVA_HOME}/include/linux")
endif()
list(LENGTH LIB_JVM LIB_JVM_COUNT)
if (LIB_JVM_COUNT EQUAL 0)
    message(FATAL_ERROR "libjvm not found under ${JAVA_HOME}")
endif()
list(GET LIB_JVM 0 LIB_JVM_PATH)
set_target_properties(jvm PROPERTIES IMPORTED_LOCATION "${LIB_JVM_PATH}")
include_directories(${JAVA_HOME}/include)
include_directories(${JAVA_PLATFORM_INCLUDE_DIR})
