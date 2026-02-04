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

cmake_minimum_required(VERSION 3.16)

# set CMAKE_C_COMPILER, this must set before project command
if (DEFINED ENV{CC} AND DEFINED ENV{CXX})
    set(CMAKE_C_COMPILER "$ENV{CC}")
    set(CMAKE_CXX_COMPILER "$ENV{CXX}")
elseif (DEFINED ENV{STARROCKS_GCC_HOME})
    # prefer GCC
    set(CMAKE_C_COMPILER "$ENV{STARROCKS_GCC_HOME}/bin/gcc")
    set(CMAKE_CXX_COMPILER "$ENV{STARROCKS_GCC_HOME}/bin/g++")
elseif (DEFINED ENV{STARROCKS_LLVM_HOME})
    set(CMAKE_C_COMPILER "$ENV{STARROCKS_LLVM_HOME}/bin/clang")
    set(CMAKE_CXX_COMPILER "$ENV{STARROCKS_LLVM_HOME}/bin/clang++")
else()
    message(FATAL_ERROR "STARROCKS_GCC_HOME environment variable is not set")
endif()

# disable pkgconfig searching libraries, all libraries are static imported
set(CMAKE_DISABLE_FIND_PACKAGE_PkgConfig ON)
# Workaround error of `Unknown CMake command "pkg_check_modules".'`
# refer to: https://gitlab.kitware.com/cmake/cmake/-/issues/17043
macro( pkg_check_modules )
endmacro()

project(starrocks CXX C)

# set CMAKE_BUILD_TYPE
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RELEASE)
endif()

string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)
message(STATUS "Build type is ${CMAKE_BUILD_TYPE}")

# set CMAKE_BUILD_TARGET_ARCH
# use `lscpu | grep 'Architecture' | awk '{print $2}'` only support system which language is en_US.UTF-8
execute_process(COMMAND bash "-c" "uname -m"
                OUTPUT_VARIABLE
                CMAKE_BUILD_TARGET_ARCH
                OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "Build target arch is ${CMAKE_BUILD_TARGET_ARCH}")

# Set dirs
set(BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
# CMAKE_BINARY_DIR is set to be $BASE_DIR/be/build_${buildType}
set(FILE_PREFIX_MAP_FLAG "-ffile-prefix-map=${CMAKE_BINARY_DIR}=. -ffile-prefix-map=${CMAKE_CURRENT_SOURCE_DIR}=be")
set(BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}")
set(THIRDPARTY_DIR "$ENV{STARROCKS_THIRDPARTY}/installed/")
set(STARROCKS_THIRDPARTY "$ENV{STARROCKS_THIRDPARTY}")
set(SRC_DIR "${BASE_DIR}/src")
set(GENSRC_DIR "${SRC_DIR}/gen_cpp/build")
set(TEST_DIR "${CMAKE_SOURCE_DIR}/test/")
set(OUTPUT_DIR "${BASE_DIR}/output")
set(CONTRIB_DIR "${BASE_DIR}/../contrib/")
set(CMAKE_EXPORT_COMPILECOMMANDS ON)

if (APPLE)
    set(MAKE_TEST "ON")
else()
    option(MAKE_TEST "ON for make unit test or OFF for not" OFF)
endif()
message(STATUS "make test: ${MAKE_TEST}")

option(WITH_GCOV "Build binary with gcov to get code coverage" OFF)

option(WITH_COMPRESS "Build binary with compresss debug section" ON)

option(WITH_STARCACHE "Build binary with starcache library" ON)
option(WITH_BRCP_KEEPALIVE "Build binary with brpc_socket_keepalive (requires brpc 1.8)" OFF)

option(WITH_BENCH "Build binary with bench" OFF)

option(USE_STAROS "Use StarOS to manager tablet info" OFF)

option(USE_SSE4_2 "Build with SSE4.2 instruction" ON)

option(USE_BMI_2 "Build with BMI2 instruction" ON)

option(USE_AVX2 "Build with AVX2 instruction" ON)

option(USE_AVX512 "Build with AVX512f/AVX512BW instruction" OFF)

option(WITH_TENANN "Build with TENANN" OFF)

option(WITH_RELATIVE_SRC_PATH "Build source file with relative path" ON)

if (${WITH_TENANN} STREQUAL "ON")
    add_definitions(-DWITH_TENANN)
endif()

# The StarRocks module enables dynamic linking. This is a developer option.
# If only certain modules are modified without altering the ABI,
# subsequent linking stages may be skipped.
option(ENABLE_MULTI_DYNAMIC_LIBS "Enable dynamic linking of individual StarRocks modules (developer option)" OFF)

message(STATUS "WITH TENANN is: ${WITH_TENANN}")

# Check gcc
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.8.2")
        message(FATAL_ERROR "Need GCC version at least 4.8.2")
    endif()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
elseif (NOT APPLE)
    message(FATAL_ERROR "Compiler should be GNU")
endif()

set(CMAKE_SKIP_RPATH TRUE)

set(ALL_BE_LIBS "" CACHE INTERNAL "All BE libraries")

FUNCTION(FINALIZE_BE_LIBS)
    LIST(SORT ALL_BE_LIBS)
    MESSAGE(STATUS "ALL BE LIBS (SORTED): ${ALL_BE_LIBS}") 
ENDFUNCTION()

include(cmake_modules/ThirdParty.cmake)

include(cmake_modules/PreBuildModule.cmake)

# LLVM
include(cmake_modules/FindLLVM.cmake)

# install runtime libraries if needed
include(cmake_modules/InstallSysRuntime.cmake)

# Check Clang-Tidy
include(cmake_modules/FindClangTidy.cmake)

if (CLANG_TIDY_PATH)
    # enable clang-tidy check for all source code files
    set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif()

# Check if functions are supported in this platform. All flags will generated
# in gensrc/build/common/env_config.h.
# You can check funcion here which depends on platform. Don't forget add this
# to be/src/common/env_config.h.in
include(CheckFunctionExists)
check_function_exists(sched_getcpu HAVE_SCHED_GETCPU)

# support to pass cxx flags from environment.
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} $ENV{STARROCKS_CXX_COMMON_FLAGS}")

# -no-pie: Disable Position Independent Executables (PIE) as certain libraries cannot be linked against PIE executables.
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -no-pie")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} $ENV{STARROCKS_CXX_LINKER_FLAGS}")

# compiler flags that are common across debug/release builds
#  -Wall: Enable all warnings.
#  -Wno-sign-compare: suppress warnings for comparison between signed and unsigned
#    integers
#   -fno-strict-aliasing: disable optimizations that assume strict aliasing. This
#       is unsafe to do if the code uses casts (which we obviously do).
#  -Wno-unknown-pragmas: suppress warnings for unknown (compiler specific) pragmas
#  -Wno-deprecated: gutil contains deprecated headers
#  -Wno-vla: we use C99-style variable-length arrays
#  -pthread: enable multithreaded malloc
#  -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG: enable nanosecond precision for boost
#  -fno-omit-frame-pointers: Keep frame pointer for functions in register
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall -Wno-sign-compare -Wno-unknown-pragmas -pthread -Wno-register")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-strict-aliasing -fno-omit-frame-pointer")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -std=gnu++20 -D__STDC_FORMAT_MACROS")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-deprecated -Wno-vla -Wno-comment")
if("${WITH_RELATIVE_SRC_PATH}" STREQUAL "ON")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} ${FILE_PREFIX_MAP_FLAG}")
endif()
# disable link delete(void*, long)
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fno-sized-deallocation")

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if (NOT ${MAKE_TEST} STREQUAL "ON")
        # there are too many warnings reported by clang in be/test and it will take some time to fix.
        # temporarily disable Werror in the unit test so that clang can compile.
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Werror")
    endif()
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-unused-parameter -Wno-documentation -Wno-weak-vtables")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-unknown-warning-option")
    # Turn on following warning as error explicitly
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Werror=string-plus-int")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Werror=pessimizing-move")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Werror=delete-non-virtual-dtor")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "11.0.0")
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-reserved-identifier -Wno-suggest-destructor-override")
    endif()
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-documentation-unknown-command -Wno-old-style-cast")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-c++20-designator -Wno-mismatched-tags")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "14.0.0")
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-bitwise-instead-of-logical -Wno-inconsistent-missing-override -Wno-ambiguous-reversed-operator")
    endif()
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "16.0.0")
        # ignore warning from apache-orc
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-unsafe-buffer-usage")
    endif ()
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "18.0.0")
        # ignore warning from apache-orc
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-switch-default")
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-error=nrvo")
    endif ()
    # Add -rtlib=compiler-rt for ARM architecture to fix LLVM bug: https://bugs.llvm.org/show_bug.cgi?id=16404
    if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "aarch64")
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -rtlib=compiler-rt")
    endif()
else ()
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "14.0.0")
        # ignore error from apache-orc
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-error=dangling-reference")
    endif ()
    set(CXX_GCC_FLAGS "${CXX_GCC_FLAGS} -fcoroutines")
endif()

set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_SYSTEM_NO_DEPRECATED -DBOOST_UUID_RANDOM_PROVIDER_FORCE_POSIX")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Werror=return-type -Werror=switch")
if (${USE_STAROS} STREQUAL "ON")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DUSE_STAROS")
endif()
if (${WITH_STARCACHE} STREQUAL "ON")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DWITH_STARCACHE")
endif()

# When LLVM is used, should give GCC_HOME to get c++11 header to use new string and list
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if (DEFINED ENV{STARROCKS_GCC_HOME})
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} --gcc-toolchain=$ENV{STARROCKS_GCC_HOME}")
    else()
        message(WARNING "STARROCKS_GCC_HOME evnironment variable is not set, ")
    endif()
endif()

if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    if (${USE_SSE4_2})
        # the compiler will define __SSE4_2__
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -msse4.2 -mpclmul")
    endif()
    if (${USE_AVX2})
        # the compiler will define __AVX2__
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -mavx2")
    endif()
    if (${USE_BMI_2})
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -mbmi2")
    endif()
    if (${USE_AVX512})
        # the compiler will define __AVX512F__ __AVX512BW__ __AVX512DQ__ __AVX512CD__ __AVX512VL__
        set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -mavx512f -mavx512bw -mavx512dq -mavx512cd -mavx512vl")
    endif()
elseif ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "aarch64")
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -march=armv8-a+crc")
endif()
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS}  -Wno-attributes -DS2_USE_GFLAGS -DS2_USE_GLOG")

if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 7.0)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -faligned-new")
endif()

if (STARROCKS_JIT_ENABLE)
    message(STATUS "Enable JIT")
    set(CXX_GCC_FLAGS "${CXX_GCC_FLAGS} -DSTARROCKS_JIT_ENABLE")
endif()

# For any gcc builds:
#   -g: Enable symbols for profiler tools
#   -Wno-unused-local-typedefs: Do not warn for local typedefs that are unused.
set(CXX_GCC_FLAGS "${CXX_GCC_FLAGS} -g -Wno-unused-local-typedefs")

if (WITH_GCOV)
    # To generate flags to code coverage
    set(CXX_GCC_FLAGS "${CXX_GCC_FLAGS} -fprofile-arcs -ftest-coverage")
endif()

if (WITH_COMPRESS)
    # to compresss debug section. https://gcc.gnu.org/onlinedocs/gcc/Debugging-Options.html
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -gz=zlib")
endif()

if (ENABLE_QUERY_DEBUG_TRACE)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DENABLE_QUERY_DEBUG_TRACE")
    message(STATUS "enable query debug trace")
endif()

if (ENABLE_FAULT_INJECTION)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DFIU_ENABLE")
    message(STATUS "enable fault injection")
endif()

option(ASSERT_STATUS_CHECKED "build with assert status checked" OFF)
if (ASSERT_STATUS_CHECKED)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DSTARROCKS_ASSERT_STATUS_CHECKED")
endif()

if (BUILD_FORMAT_LIB)
    set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fPIC -DBUILD_FORMAT_LIB")
    message(STATUS "enable build format library")
endif()

# For CMAKE_BUILD_TYPE=Debug
#   -ggdb: Enable gdb debugging
#   -Werror: compile warnings should be errors when using the toolchain compiler.
# Only enable for debug builds because this is what we test in pre-commit tests.
set(CXX_FLAGS_DEBUG "${CXX_GCC_FLAGS} -ggdb -O0 -gdwarf-5 -DDEBUG")

# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   -DNDEBUG: Turn off dchecks/asserts/debug only code.
set(CXX_FLAGS_RELEASE "${CXX_GCC_FLAGS} -O3 -gdwarf-4 -DNDEBUG")

SET(CXX_FLAGS_ASAN "${CXX_GCC_FLAGS} -ggdb3 -O0 -gdwarf-5 -fsanitize=address -DADDRESS_SANITIZER")
SET(CXX_FLAGS_LSAN "${CXX_GCC_FLAGS} -ggdb3 -O0 -gdwarf-5 -fsanitize=leak -DLEAK_SANITIZER")

# Set the flags to the undefined behavior sanitizer, also known as "ubsan"
# Turn on sanitizer and debug symbols to get stack traces:
SET(CXX_FLAGS_UBSAN "${CXX_FLAGS_RELEASE} -fsanitize=undefined")
# Ignore a number of noisy errors with too many false positives:
# TODO(zc):
# SET(CXX_FLAGS_UBSAN "${CXX_FLAGS_UBSAN} -fno-sanitize=alignment,function,vptr,float-divide-by-zero,float-cast-overflow")
# Don't enforce wrapped signed integer arithmetic so that the sanitizer actually sees

# Set the flags to the thread sanitizer, also known as "tsan"
# Turn on sanitizer and debug symbols to get stack traces:
SET(CXX_FLAGS_TSAN "${CXX_GCC_FLAGS} -O0 -ggdb3 -fsanitize=thread -DTHREAD_SANITIZER")

# Set compile flags based on the build type.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    SET(CMAKE_CXX_FLAGS ${CXX_FLAGS_DEBUG})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    SET(CMAKE_CXX_FLAGS ${CXX_FLAGS_RELEASE})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN")
    SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_ASAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "LSAN")
    SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_LSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN")
    SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_UBSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "TSAN")
    SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_TSAN}")
else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

# Add flags that are common across build types
SET(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")
if("${WITH_RELATIVE_SRC_PATH}" STREQUAL "ON")
    # apply -ffile-prefix-map option to .c source files as well
    SET(CMAKE_C_FLAGS "${FILE_PREFIX_MAP_FLAG} ${CMAKE_C_FLAGS}")
endif()

message(STATUS "Compiler Flags: ${CMAKE_CXX_FLAGS}")

# Thrift requires these two definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H)

# Set include dirs
include_directories(
    BEFORE
    ${SRC_DIR}/formats/orc/apache-orc/c++/include/
)
include_directories(
    ${SRC_DIR}/
    ${TEST_DIR}/
    ${GENSRC_DIR}/
    SYSTEM ${THIRDPARTY_DIR}/include
    SYSTEM ${GPERFTOOLS_HOME}/include
    SYSTEM ${JEMALLOC_HOME}/include
    SYSTEM ${THIRDPARTY_DIR}/include/thrift/
    SYSTEM ${THIRDPARTY_DIR}/include/event/
    SYSTEM ${THIRDPARTY_DIR}/llvm/include/
)

if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    include_directories(${THIRDPARTY_DIR}/include/breakpad/)
endif()

set(WL_START_GROUP "-Wl,--start-group")
set(WL_END_GROUP "-Wl,--end-group")
set(WL_LINK_STATIC "-Wl,-Bstatic")
set(WL_LINK_DYNAMIC "-Wl,-Bdynamic")

# Set starrocks libraries
set(STARROCKS_LINK_LIBS
    ${WL_START_GROUP}
    Agent
    Column
    Connector
    Exec
    -Wl,--whole-archive Exprs -Wl,--no-whole-archive
    FileSystem
    Formats
    IO
    Serde
    Storage
    Rowset
    Runtime
    Service
    ServiceBE
    Types
    Udf
    Util
    Script
    Webserver
    TestUtil
    Tools
    Geo
    Cache
    orc
    ${WL_END_GROUP}
    Common
    Base
    Gutil
    StarRocksGen
)

set(STARROCKS_DEPENDENCIES ${STARROCKS_DEPENDENCIES}
    mysql
)

set(STARROCKS_DEPENDENCIES ${STARROCKS_DEPENDENCIES}
    ${AWSSDK_LINK_LIBRARIES}
)
set_target_properties(aws-cpp-sdk-core PROPERTIES INTERFACE_LINK_LIBRARIES AWS::aws-crt-cpp)

if (STARROCKS_JIT_ENABLE)
    set(STARROCKS_DEPENDENCIES
        ${STARROCKS_DEPENDENCIES}
        ${WL_START_GROUP}
        ${LLVM_LIBRARIES}
        ${WL_END_GROUP}
    )
endif()

# Set thirdparty libraries
set(STARROCKS_DEPENDENCIES
    ${STARROCKS_DEPENDENCIES}
    build_version
    ${WL_START_GROUP}
    clucene-core
    clucene-shared
    clucene-contribs-lib
    rocksdb
    libs2
    snappy
    ${Boost_LIBRARIES}
    thrift
    thriftnb
    glog
    re2
    pprof
    # Put lz4 in front of librdkafka to make sure that segment use the native lz4 library to compress/decompress page
    # Otherwise, he will use the lz4 Library in librdkafka
    lz4
    libevent
    curl
    ${Poco_LIBRARIES}
    libz
    libbz2
    gflags
    brpc
    protobuf
    openssl
    crypto
    leveldb
    bitshuffle
    roaring
    brotlicommon
    brotlidec
    brotlienc
    zstd
    streamvbyte
    arrow
    arrow_flight
    arrow_flight_sql
    benchgen
    gRPC::grpc
    gRPC::grpc++
    parquet
    cctz
    fmt
    ryu
    hyperscan
    simdjson
    simdutf
    sasl
    gssapi_krb5
    krb5
    krb5support
    k5crypto
    com_err
    librdkafka_cpp
    librdkafka
    libpulsar
    velocypack
    opentelemetry_trace
    http_client_curl
    opentelemetry_resources
    opentelemetry_common
    opentelemetry_exporter_jaeger_trace
    libxml2
    azure-core
    azure-identity
    azure-storage-common
    azure-storage-blobs
    azure-storage-files-datalake
    jansson
    avro
    avrocpp
    serdes
    fiu
    icuuc
    icui18n
    icudata
    ${WL_END_GROUP}
)

if (NOT BUILD_FORMAT_LIB)
    set(STARROCKS_DEPENDENCIES
            ${STARROCKS_DEPENDENCIES}
            jemalloc # required by arrow
    )
endif()

if (${WITH_STARCACHE} STREQUAL "ON")
    set(STARROCKS_DEPENDENCIES
        ${STARROCKS_DEPENDENCIES}
        ${WL_START_GROUP}
        brpc
        starcache
        ${WL_END_GROUP}
    )
endif()

if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    set(STARROCKS_DEPENDENCIES
        ${STARROCKS_DEPENDENCIES}
        breakpad
        libdeflate
    )
endif()

if (${WITH_TENANN} STREQUAL "ON")
    set(STARROCKS_DEPENDENCIES
        ${STARROCKS_DEPENDENCIES}
        tenann
    )
endif()

# enable global pch compiling
add_library(StarRocksPCH INTERFACE)
target_precompile_headers(StarRocksPCH INTERFACE ${SRC_DIR}/pch/starrocks_pch.h)
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_compile_options(StarRocksPCH INTERFACE "$<$<COMPILE_LANGUAGE:CXX>:SHELL:-Xclang -fno-pch-timestamp>")
endif()


# Add all external dependencies. They should come after the starrocks libs.

set(STARROCKS_LINK_LIBS ${STARROCKS_LINK_LIBS} ${STARROCKS_DEPENDENCIES} hdfs jvm)

set(BUILD_FOR_SANITIZE "OFF")
set(DEP_SANITIZE_LIBS "")

# Add sanitize static link flags or jemalloc
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR "${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    message("use jemalloc")
    set(STARROCKS_LINK_LIBS ${STARROCKS_LINK_LIBS} jemalloc)
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ASAN")
    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(STARROCKS_LINK_LIBS ${STARROCKS_LINK_LIBS} -static-libsan)
        set(DEP_SANITIZE_LIBS -static-libsan)
    else()
        set(STARROCKS_LINK_LIBS ${STARROCKS_LINK_LIBS} -static-libasan)
        set(DEP_SANITIZE_LIBS -static-libasan)
    endif()
    set(BUILD_FOR_SANITIZE "ON")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "LSAN")
    set(STARROCKS_LINK_LIBS ${STARROCKS_LINK_LIBS} -static-liblsan)
    set(DEP_SANITIZE_LIBS -static-liblsan)
    set(BUILD_FOR_SANITIZE "ON")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN")
    message("use jemalloc")
    set(STARROCKS_LINK_LIBS ${STARROCKS_LINK_LIBS} -static-libubsan jemalloc)
    set(DEP_SANITIZE_LIBS -static-libubsan)
    set(BUILD_FOR_SANITIZE "ON")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "TSAN")
    set(STARROCKS_LINK_LIBS ${STARROCKS_LINK_LIBS} -static-libtsan)
    set(DEP_SANITIZE_LIBS -static-libtsan)
    set(BUILD_FOR_SANITIZE "ON")
else()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

set(WRAP_LINKER_FLAGS "-Wl,-wrap,__cxa_throw -Wl,-wrap,__floattidf")

FUNCTION(ADD_BE_LIB LIB_NAME)
    IF(ENABLE_MULTI_DYNAMIC_LIBS)
        # support build and install component 
        # ${CMAKE_CMD} -DCOMPONENT=$LIB_NAME -P cmake_install.cmake
        ADD_LIBRARY(${LIB_NAME} SHARED ${ARGN})
        target_link_libraries(${LIB_NAME} PRIVATE ${WRAP_LINKER_FLAGS} ${DEP_SANITIZE_LIBS})
        install(TARGETS ${LIB_NAME}
            LIBRARY DESTINATION ${OUTPUT_DIR}/lib COMPONENT ${LIB_NAME} 
            ARCHIVE DESTINATION ${OUTPUT_DIR}/lib COMPONENT ${LIB_NAME} 
        )
    ELSE()
        ADD_LIBRARY(${LIB_NAME} STATIC ${ARGN})
        target_link_libraries(${LIB_NAME} PRIVATE StarRocksPCH)
    ENDIF()
    set(ALL_BE_LIBS "${ALL_BE_LIBS};${LIB_NAME}" CACHE INTERNAL "All BE libraries")
ENDFUNCTION()


set (STARROCKS_STATIC_LINK_LIBS ${WL_LINK_STATIC} -lgomp)

if (NOT ${ENABLE_MULTI_DYNAMIC_LIBS} AND NOT ("${MAKE_TEST}" STREQUAL "ON" AND "${BUILD_FOR_SANITIZE}" STREQUAL "ON"))
    # In other words, turn to dynamic link when ENABLE_MULTI_DYNAMIC_LIBS or MAKE_TEST and BUILD_TYPE == *SAN
    # otherwise do static link gcc's lib
    set(STARROCKS_STATIC_LINK_LIBS ${STARROCKS_STATIC_LINK_LIBS} -static-libstdc++ -static-libgcc)
endif()

set(STARROCKS_LINK_LIBS ${STARROCKS_LINK_LIBS}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} -lresolv -liberty -lc -lm -ldl -rdynamic -pthread ${WRAP_LINKER_FLAGS}
)

# link gcov if WITH_GCOV is on
if (WITH_GCOV)
    set(STARROCKS_LINK_LIBS ${STARROCKS_LINK_LIBS} --coverage)
endif()

# Set libraries for test
set (TEST_LINK_LIBS ${STARROCKS_LINK_LIBS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

# Only build static libs
set(BUILD_SHARED_LIBS OFF)

set (STARROCKS_LINKER "$ENV{STARROCKS_LINKER}")
include(cmake_modules/FindGlibcVersion.cmake)

if (GLIBC_VERSION VERSION_LESS "2.29" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 14.0.0)
    if ("${STARROCKS_LINKER}" STREQUAL "" AND NOT "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "aarch64")
        set(STARROCKS_LINKER "gold")
        message(STATUS "forcibly switch linker to: ${STARROCKS_LINKER}")
    endif()
endif()
message(STATUS "using linker: ${STARROCKS_LINKER}")

if (NOT STARROCKS_LINKER STREQUAL "")
    add_link_options("-fuse-ld=${STARROCKS_LINKER}")
endif ()

if (${MAKE_TEST} STREQUAL "ON")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DGTEST_USE_OWN_TR1_TUPLE=0")
    add_definitions(-DBE_TEST)
    add_definitions(-DFIU_ENABLE)
else()
    if (BUILD_FORMAT_LIB)
        # output *.a, *.so, *.dylib to output/format-lib-tmp
        set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${OUTPUT_DIR}/format-lib-tmp/${CMAKE_BUILD_TYPE})
        set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${OUTPUT_DIR}/format-lib-tmp/${CMAKE_BUILD_TYPE})
    else()
        # output *.a, *.so, *.dylib to output/tmp
        set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${OUTPUT_DIR}/tmp/${CMAKE_BUILD_TYPE})
        set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${OUTPUT_DIR}/tmp/${CMAKE_BUILD_TYPE})
    endif ()
    # output *.exe to output/lib
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${OUTPUT_DIR}/lib)
endif ()

# Utility CMake function to make specifying tests and benchmarks less verbose
FUNCTION(ADD_BE_TEST TEST_NAME)
    set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/")
    # This gets the directory where the test is from (e.g. 'exprs' or 'runtime')
    get_filename_component(DIR_NAME ${CMAKE_CURRENT_SOURCE_DIR} NAME)
    get_filename_component(TEST_DIR_NAME ${TEST_NAME} PATH)
    get_filename_component(TEST_FILE_NAME ${TEST_NAME} NAME)

    ADD_EXECUTABLE(${TEST_FILE_NAME} ${TEST_NAME}.cpp)
    TARGET_LINK_LIBRARIES(${TEST_FILE_NAME} ${TEST_LINK_LIBS} gtest_main)
    SET_TARGET_PROPERTIES(${TEST_FILE_NAME} PROPERTIES COMPILE_FLAGS "-fno-access-control")
    if (NOT "${TEST_DIR_NAME}" STREQUAL "")
        SET_TARGET_PROPERTIES(${TEST_FILE_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}/${TEST_DIR_NAME}")
    endif()
    ADD_TEST(${TEST_FILE_NAME} "${BUILD_OUTPUT_ROOT_DIRECTORY}/${TEST_NAME}")
ENDFUNCTION()

# Add a benchmark to BE
FUNCTION(ADD_BE_BENCH BENCH_NAME)
    message(STATUS "Add Be Bench")
    set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/")
    # This gets the directory where the test is from (e.g. 'exprs' or 'runtime')
    get_filename_component(DIR_NAME ${CMAKE_CURRENT_SOURCE_DIR} NAME)
    get_filename_component(BENCH_DIR_NAME ${BENCH_NAME} PATH)
    set(BENCH_OUTPUT_DIR_NAME "output")
    get_filename_component(BENCH_FILE_NAME ${BENCH_NAME} NAME)

    find_package(benchmark REQUIRED)
    ADD_EXECUTABLE(${BENCH_FILE_NAME} ${BENCH_NAME}.cpp)
    TARGET_LINK_LIBRARIES(${BENCH_FILE_NAME} benchmark benchmark_main ${TEST_LINK_LIBS})

    SET_TARGET_PROPERTIES(${BENCH_FILE_NAME} PROPERTIES COMPILE_FLAGS "-fno-access-control")
    SET_TARGET_PROPERTIES(${BENCH_FILE_NAME} PROPERTIES COMPILE_FLAGS ${CXX_FLAGS_RELEASE})
    if (NOT "${BENCH_OUTPUT_DIR_NAME}" STREQUAL "")
        SET_TARGET_PROPERTIES(${BENCH_FILE_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}/${BENCH_OUTPUT_DIR_NAME}")
    endif()
    ADD_TEST(${BENCH_FILE_NAME} "${BUILD_OUTPUT_ROOT_DIRECTORY}/${BENCH_NAME}")
ENDFUNCTION()

add_subdirectory(${SRC_DIR}/agent)
add_subdirectory(${SRC_DIR}/base)
add_subdirectory(${SRC_DIR}/common)
add_subdirectory(${SRC_DIR}/connector)
add_subdirectory(${SRC_DIR}/column)
add_subdirectory(${SRC_DIR}/formats)
add_subdirectory(${SRC_DIR}/fs)
add_subdirectory(${SRC_DIR}/exec)
add_subdirectory(${SRC_DIR}/exprs)
add_subdirectory(${SRC_DIR}/gen_cpp)
add_subdirectory(${SRC_DIR}/geo)
add_subdirectory(${SRC_DIR}/gutil)
add_subdirectory(${SRC_DIR}/http)
add_subdirectory(${SRC_DIR}/io)
add_subdirectory(${SRC_DIR}/storage)
add_subdirectory(${SRC_DIR}/runtime)
add_subdirectory(${SRC_DIR}/serde)
add_subdirectory(${SRC_DIR}/service)
add_subdirectory(${SRC_DIR}/testutil)
add_subdirectory(${SRC_DIR}/types)
add_subdirectory(${SRC_DIR}/udf)
add_subdirectory(${SRC_DIR}/cache)

add_subdirectory(${SRC_DIR}/tools)
add_subdirectory(${SRC_DIR}/util)
add_subdirectory(${SRC_DIR}/script)

if (BUILD_FORMAT_LIB)
    message(STATUS "Build starrocks format lib")
    add_subdirectory(${SRC_DIR}/starrocks_format)
endif()

if (WITH_BENCH STREQUAL "ON")
    message(STATUS "Build binary with bench")
    add_subdirectory(${SRC_DIR}/bench)
endif()

if (${MAKE_TEST} STREQUAL "ON")
    add_subdirectory(test)
endif ()

# Install be
install(DIRECTORY DESTINATION ${OUTPUT_DIR})
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/bin)
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/conf)
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib)

install(FILES
    ${BASE_DIR}/../bin/common.sh
    ${BASE_DIR}/../bin/start_backend.sh
    ${BASE_DIR}/../bin/start_be.sh
    ${BASE_DIR}/../bin/start_cn.sh
    ${BASE_DIR}/../bin/stop_be.sh
    ${BASE_DIR}/../bin/stop_cn.sh
    ${BASE_DIR}/../bin/show_be_version.sh
    ${BASE_DIR}/../bin/meta_tool.sh
    ${BASE_DIR}/../bin/collect_be_profile.sh
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_WRITE GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    DESTINATION ${OUTPUT_DIR}/bin)

install(FILES
    ${BASE_DIR}/../conf/be.conf
    ${BASE_DIR}/../conf/be_test.conf
    ${BASE_DIR}/../conf/cn.conf
    ${BASE_DIR}/../conf/hadoop_env.sh
    ${BASE_DIR}/../conf/log4j2.properties
    ${BASE_DIR}/../conf/udf_security.policy
    ${BASE_DIR}/../conf/core-site.xml
    ${BASE_DIR}/../conf/asan_suppressions.conf
    ${BASE_DIR}/../conf/type_checker_config.xml
    DESTINATION ${OUTPUT_DIR}/conf)

install(DIRECTORY
    ${BASE_DIR}/../webroot/be/
    DESTINATION ${OUTPUT_DIR}/www)

FINALIZE_BE_LIBS()
