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

cmake_minimum_required(VERSION 3.11.1)

# ==============================================================================
# PLATFORM VALIDATION
# ==============================================================================
if(NOT APPLE)
    message(FATAL_ERROR "This CMakeLists.txt is specifically for macOS builds only")
endif()

# Check for ARM64 architecture (handle both possible values)
execute_process(COMMAND uname -m OUTPUT_VARIABLE SYSTEM_ARCH OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT SYSTEM_ARCH STREQUAL "arm64" AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
    message(FATAL_ERROR "This CMakeLists.txt is specifically for Apple Silicon (ARM64) only. Current architecture: ${SYSTEM_ARCH}")
endif()

# ==============================================================================
# COMPILER CONFIGURATION
# ==============================================================================
# Set CMAKE_C_COMPILER and CMAKE_CXX_COMPILER before project command
if(DEFINED ENV{CC} AND DEFINED ENV{CXX})
    set(CMAKE_C_COMPILER "$ENV{CC}")
    set(CMAKE_CXX_COMPILER "$ENV{CXX}")
elseif(DEFINED ENV{STARROCKS_LLVM_HOME})
    set(CMAKE_C_COMPILER "$ENV{STARROCKS_LLVM_HOME}/bin/clang")
    set(CMAKE_CXX_COMPILER "$ENV{STARROCKS_LLVM_HOME}/bin/clang++")
elseif(DEFINED ENV{STARROCKS_GCC_HOME})
    set(CMAKE_C_COMPILER "$ENV{STARROCKS_GCC_HOME}/bin/clang")
    set(CMAKE_CXX_COMPILER "$ENV{STARROCKS_GCC_HOME}/bin/clang++")
else()
    message(FATAL_ERROR "Either CC/CXX or STARROCKS_LLVM_HOME environment variable must be set")
endif()

# Set archiver tools early (before enabling languages)
if(DEFINED ENV{STARROCKS_LLVM_HOME})
    if(NOT DEFINED CMAKE_AR)
        set(CMAKE_AR "$ENV{STARROCKS_LLVM_HOME}/bin/llvm-ar" CACHE FILEPATH "Archiver" FORCE)
    endif()
    if(NOT DEFINED CMAKE_RANLIB)
        set(CMAKE_RANLIB "$ENV{STARROCKS_LLVM_HOME}/bin/llvm-ranlib" CACHE FILEPATH "Ranlib" FORCE)
    endif()
endif()

# Disable pkgconfig searching libraries, all libraries are static imported
set(CMAKE_DISABLE_FIND_PACKAGE_PkgConfig ON)
# Workaround error of `Unknown CMake command "pkg_check_modules".'`
macro(pkg_check_modules)
endmacro()

project(starrocks_macos CXX C)

string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)
message(STATUS "Build type is ${CMAKE_BUILD_TYPE}")

# Set CMAKE_BUILD_TARGET_ARCH
execute_process(COMMAND bash "-c" "uname -m"
                OUTPUT_VARIABLE CMAKE_BUILD_TARGET_ARCH
                OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "Build target arch is ${CMAKE_BUILD_TARGET_ARCH}")

# ==============================================================================
# HOMEBREW PREFIX DETECTION
# ==============================================================================
# Dynamically detect Homebrew installation path
execute_process(
    COMMAND brew --prefix
    OUTPUT_VARIABLE HOMEBREW_PREFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE BREW_RESULT
)

if(NOT BREW_RESULT EQUAL 0)
    message(FATAL_ERROR "Homebrew not found. Please install Homebrew first: https://brew.sh")
endif()

message(STATUS "Homebrew prefix: ${HOMEBREW_PREFIX}")

# ==============================================================================
# DIRECTORY CONFIGURATION
# ==============================================================================
set(BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../be")
set(BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}")
set(THIRDPARTY_DIR "$ENV{STARROCKS_THIRDPARTY}")
set(STARROCKS_THIRDPARTY "$ENV{STARROCKS_THIRDPARTY}")
set(SRC_DIR "${BASE_DIR}/src")
set(GENSRC_DIR "${SRC_DIR}/gen_cpp/build")
set(GENCODE_DIR "${GENSRC_DIR}/gen_cpp")
if(NOT EXISTS "${GENCODE_DIR}")
    message(FATAL_ERROR "Generated C++ sources directory not found: ${GENCODE_DIR}")
endif()
set(TEST_DIR "${CMAKE_SOURCE_DIR}/test/")
set(OUTPUT_DIR "${BASE_DIR}/output")
set(CONTRIB_DIR "${BASE_DIR}/../contrib/")

# Validate essential paths
if(NOT EXISTS "${BASE_DIR}")
    message(FATAL_ERROR "StarRocks BE directory not found: ${BASE_DIR}")
endif()

if(NOT THIRDPARTY_DIR)
    message(FATAL_ERROR "STARROCKS_THIRDPARTY environment variable must be set")
endif()

message(STATUS "Base directory: ${BASE_DIR}")
message(STATUS "Build directory: ${BUILD_DIR}")
message(STATUS "Third-party directory: ${THIRDPARTY_DIR}")

# Force-include macOS shim headers (required for typeof, std::max overloads, etc.)
# Note: We now use fmt 8.1.1 from source (same as Linux), so fmt-related conflicts are resolved
add_compile_options(-include ${CMAKE_CURRENT_LIST_DIR}/shims/include/starrocks_macos_fmt_shims.h)

# Ensure starcache/staros code paths are disabled by making the macros undefined
add_compile_options(-UWITH_STARCACHE -UUSE_STAROS)
# Add glog export definition to fix compilation issues
add_compile_definitions(GLOG_USE_GLOG_EXPORT)

message(STATUS "macOS: Disabled problematic components for ARM64 compatibility")

# ==============================================================================
# PROTOBUF CONFIGURATION - CRITICAL FOR BRPC COMPATIBILITY
# ==============================================================================
# Use compiled protobuf-3.14.0 from thirdparty directory for version compatibility
if(DEFINED ENV{PROTOBUF_ROOT})
    set(PROTOBUF_HOME "$ENV{PROTOBUF_ROOT}")
else()
    set(PROTOBUF_HOME "${THIRDPARTY_DIR}/installed")
endif()

# Force CMake to use our protobuf 3.14.0 instead of system version
set(CMAKE_FIND_PACKAGE_PREFER_CONFIG ON)
set(CMAKE_PREFIX_PATH "${PROTOBUF_HOME};${CMAKE_PREFIX_PATH}")
set(Protobuf_DIR "${PROTOBUF_HOME}/lib/cmake/protobuf" CACHE PATH "Protobuf config dir" FORCE)

# Set protobuf variables for direct usage
set(PROTOBUF_LIBRARY "${PROTOBUF_HOME}/lib/libprotobuf.a" CACHE FILEPATH "libprotobuf.a" FORCE)
set(PROTOBUF_INCLUDE_DIR "${PROTOBUF_HOME}/include" CACHE PATH "protobuf include dir" FORCE)
set(PROTOBUF_PROTOC_EXECUTABLE "${PROTOBUF_HOME}/bin/protoc" CACHE FILEPATH "protoc" FORCE)

# Create protobuf target - STATIC ONLY to avoid dynamic linking issues
add_library(protobuf STATIC IMPORTED GLOBAL)
set_target_properties(protobuf PROPERTIES
    IMPORTED_LOCATION "${PROTOBUF_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${PROTOBUF_INCLUDE_DIR}")

# Force static linking preferences
set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
set(BUILD_SHARED_LIBS OFF)

# Force linker to use static protobuf and reject dynamic ones
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-search_paths_first")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,-search_paths_first")

# Add linker flags for libc++ compatibility on macOS
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -stdlib=libc++")

# Force the linker to use the correct libc++ version
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lc++ -lc++abi")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -lc++ -lc++abi")

message(STATUS "Protobuf configuration:")
message(STATUS "  Home: ${PROTOBUF_HOME}")
message(STATUS "  Library: ${PROTOBUF_LIBRARY}")
message(STATUS "  Include: ${PROTOBUF_INCLUDE_DIR}")
message(STATUS "  Protoc: ${PROTOBUF_PROTOC_EXECUTABLE}")

# ==============================================================================
# COMPILER VALIDATION
# ==============================================================================
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.8.2")
        message(FATAL_ERROR "Need GCC version at least 4.8.2")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    message(STATUS "Using Clang compiler: ${CMAKE_CXX_COMPILER}")
else()
    message(FATAL_ERROR "Unsupported compiler: ${CMAKE_CXX_COMPILER_ID}")
endif()

# ==============================================================================
# BUILD VERSION CONFIGURATION
# ==============================================================================
set(BUILD_VERSION_CC ${CMAKE_BINARY_DIR}/build_version.cc)
configure_file(${SRC_DIR}/common/build_version.cc.in ${BUILD_VERSION_CC} @ONLY)
add_library(build_version OBJECT ${BUILD_VERSION_CC})
target_include_directories(build_version PRIVATE ${SRC_DIR}/common)

# ==============================================================================
# ENV CONFIG GENERATION
# ==============================================================================
# Generate env_config.h according to env_config.h.in
configure_file(${SRC_DIR}/common/env_config.h.in ${GENSRC_DIR}/common/env_config.h @ONLY)

# ==============================================================================
# THIRD-PARTY LIBRARY PATHS
# ==============================================================================
# Add common cmake prefix path and link library path
list(APPEND CMAKE_PREFIX_PATH ${THIRDPARTY_DIR}/installed/lib/cmake)
list(APPEND CMAKE_PREFIX_PATH ${THIRDPARTY_DIR}/installed/lib64/cmake)
link_directories(${THIRDPARTY_DIR}/installed/lib ${THIRDPARTY_DIR}/installed/lib64)

# ==============================================================================
# BOOST CONFIGURATION
# ==============================================================================
set(Boost_DEBUG FALSE)
set(Boost_USE_MULTITHREADED ON)
set(Boost_NO_BOOST_CMAKE ON)
set(BOOST_ROOT ${THIRDPARTY_DIR}/installed)

# On macOS, use static Boost libraries (matching thirdparty build)
set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_STATIC_RUNTIME ON)

# Thread configuration for macOS
set(CMAKE_THREAD_LIBS_INIT "-lpthread")
set(CMAKE_HAVE_THREADS_LIBRARY 1)
set(CMAKE_USE_WIN32_THREADS_INIT 0)
set(CMAKE_USE_PTHREADS_INIT 1)
set(THREADS_PREFER_PTHREAD_FLAG ON)

# Find threads before Boost
find_package(Threads REQUIRED)

# Skip gensrc build on macOS (causes issues with cross-compilation tools)
message(STATUS "Skipping gensrc build on macOS")

# ==============================================================================
# BOOST PACKAGE DISCOVERY
# ==============================================================================
# Note: In Boost 1.73+, system is header-only and doesn't need to be specified
find_package(Boost 1.70 REQUIRED COMPONENTS
    filesystem thread regex program_options)

if(NOT Boost_FOUND)
    message(FATAL_ERROR "Boost not found. Please check BOOST_ROOT: ${BOOST_ROOT}")
endif()

message(STATUS "Boost configuration:")
message(STATUS "  Version: ${Boost_VERSION}")
message(STATUS "  Libraries: ${Boost_LIBRARIES}")
message(STATUS "  Include dirs: ${Boost_INCLUDE_DIRS}")

# ==============================================================================
# THIRD-PARTY STATIC LIBRARIES CONFIGURATION
# ==============================================================================
# Create imported targets for all third-party libraries

# gflags
add_library(gflags STATIC IMPORTED GLOBAL)
set_target_properties(gflags PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libgflags.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)

# glog - use custom built glog for consistency with brpc
add_library(glog STATIC IMPORTED GLOBAL)
set_target_properties(glog PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libglog.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)
target_compile_definitions(glog INTERFACE GLOG_USE_GLOG_EXPORT)

# Create gflags-glog combined library to fix missing glog symbols
add_library(gflags-glog INTERFACE)
target_link_libraries(gflags-glog INTERFACE gflags glog)

# leveldb
add_library(leveldb STATIC IMPORTED GLOBAL)
set_target_properties(leveldb PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libleveldb.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)

# brpc - critical for StarRocks functionality
add_library(brpc STATIC IMPORTED GLOBAL)
set_target_properties(brpc PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libbrpc.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)
target_compile_definitions(brpc INTERFACE BRPC_WITH_GLOG=1)

# rocksdb
add_library(rocksdb STATIC IMPORTED GLOBAL)
set_target_properties(rocksdb PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/librocksdb.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)

# velocypack
add_library(velocypack STATIC IMPORTED GLOBAL)
set_target_properties(velocypack PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libvelocypack.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)

# fmt - formatting library (check if not already imported by Arrow)
if(NOT TARGET fmt)
    add_library(fmt STATIC IMPORTED GLOBAL)
    set_target_properties(fmt PROPERTIES
        IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libfmt.a
        INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)
    message(STATUS "fmt library imported from thirdparty: ${THIRDPARTY_DIR}/installed/lib/libfmt.a")
else()
    message(STATUS "fmt target already exists (likely from Arrow), using existing target")
endif()

# bitshuffle
add_library(bitshuffle STATIC IMPORTED GLOBAL)
set_target_properties(bitshuffle PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libbitshuffle.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)

# Homebrew libraries - snappy
find_library(SNAPPY_LIB NAMES snappy PATHS ${HOMEBREW_PREFIX}/lib REQUIRED)
add_library(snappy STATIC IMPORTED GLOBAL)
set_target_properties(snappy PROPERTIES
    IMPORTED_LOCATION ${SNAPPY_LIB}
    INTERFACE_INCLUDE_DIRECTORIES ${HOMEBREW_PREFIX}/include)

# Homebrew libraries - lz4
find_library(LZ4_LIB NAMES lz4 PATHS ${HOMEBREW_PREFIX}/lib REQUIRED)
add_library(lz4 STATIC IMPORTED GLOBAL)
set_target_properties(lz4 PROPERTIES
    IMPORTED_LOCATION ${LZ4_LIB}
    INTERFACE_INCLUDE_DIRECTORIES ${HOMEBREW_PREFIX}/opt/lz4/include)

# On macOS, Homebrew installs headers without subdirectory
# Add macro to use direct includes instead of subdirectory includes
add_compile_definitions(STARROCKS_MACOS_USE_FLAT_INCLUDES=1)

# Homebrew libraries - zstd
find_library(ZSTD_LIB NAMES zstd PATHS ${HOMEBREW_PREFIX}/lib REQUIRED)
add_library(zstd STATIC IMPORTED GLOBAL)
set_target_properties(zstd PROPERTIES
    IMPORTED_LOCATION ${ZSTD_LIB}
    INTERFACE_INCLUDE_DIRECTORIES ${HOMEBREW_PREFIX}/opt/zstd/include)

# zlib (from homebrew) - use dynamic lookup
find_library(ZLIB_LIB NAMES z PATHS
    ${HOMEBREW_PREFIX}/Cellar/zlib/*/lib
    ${HOMEBREW_PREFIX}/lib
    REQUIRED
    NO_DEFAULT_PATH)
if(NOT ZLIB_LIB)
    message(FATAL_ERROR "zlib library not found. Please install: brew install zlib")
endif()
add_library(zlib STATIC IMPORTED GLOBAL)
set_target_properties(zlib PROPERTIES
    IMPORTED_LOCATION ${ZLIB_LIB}
    INTERFACE_INCLUDE_DIRECTORIES ${HOMEBREW_PREFIX}/include)
message(STATUS "zlib found: ${ZLIB_LIB}")

# Bzip2 library for compression support - use dynamic lookup
find_library(BZIP2_LIB NAMES bz2 PATHS
    ${HOMEBREW_PREFIX}/Cellar/bzip2/*/lib
    ${HOMEBREW_PREFIX}/lib
    REQUIRED
    NO_DEFAULT_PATH)
if(NOT BZIP2_LIB)
    message(FATAL_ERROR "bzip2 library not found. Please install: brew install bzip2")
endif()
add_library(bzip2 STATIC IMPORTED GLOBAL)
set_target_properties(bzip2 PROPERTIES
    IMPORTED_LOCATION ${BZIP2_LIB}
    INTERFACE_INCLUDE_DIRECTORIES ${HOMEBREW_PREFIX}/include)
message(STATUS "bzip2 found: ${BZIP2_LIB}")

# Ryu library for fast float-to-string conversion
add_library(ryu STATIC IMPORTED GLOBAL)
set_target_properties(ryu PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libryu.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)
message(STATUS "ryu found: ${THIRDPARTY_DIR}/installed/lib/libryu.a")

# vectorscan (hyperscan-compatible) from thirdparty
# Vectorscan installs libhs.a and libhs_runtime.a (hyperscan ABI)
add_library(vectorscan STATIC IMPORTED GLOBAL)
set_target_properties(vectorscan PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libhs.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)

# roaring bitmap - use only compiled static library from thirdparty
find_library(ROARING_LIB NAMES libroaring.a PATHS
    ${THIRDPARTY_DIR}/installed/lib
    NO_DEFAULT_PATH)

# roaring bitmap - use only compiled static library from thirdparty
find_library(ROARING_LIB NAMES libroaring.a PATHS
    ${THIRDPARTY_DIR}/installed/lib
    NO_DEFAULT_PATH)

if(NOT ROARING_LIB)
    message(FATAL_ERROR "roaring library not found in thirdparty. Please run build_thirdparty.sh first to build croaringbitmap from source")
endif()

message(STATUS "roaring found (compiled): ${ROARING_LIB}")
add_library(roaring STATIC IMPORTED GLOBAL)
set_target_properties(roaring PROPERTIES
    IMPORTED_LOCATION ${ROARING_LIB}
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)

# Apache Arrow and Parquet from Homebrew - required for file formats
set(APACHE_ARROW_HOME "${HOMEBREW_PREFIX}/opt/apache-arrow")
if(EXISTS "${APACHE_ARROW_HOME}/lib/libarrow.dylib")
    add_library(arrow SHARED IMPORTED GLOBAL)
    set_target_properties(arrow PROPERTIES
        IMPORTED_LOCATION "${APACHE_ARROW_HOME}/lib/libarrow.dylib"
        INTERFACE_INCLUDE_DIRECTORIES "${APACHE_ARROW_HOME}/include")

    add_library(parquet SHARED IMPORTED GLOBAL)
    set_target_properties(parquet PROPERTIES
        IMPORTED_LOCATION "${APACHE_ARROW_HOME}/lib/libparquet.dylib"
        INTERFACE_INCLUDE_DIRECTORIES "${APACHE_ARROW_HOME}/include")

    message(STATUS "Apache Arrow and Parquet found at: ${APACHE_ARROW_HOME}")
else()
    message(FATAL_ERROR "Apache Arrow not found. Please install: brew install apache-arrow")
endif()

# fmt library - already defined above using thirdparty compiled version (removed duplicate)

# ICU libraries (use thirdparty compiled version for compatibility)
# Use static libraries from thirdparty to avoid version conflicts
add_library(icuuc STATIC IMPORTED GLOBAL)
set_target_properties(icuuc PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libicuuc.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)

add_library(icui18n STATIC IMPORTED GLOBAL)
set_target_properties(icui18n PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libicui18n.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)

add_library(icuio STATIC IMPORTED GLOBAL)
set_target_properties(icuio PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libicuio.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)

add_library(icudata STATIC IMPORTED GLOBAL)
set_target_properties(icudata PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libicudata.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)

add_library(icutest STATIC IMPORTED GLOBAL)
set_target_properties(icutest PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libicutest.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)

add_library(icutu STATIC IMPORTED GLOBAL)
set_target_properties(icutu PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libicutu.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)

message(STATUS "ICU libraries found in thirdparty")

# Thrift from thirdparty - built from source for complete functionality
set(THRIFT_HOME "${THIRDPARTY_DIR}/installed")

# Main Thrift library
if(EXISTS "${THRIFT_HOME}/lib/libthrift.a")
    add_library(thrift STATIC IMPORTED GLOBAL)
    set_target_properties(thrift PROPERTIES
        IMPORTED_LOCATION "${THRIFT_HOME}/lib/libthrift.a"
        INTERFACE_INCLUDE_DIRECTORIES "${THRIFT_HOME}/include")

    message(STATUS "Thrift static library found at: ${THRIFT_HOME}")
else()
    message(FATAL_ERROR "Thrift static library not found. Please run build_thirdparty.sh first")
endif()

# Thrift Non-Blocking library (critical for TNonblockingServer)
if(EXISTS "${THRIFT_HOME}/lib/libthriftnb.a")
    add_library(thriftnb STATIC IMPORTED GLOBAL)
    set_target_properties(thriftnb PROPERTIES
        IMPORTED_LOCATION "${THRIFT_HOME}/lib/libthriftnb.a"
        INTERFACE_INCLUDE_DIRECTORIES "${THRIFT_HOME}/include")

    message(STATUS "Thrift NB static library found at: ${THRIFT_HOME}")
else()
    message(FATAL_ERROR "Thrift NB static library not found. Please run build_thirdparty.sh first")
endif()

# OpenSSL from Homebrew - required for SSL/TLS support in brpc and thrift
find_package(OpenSSL REQUIRED)
if(NOT OpenSSL_FOUND)
    message(FATAL_ERROR "OpenSSL not found. Please install: brew install openssl@3")
endif()

# Ensure we have both ssl and crypto libraries
if(NOT OPENSSL_CRYPTO_LIBRARY)
    message(FATAL_ERROR "OpenSSL crypto library not found")
endif()

message(STATUS "OpenSSL configuration:")
message(STATUS "  SSL Library: ${OPENSSL_SSL_LIBRARY}")
message(STATUS "  Crypto Library: ${OPENSSL_CRYPTO_LIBRARY}")

# cURL from thirdparty - use our custom compiled version
set(CURL_LIBRARY "${THIRDPARTY_DIR}/installed/lib/libcurl.a")
if(NOT EXISTS ${CURL_LIBRARY})
    message(FATAL_ERROR "Custom curl library not found. Please run build_thirdparty.sh first")
endif()

# Create imported target for curl
add_library(CURL::libcurl STATIC IMPORTED GLOBAL)
set_target_properties(CURL::libcurl PROPERTIES
    IMPORTED_LOCATION ${CURL_LIBRARY}
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)
message(STATUS "Using custom curl: ${CURL_LIBRARY}")

# libevent from Homebrew - required for HTTP server functionality
find_library(LIBEVENT_LIB NAMES event PATHS ${HOMEBREW_PREFIX}/lib REQUIRED)
if(NOT LIBEVENT_LIB)
    message(FATAL_ERROR "libevent not found. Please install: brew install libevent")
endif()

add_library(libevent STATIC IMPORTED GLOBAL)
set_target_properties(libevent PROPERTIES
    IMPORTED_LOCATION ${LIBEVENT_LIB}
    INTERFACE_INCLUDE_DIRECTORIES ${HOMEBREW_PREFIX}/include)

# re2 library from Homebrew - required for regex support
find_library(RE2_LIB NAMES re2 PATHS ${HOMEBREW_PREFIX}/lib REQUIRED)
if(NOT RE2_LIB)
    message(FATAL_ERROR "re2 not found. Please install: brew install re2")
endif()

add_library(re2 STATIC IMPORTED GLOBAL)
set_target_properties(re2 PROPERTIES
    IMPORTED_LOCATION ${RE2_LIB}
    INTERFACE_INCLUDE_DIRECTORIES ${HOMEBREW_PREFIX}/include)

# abseil libraries from Homebrew - required by re2 (dynamic libraries)
# Temporarily allow finding .dylib files for abseil
set(CMAKE_FIND_LIBRARY_SUFFIXES_SAVED ${CMAKE_FIND_LIBRARY_SUFFIXES})
set(CMAKE_FIND_LIBRARY_SUFFIXES ".dylib" ".a")

find_library(ABSL_BASE_LIB NAMES absl_base PATHS ${HOMEBREW_PREFIX}/lib NO_DEFAULT_PATH)
find_library(ABSL_SPINLOCK_WAIT_LIB NAMES absl_spinlock_wait PATHS ${HOMEBREW_PREFIX}/lib NO_DEFAULT_PATH)
find_library(ABSL_HASH_LIB NAMES absl_hash PATHS ${HOMEBREW_PREFIX}/lib NO_DEFAULT_PATH)
find_library(ABSL_LOG_INTERNAL_MESSAGE_LIB NAMES absl_log_internal_message PATHS ${HOMEBREW_PREFIX}/lib NO_DEFAULT_PATH)
find_library(ABSL_SYNCHRONIZATION_LIB NAMES absl_synchronization PATHS ${HOMEBREW_PREFIX}/lib NO_DEFAULT_PATH)
find_library(ABSL_STRINGS_LIB NAMES absl_strings PATHS ${HOMEBREW_PREFIX}/lib NO_DEFAULT_PATH)
find_library(ABSL_STR_FORMAT_INTERNAL_LIB NAMES absl_str_format_internal PATHS ${HOMEBREW_PREFIX}/lib NO_DEFAULT_PATH)
find_library(ABSL_RAW_HASH_SET_LIB NAMES absl_raw_hash_set PATHS ${HOMEBREW_PREFIX}/lib NO_DEFAULT_PATH)

# Restore the original suffixes
set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES_SAVED})

if(NOT ABSL_BASE_LIB)
    message(FATAL_ERROR "abseil libraries not found. Please install: brew install abseil")
endif()

message(STATUS "Third-party libraries configured successfully")

# ==============================================================================
# COMPILER FLAGS AND BUILD OPTIONS
# ==============================================================================
set(CMAKE_SKIP_RPATH TRUE)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Base compiler flags for ARM64
# Enable CRC32 intrinsics on ARM64
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -march=armv8-a+crc")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -std=gnu++20")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -stdlib=libc++")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fPIC")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fno-omit-frame-pointer")
# Increase template recursion depth for complex constexpr calculations
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -ftemplate-depth=1024")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fconstexpr-depth=1024")

# Disable nontrivial-memcall warning for brpc compatibility on macOS
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-nontrivial-memcall")
# Disable deprecated warnings for macOS compatibility
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-deprecated-literal-operator")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-deprecated-builtins")
# Disable additional warnings for cleaner build output
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-inconsistent-missing-override")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-vla-cxx-extension")
# Disable warnings about deprecated headers (ext/hash_map, ext/hash_set)
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-#warnings")
# Disable deprecated declarations warning (e.g., std::iterator deprecated in C++17)
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-deprecated-declarations")
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-macro-redefined")
# Enable removed std::unary_function/binary_function in libc++ for legacy code
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -D_LIBCPP_ENABLE_CXX17_REMOVED_UNARY_BINARY_FUNCTION")

# Suppress all warnings on macOS build as requested
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-everything")

# macOS SDK and deployment target
set(CMAKE_OSX_DEPLOYMENT_TARGET "15.0")
set(CMAKE_OSX_ARCHITECTURES "arm64")
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build type specific flags
if(CMAKE_BUILD_TYPE STREQUAL "DEBUG")
    set(CXX_FLAGS_DEBUG "-g -O0 -DDEBUG")
    set(CMAKE_CXX_FLAGS_DEBUG "${CXX_FLAGS_DEBUG}")
    set(CMAKE_C_FLAGS_DEBUG "-g -O0 -DDEBUG")
elseif(CMAKE_BUILD_TYPE STREQUAL "RELEASE")
    set(CXX_FLAGS_RELEASE "-O3 -DNDEBUG -DBOOST_DISABLE_ASSERTS")
    set(CMAKE_CXX_FLAGS_RELEASE "${CXX_FLAGS_RELEASE}")
    set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG")
elseif(CMAKE_BUILD_TYPE STREQUAL "ASAN")
    set(CXX_FLAGS_ASAN "-g -O0 -fsanitize=address -fno-omit-frame-pointer -DDEBUG -DADDRESS_SANITIZER")
    set(CMAKE_CXX_FLAGS_ASAN "${CXX_FLAGS_ASAN}")
    set(CMAKE_C_FLAGS_ASAN "-g -O0 -fsanitize=address -fno-omit-frame-pointer -DDEBUG")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address")
endif()

# Apply common flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_COMMON_FLAGS}")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CXX_COMMON_FLAGS} -Wno-everything")

# ==============================================================================
# LINK LIBRARIES CONFIGURATION
# ==============================================================================
# Base link libraries for macOS
set(STARROCKS_LINK_LIBS "")

# Core libraries
list(APPEND STARROCKS_LINK_LIBS
    protobuf
    brpc
    glog
    gflags
    leveldb
    rocksdb
    velocypack
    bitshuffle
    snappy
    lz4
    zstd
    zlib
    bzip2
    ryu
    vectorscan
    roaring
    arrow
    parquet
    thrift
    thriftnb
    libevent
    re2
    ${ABSL_BASE_LIB}
    ${ABSL_SPINLOCK_WAIT_LIB}
    ${ABSL_HASH_LIB}
    ${ABSL_LOG_INTERNAL_MESSAGE_LIB}
    ${ABSL_SYNCHRONIZATION_LIB}
    ${ABSL_STRINGS_LIB}
    ${ABSL_STR_FORMAT_INTERNAL_LIB}
    ${ABSL_RAW_HASH_SET_LIB}
    icuuc
    icui18n
    icudata
    fmt
    ${Boost_LIBRARIES}
    ${OPENSSL_SSL_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY}
    CURL::libcurl
    # Additional libraries for missing symbols
    -levent_pthreads
)

# cctz - Google's C++ time zone library (from thirdparty)
add_library(cctz STATIC IMPORTED GLOBAL)
set_target_properties(cctz PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libcctz.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)
list(APPEND STARROCKS_LINK_LIBS cctz)
message(STATUS "cctz found (thirdparty): ${THIRDPARTY_DIR}/installed/lib/libcctz.a")

# streamvbyte for integer compression
find_library(STREAMVBYTE_LIB NAMES streamvbyte PATHS ${HOMEBREW_PREFIX}/lib REQUIRED)
if(STREAMVBYTE_LIB)
    add_library(streamvbyte STATIC IMPORTED GLOBAL)
    set_target_properties(streamvbyte PROPERTIES
        IMPORTED_LOCATION ${STREAMVBYTE_LIB}
        INTERFACE_INCLUDE_DIRECTORIES ${HOMEBREW_PREFIX}/include)
    list(APPEND STARROCKS_LINK_LIBS streamvbyte)
    message(STATUS "streamvbyte found: ${STREAMVBYTE_LIB}")
else()
    message(FATAL_ERROR "streamvbyte not found. Please install: brew install streamvbyte")
endif()

# simdjson for fast JSON parsing
find_library(SIMDJSON_LIB NAMES simdjson PATHS ${HOMEBREW_PREFIX}/lib REQUIRED)
if(SIMDJSON_LIB)
    add_library(simdjson SHARED IMPORTED GLOBAL)
    set_target_properties(simdjson PROPERTIES
        IMPORTED_LOCATION ${SIMDJSON_LIB}
        INTERFACE_INCLUDE_DIRECTORIES ${HOMEBREW_PREFIX}/include)
    list(APPEND STARROCKS_LINK_LIBS simdjson)
    message(STATUS "simdjson found: ${SIMDJSON_LIB}")
else()
    message(FATAL_ERROR "simdjson not found. Please install: brew install simdjson")
endif()

# simdutf from thirdparty (statically built)
add_library(simdutf STATIC IMPORTED GLOBAL)
set_target_properties(simdutf PROPERTIES
    IMPORTED_LOCATION ${THIRDPARTY_DIR}/installed/lib/libsimdutf.a
    INTERFACE_INCLUDE_DIRECTORIES ${THIRDPARTY_DIR}/installed/include)
list(APPEND STARROCKS_LINK_LIBS simdutf)
message(STATUS "simdutf found (compiled): ${THIRDPARTY_DIR}/installed/lib/libsimdutf.a")

# System libraries for macOS - fix libc++ linking, put standard libraries last
list(APPEND STARROCKS_LINK_LIBS
    -lresolv
    -lc
    -lm
    -ldl
    -lc++
    -lc++abi
    ${CMAKE_THREAD_LIBS_INIT}
)

# ==============================================================================
# APPLE FRAMEWORKS REQUIRED BY BRPC
# ==============================================================================
find_library(COREGRAPHICS_FRAMEWORK CoreGraphics REQUIRED)
find_library(CORETEXT_FRAMEWORK CoreText REQUIRED)
find_library(FOUNDATION_FRAMEWORK Foundation REQUIRED)
find_library(SECURITY_FRAMEWORK Security REQUIRED)
find_library(SYSTEMCONFIGURATION_FRAMEWORK SystemConfiguration REQUIRED)

list(APPEND STARROCKS_LINK_LIBS
    ${COREGRAPHICS_FRAMEWORK}
    ${CORETEXT_FRAMEWORK}
    ${FOUNDATION_FRAMEWORK}
    ${SECURITY_FRAMEWORK}
    ${SYSTEMCONFIGURATION_FRAMEWORK}
)

message(STATUS "Apple frameworks configured for brpc support")

# ==============================================================================
# SOURCE FILES CONFIGURATION
# ==============================================================================
# Include all source directories
include_directories(BEFORE ${CMAKE_CURRENT_LIST_DIR}/shims/include)
include_directories(BEFORE ${CMAKE_CURRENT_LIST_DIR}/shims/override)
include_directories(${SRC_DIR})
include_directories(${GENSRC_DIR})
include_directories(${BASE_DIR}/build)
include_directories(${BASE_DIR}/../gensrc/build)
include_directories(${BASE_DIR}/../gensrc/build/gen_cpp)
include_directories(${THIRDPARTY_DIR}/installed/include)
include_directories(${HOMEBREW_PREFIX}/opt/re2/include)
include_directories(${HOMEBREW_PREFIX}/opt/abseil/include)
include_directories(${HOMEBREW_PREFIX}/opt/rapidjson/include)
include_directories(${HOMEBREW_PREFIX}/opt/fast_float/include)
include_directories(${HOMEBREW_PREFIX}/opt/simdjson/include)
include_directories(${HOMEBREW_PREFIX}/opt/googletest/include)
include_directories(${HOMEBREW_PREFIX}/opt/libevent/include)
include_directories(${HOMEBREW_PREFIX}/opt/apache-arrow/include)
include_directories(${HOMEBREW_PREFIX}/opt/openssl@3/include)
include_directories(${HOMEBREW_PREFIX}/opt/gperftools/include)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})

# Add source subdirectories - following the same pattern as main CMakeLists.txt
# Note: We need to be selective about which subdirectories to include
set(STARROCKS_SOURCE_DIRS
    ${SRC_DIR}/common
    ${SRC_DIR}/exec
    ${SRC_DIR}/exprs
    ${SRC_DIR}/fs
    ${SRC_DIR}/gen_cpp
    ${SRC_DIR}/geo
    ${SRC_DIR}/runtime
    ${SRC_DIR}/serde
    ${SRC_DIR}/service
    ${SRC_DIR}/storage
    ${SRC_DIR}/types
    ${SRC_DIR}/util
)

# Create a library for all StarRocks sources
file(GLOB_RECURSE STARROCKS_SOURCES
    ${SRC_DIR}/agent/*.cpp
    ${SRC_DIR}/common/*.cpp
    ${SRC_DIR}/column/*.cpp
    ${SRC_DIR}/exec/*.cpp
    ${SRC_DIR}/exec/**/*.cpp
    ${SRC_DIR}/exprs/*.cpp
    ${SRC_DIR}/exprs/**/*.cpp
    ${SRC_DIR}/formats/*.cpp
    ${SRC_DIR}/formats/**/*.cpp
    ${SRC_DIR}/fs/*.cpp
    ${SRC_DIR}/fs/**/*.cpp
    # Remove gen_cpp/*.cc to avoid duplicates - these are included via THRIFT_SOURCES
    ${SRC_DIR}/geo/*.cpp
    ${SRC_DIR}/http/*.cpp
    ${SRC_DIR}/io/*.cpp
    ${SRC_DIR}/cache/*.cpp
    ${SRC_DIR}/cache/**/*.cpp
    ${SRC_DIR}/gutil/*.cc
    ${SRC_DIR}/gutil/**/*.cc
    ${SRC_DIR}/gutil/*.cpp
    ${SRC_DIR}/gutil/**/*.cpp
    ${SRC_DIR}/runtime/*.cpp
    ${SRC_DIR}/runtime/**/*.cpp
    ${SRC_DIR}/serde/*.cpp
    ${SRC_DIR}/service/*.cpp
    ${SRC_DIR}/service/**/*.cpp
    ${SRC_DIR}/storage/*.cpp
    ${SRC_DIR}/storage/**/*.cpp
    ${SRC_DIR}/types/*.cpp
    # util contains important .cc sources on macOS (e.g., easy_json.cc)
    ${SRC_DIR}/util/*.cpp
    ${SRC_DIR}/util/*.cc
    ${SRC_DIR}/util/**/*.cpp
    ${SRC_DIR}/util/**/*.cc
)

# Exclude generated sources under gen_cpp to avoid duplicate compilation
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/gen_cpp/.*")

# Avoid compiling aggregate operator helper sources twice; aggregate_operators.cpp
# already includes the individual operator implementations.
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/pipeline/aggregate/[^/]*_operator.cpp$")

# Force-remove geo sources from build on macOS
file(GLOB_RECURSE GEO_SOURCES
    ${SRC_DIR}/geo/*.cpp
    ${SRC_DIR}/geo/**/*.cpp)
list(REMOVE_ITEM STARROCKS_SOURCES ${GEO_SOURCES})

# Add generated protobuf files
# Only use the resolved gen_cpp directory to avoid duplicate sources mirrored in gensrc/build
file(GLOB PROTOBUF_SOURCES
    ${GENCODE_DIR}/*.pb.cc
)
list(REMOVE_DUPLICATES PROTOBUF_SOURCES)

# Add generated Thrift files (exclude server skeleton helpers)
file(GLOB THRIFT_SOURCES
    ${GENCODE_DIR}/*.cpp
)
list(FILTER THRIFT_SOURCES EXCLUDE REGEX ".*_server\\.skeleton\\.cpp$")
list(REMOVE_DUPLICATES THRIFT_SOURCES)

# Combine all sources
list(APPEND STARROCKS_SOURCES ${PROTOBUF_SOURCES})
list(APPEND STARROCKS_SOURCES ${THRIFT_SOURCES})
list(REMOVE_DUPLICATES STARROCKS_SOURCES)

# Exclude the BE entry point from the library; it is compiled separately for the executable.
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/service/starrocks_main.cpp$")

# Include manual customizations for generated thrift types
# This implements operators like TUniqueId::operator< used by std::map on macOS
set(THRIFT_CUSTOM_IMPL "${SRC_DIR}/gen_cpp/thrift_types_customize_impl.cpp")
if(EXISTS ${THRIFT_CUSTOM_IMPL})
    list(APPEND STARROCKS_SOURCES ${THRIFT_CUSTOM_IMPL})
endif()

# Include minimal connector utilities needed by local_exchange
list(APPEND STARROCKS_SOURCES ${SRC_DIR}/connector/utils.cpp)

# Include connector sink executor for spill functionality
list(APPEND STARROCKS_SOURCES ${SRC_DIR}/connector/connector_sink_executor.cpp)
list(APPEND STARROCKS_SOURCES ${SRC_DIR}/connector/partition_chunk_writer.cpp)

# Exclude problematic files for macOS - be more specific to avoid excluding core functionality
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*_test\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*_unittest\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*test_.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*benchmark.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*java_.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*python_.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*routine_load_task_executor.*\\.cpp$")

# Completely exclude HDFS related files
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/fs/hdfs/.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*hdfs.*\\.cpp$")

# Exclude Elasticsearch (ES) related files on macOS
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/es/.*\\.cpp$")

# Exclude Python UDF and Arrow Flight client
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/udf/python/.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/arrow_flight_.*\\.cpp$")

# Exclude x86-specific gutil sources on ARM64
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/gutil/atomicops-internals-x86\\.cc$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/util/memory_lock.cpp$")

# ==============================================================================
# EXCLUDE DISABLED FILE FORMATS (Avro, ORC, Parquet, Iceberg)
# ==============================================================================
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/formats/(avro|orc|parquet)/.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/file_scanner/(avro|orc|parquet)_.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*(avro|orc|parquet|iceberg).*\\.cpp$")

# ==============================================================================
# EXCLUDE CLOUD STORAGE BACKENDS (S3, Azure, HDFS)
# ==============================================================================
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/fs/(s3|hdfs|azure|cloud)/.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*(s3|aws|azure|hdfs|cloud).*\\.cpp$")

# ==============================================================================
# EXCLUDE DISABLED RUNTIME COMPONENTS
# ==============================================================================
# JIT compilation
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/jit/.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*jit.*\\.cpp$")
# Java/JDBC
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*(java|jdbc|jvm_metrics).*\\.cpp$")
# Message queues
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*(kafka|pulsar|data_consumer).*\\.cpp$")
# Database connectors
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*(mysql_scanner|mysql_table_writer).*\\.cpp$")
# Debug/profiling
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*(minidump|tracer|opentelemetry).*\\.cpp$")

# Exclude problematic components
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/inverted_plugin_factory\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*clucene.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*clucene.*\\.h$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/storage/index/inverted/.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exprs/gin_functions\\.cpp$")

# Exclude geo components in exprs as requested
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exprs/geo_functions\\.cpp$")

# Enable spill components - remove aggressive filtering
# Only exclude truly incompatible components, keep spill functionality
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/pipeline/hashjoin/spillable.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/pipeline/aggregate/.*spill.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/pipeline/aggregate/spillable.*\\.cpp$")
# Exclude additional pipeline spill operators that cause vtable issues
# Keep partition_sort_sink_operator.cpp to fix vtable errors
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/pipeline/sort/spillable_partition_sort.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/pipeline/sort/partition_sort_probe.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/pipeline/sort/partitioned_spill.*\\.cpp$")

# Exclude tracer/opentelemetry - more comprehensive
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*tracer.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*opentelemetry.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/common/tracer\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/src/common/tracer\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*src/common/tracer\\.cpp$")

# Alternative approach: directly remove tracer.cpp if it exists
list(REMOVE_ITEM STARROCKS_SOURCES "${SRC_DIR}/common/tracer.cpp")

# Debug: Print a message to verify the exclusions are working
message(STATUS "Checking if tracer.cpp is in STARROCKS_SOURCES...")
foreach(source_file ${STARROCKS_SOURCES})
    if(source_file MATCHES ".*tracer\\.cpp$")
        message(STATUS "Found tracer file: ${source_file}")
        list(REMOVE_ITEM STARROCKS_SOURCES "${source_file}")
        message(STATUS "Removed tracer file: ${source_file}")
    endif()
endforeach()

# Exclude additional ORC dependent files
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*file_chunk_sink.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*orc_chunk_sink.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*connector.*orc.*\\.cpp$")

# Exclude HTTP actions that depend on disabled components
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/http/action/jit_.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/http/action/.*jit.*\\.cpp$")

# Exclude file/hive/table_function sinks to avoid Parquet/ORC builders on macOS
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/runtime/file_result_writer\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/runtime/hive_table_sink\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/runtime/table_function_table_sink\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/pipeline/sink/table_function_table_sink_operator\\.cpp$")

# Exclude JVM/JNI dependent metrics on macOS
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/util/jvm_metrics\\.cpp$")

# Exclude Arrow Flight components
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*arrow_flight.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*flight.*\\.cpp$")

# Exclude Kafka and Pulsar data consumers - more comprehensive
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*kafka.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*pulsar.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*data_consumer.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*data_consumer_pool.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*data_consumer_group.*\\.cpp$")

# Exclude Iceberg table sink
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*iceberg.*sink.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*iceberg_table_sink.*\\.cpp$")

# Exclude Linux-only memory hook implementation on macOS
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/service/mem_hook\\.cpp$")

# Exclude Linux-specific utilities that don't have macOS equivalents
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/util/bfd_parser\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/util/buffered_stream\\.cpp$")

# Completely exclude Lake and cloud native functionality on macOS
# This includes all lake-related files to avoid dependency issues
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*lake.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/storage/lake/.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/runtime/lake.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/service/lake.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/pipeline/sink/lake.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/schema_scanner/schema_be_cloud_native.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/schema_scanner/schema_be_datacache_metrics_scanner\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/storage/cloud.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/fs/cloud.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/lake_meta_reader\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/engine_alter_tablet_task\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/update_config_action\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/engine_batch_load_task\\.cpp$")

# Exclude CSV scanner specifically
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/csv_scanner\\.cpp$")

# Exclude additional files that reference lake components
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/engine_alter_tablet_task\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/update_config_action\\.cpp$")

# Exclude cache-related files that depend on starcache (not available on macOS)
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/cache/.*\\.cpp$")
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/http/action/datacache_action\\.cpp$")

# Exclude connector sink operator which requires full connector implementations
list(FILTER STARROCKS_SOURCES EXCLUDE REGEX ".*/exec/pipeline/sink/connector_sink_operator\\.cpp$")

# Keep spill and local exchange components enabled; required for queries

# Add a minimal set of macOS shim sources only when required.
set(MACOS_SHIM_SOURCES
    "${CMAKE_CURRENT_LIST_DIR}/shims/src/gperftools_stubs.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/shims/src/bitset_resize.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/shims/src/variant_shims.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/shims/src/sinks_shim.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/shims/src/connector_shims.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/shims/src/disabled_components_shim.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/shims/src/thrift_uuid_stub.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/shims/src/tracer_shim.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/shims/src/lake_manager_stub.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/shims/src/inverted_index_stub.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/shims/src/java_window_function_stub.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/shims/src/missing_symbols_shim.cpp"
)
foreach(_shim_file IN LISTS MACOS_SHIM_SOURCES)
    if(EXISTS "${_shim_file}")
        list(APPEND STARROCKS_SOURCES "${_shim_file}")
    endif()
endforeach()

# ==============================================================================
# CREATE SEPARATE LIBRARY FOR EXPRS TO FORCE-LOAD CONSTRUCTORS
# ==============================================================================
# Extract exprs files from STARROCKS_SOURCES for separate linking
# This is needed because constructor functions in .inc files must be force-loaded on macOS
set(EXPRS_SOURCES "")
foreach(source_file ${STARROCKS_SOURCES})
    if(source_file MATCHES ".*/exprs/.*\\.cpp$")
        list(APPEND EXPRS_SOURCES "${source_file}")
    endif()
endforeach()

# Count the number of exprs files found
list(LENGTH EXPRS_SOURCES EXPRS_COUNT)
message(STATUS "Found ${EXPRS_COUNT} exprs source files for force-loading")

# If no exprs files found, this is a problem
if(EXPRS_COUNT EQUAL 0)
    message(FATAL_ERROR "No exprs source files found! Pattern matching may have failed.")
endif()

# Remove exprs from main sources
list(REMOVE_ITEM STARROCKS_SOURCES ${EXPRS_SOURCES})

# Create StarRocks core library with special handling for kv_store.cpp
# Separate kv_store.cpp to apply different compile flags
list(REMOVE_ITEM STARROCKS_SOURCES "${SRC_DIR}/storage/kv_store.cpp")
add_library(starrocks_core STATIC ${STARROCKS_SOURCES})
set_target_properties(starrocks_core PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
)

# Create separate exprs library to enable force-loading of constructor functions
add_library(starrocks_exprs STATIC ${EXPRS_SOURCES})
set_target_properties(starrocks_exprs PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
)

# Add kv_store.cpp with special compile flags (without shim header)
add_library(kv_store_obj OBJECT "${SRC_DIR}/storage/kv_store.cpp")
# Disable fmt shims for kv_store.cpp to avoid RocksDB/fmt conflicts
set_source_files_properties("${SRC_DIR}/storage/kv_store.cpp" PROPERTIES
    COMPILE_FLAGS "-DSTARROCKS_DISABLE_FMT_SHIMS=1")

# macOS-specific replacement sources

target_link_libraries(starrocks_core PRIVATE ${STARROCKS_LINK_LIBS} kv_store_obj)

# ==============================================================================
# MAIN EXECUTABLE TARGET
# ==============================================================================
# Create the main StarRocks BE executable
add_executable(starrocks_be ${SRC_DIR}/service/starrocks_main.cpp)

# Ensure the force-loaded archive is built before linking the final binary.
add_dependencies(starrocks_be starrocks_exprs)

# Link against core library and all dependencies
# Force-load only the exprs library to ensure constructor functions (like function registration)
# are linked even if not directly referenced. This is required for the .inc files to work.
# The rest of starrocks_core is linked normally to avoid pulling in unwanted symbols.
target_link_libraries(starrocks_be
    -Wl,-force_load,$<TARGET_FILE:starrocks_exprs>
    starrocks_exprs  # Explicitly add the library target so CMake/ninja tracks the dependency
    starrocks_core
    build_version
    ${STARROCKS_LINK_LIBS}
)

# Set executable properties
set_target_properties(starrocks_be PROPERTIES
    OUTPUT_NAME starrocks_be
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
)

# ==============================================================================
# BUILD OUTPUT CONFIGURATION
# ==============================================================================
# Output directories
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${OUTPUT_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${OUTPUT_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${OUTPUT_DIR}/lib)

# ==============================================================================
# INSTALL CONFIGURATION
# ==============================================================================
# Install the main executable
install(TARGETS starrocks_be
    RUNTIME DESTINATION ${OUTPUT_DIR}/lib
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
)

# Create necessary runtime directories
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/bin)
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/conf)
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib)

# ==============================================================================
# BUILD SUMMARY
# ==============================================================================
message(STATUS "")
message(STATUS "========================================")
message(STATUS "StarRocks BE macOS Build Configuration")
message(STATUS "========================================")
message(STATUS "Platform: macOS ${CMAKE_SYSTEM_VERSION} (${CMAKE_SYSTEM_PROCESSOR})")
message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Compiler: ${CMAKE_CXX_COMPILER}")
message(STATUS "")
message(STATUS "Paths:")
message(STATUS "  Source: ${BASE_DIR}")
message(STATUS "  Build: ${BUILD_DIR}")
message(STATUS "  Third-party: ${THIRDPARTY_DIR}")
message(STATUS "  Output: ${OUTPUT_DIR}")
message(STATUS "")
