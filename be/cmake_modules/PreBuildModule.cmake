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

if (${WITH_STARCACHE} STREQUAL "ON")
    add_library(starcache STATIC IMPORTED GLOBAL)
    set(STARCACHE_DIR "${THIRDPARTY_DIR}/starcache")
    set_target_properties(starcache PROPERTIES IMPORTED_LOCATION ${STARCACHE_DIR}/lib/libstarcache.a)
    include_directories(SYSTEM ${STARCACHE_DIR}/include)
    message(STATUS "link the starcache in directory: ${STARCACHE_DIR}")
endif()

if ("${USE_STAROS}" STREQUAL "ON")
    if (DEFINED ENV{STARLET_INSTALL_DIR})
        set(STARLET_INSTALL_DIR "$ENV{STARLET_INSTALL_DIR}")
    else()
        set(STARLET_INSTALL_DIR "${THIRDPARTY_DIR}/starlet")
    endif()
    if (NOT EXISTS ${STARLET_INSTALL_DIR})
        message(FATAL_ERROR "Starlet thirdparty directory ${STARLET_INSTALL_DIR} not exist!")
    else()
        message(STATUS "Searching starlet related libraries from ${STARLET_INSTALL_DIR} ...")
    endif()

    # Tell cmake that PROTOBUF library is already found
    set(PROTOBUF_FOUND TRUE)
    # starrocks project has its imported libprotobuf.a and libre2.a
    # add following ALIAS so grpc can find the correct dependent libraries
    add_library(re2::re2 ALIAS re2)
    add_library(glog::glog ALIAS glog)
    add_library(gflags::gflags_static ALIAS gflags)
    add_library(hdfs::hdfs ALIAS hdfs)
    add_library(brpc::brpc ALIAS brpc)
    add_library(leveldb::leveldb ALIAS leveldb)
    add_library(CURL::libcurl ALIAS curl)
    add_library(rocksdb::rocksdb ALIAS rocksdb)
    add_library(starcache::starcache ALIAS starcache)
    add_library(Snappy::snappy ALIAS snappy)
    add_library(libxml2::libxml2 ALIAS libxml2)
    add_library(Azure::azure-core ALIAS azure-core)
    add_library(Azure::azure-identity ALIAS azure-identity)
    add_library(Azure::azure-storage-common ALIAS azure-storage-common)
    add_library(Azure::azure-storage-blobs ALIAS azure-storage-blobs)
    add_library(Azure::azure-storage-files-datalake ALIAS azure-storage-files-datalake)

    set(prometheus-cpp_DIR "${STARLET_INSTALL_DIR}/third_party/lib/cmake/prometheus-cpp" CACHE PATH "prometheus cpp client search path")
    find_package (prometheus-cpp CONFIG REQUIRED)
    get_target_property(prometheus-cpp_INCLUDE_DIR prometheus-cpp::core INTERFACE_INCLUDE_DIRECTORIES)
    message(STATUS "Using prometheus-cpp")
    message(STATUS "  include: ${prometheus-cpp_INCLUDE_DIR}")
    include_directories(SYSTEM ${prometheus-cpp_INCLUDE_DIR})

    # gcp cloud storage related dependencies
    set(ZLIB_INCLUDE_DIR "${THIRDPARTY_DIR}/include")
    set(ZLIB_LIBRARY "${THIRDPARTY_DIR}/lib/libz.a")
    set(Crc32c_DIR "${STARLET_INSTALL_DIR}/third_party/lib/cmake/Crc32c" CACHE PATH "Crc32c search path")
    find_package (Crc32c CONFIG REQUIRED)
    set(nlohmann_json_DIR "${STARLET_INSTALL_DIR}/third_party/share/cmake/nlohmann_json" CACHE PATH "nlohmann json search path")
    find_package (nlohmann_json CONFIG REQUIRED)
    set(google_cloud_cpp_common_DIR "${STARLET_INSTALL_DIR}/third_party/lib/cmake/google_cloud_cpp_common" CACHE PATH "google cloud cpp common search path")
    find_package (google_cloud_cpp_common CONFIG REQUIRED)
    set(google_cloud_cpp_rest_internal_DIR "${STARLET_INSTALL_DIR}/third_party/lib/cmake/google_cloud_cpp_rest_internal" CACHE PATH "google cloud cpp rest internal search path")
    find_package (google_cloud_cpp_rest_internal CONFIG REQUIRED)
    set(google_cloud_cpp_storage_DIR "${STARLET_INSTALL_DIR}/third_party/lib/cmake/google_cloud_cpp_storage" CACHE PATH "google cloud cpp storage search path")
    find_package (google_cloud_cpp_storage CONFIG REQUIRED)

    set(starlet_DIR "${STARLET_INSTALL_DIR}/starlet_install/lib/cmake" CACHE PATH "starlet search path")
    find_package(starlet CONFIG REQUIRED)
    message(STATUS "Using starlet ${starlet_VERSION}")
    message(STATUS "starlet inc dir: ${STARLET_INCLUDE_DIRS}")
    message(STATUS "starlet lib dir: ${STARLET_LIBS}")
    include_directories(SYSTEM ${STARLET_INCLUDE_DIRS})

    set_target_properties(starlet::starlet_fslib_all PROPERTIES IMPORTED_GLOBAL TRUE)
    add_library(starlet_fslib_all ALIAS starlet::starlet_fslib_all)
    set_target_properties(starlet::starlet PROPERTIES IMPORTED_GLOBAL TRUE)
    add_library(starlet ALIAS starlet::starlet)

    set(STARROCKS_DEPENDENCIES
       ${STARROCKS_DEPENDENCIES}
       starlet
       starlet_fslib_all
       starcache
       ${AZURE_SDK_LIB}
       )
endif()