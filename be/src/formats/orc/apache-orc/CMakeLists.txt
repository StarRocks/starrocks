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

cmake_minimum_required (VERSION 2.8.12)
if (POLICY CMP0048)
    cmake_policy(SET CMP0048 NEW)
endif ()

project(ORC C CXX)

# Version number of package
SET(CPACK_PACKAGE_VERSION_MAJOR "1")
SET(CPACK_PACKAGE_VERSION_MINOR "7")
SET(CPACK_PACKAGE_VERSION_PATCH "0-SNAPSHOT")
SET(ORC_VERSION "${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake_modules")

if (DEFINED ENV{STARROCKS_HOME})
message("++++ <STARROCKS_HOME> is set. formats/orc/apache-orc is compiled in StarRocks env ++++")
endif()

option (BUILD_JAVA
    "Include ORC Java library in the build process"
     ON)

option (ANALYZE_JAVA
    "Run static analysis of the Java"
     OFF)

option (BUILD_LIBHDFSPP
    "Include LIBHDFSPP library in the build process"
     ON)

option(BUILD_CPP_TESTS
    "Build the googletest unit tests"
    ON)

option(BUILD_TOOLS
    "Build the tools"
    ON)

option(TEST_VALGRIND_MEMCHECK
    "Run the test suite using valgrind --tool=memcheck"
    OFF)

option(INSTALL_VENDORED_LIBS
    "Install vendored thirdparty libraries along with liborc"
    ON)

option(STOP_BUILD_ON_WARNING
    "Fail the build on C++ warnings"
    ON)
  
option(BUILD_POSITION_INDEPENDENT_LIB
    "Compile static libraries with position independent code"
    OFF)

if (DEFINED ENV{STARROCKS_HOME}) # specify compiler options of starrocks
  SET(BUILD_JAVA OFF)
  SET(BUILD_LIBHDFSPP OFF)
  SET(BUILD_CPP_TESTS OFF)
  SET(BUILD_TOOLS OFF)
  set(BUILD_POSITION_INDEPENDENT_LIB ON)
  SET(SNAPPY_HOME ${STARROCKS_THIRDPARTY}/installed)
  SET(ZLIB_HOME ${STARROCKS_THIRDPARTY}/installed)
  SET(GTEST_HOME ${STARROCKS_THIRDPARTY}/installed)
  SET(PROTOBUF_HOME ${STARROCKS_THIRDPARTY}/installed)
  SET(ZSTD_HOME ${STARROCKS_THIRDPARTY}/installed)
  SET(ZSTD_INCLUDE_DIR ${STARROCKS_THIRDPARTY}/installed/include/zstd)
  SET(ZSTD_LIBRARY ${STARROCKS_THIRDPARTY}/installed/lib)
  SET(LZ4_HOME ${STARROCKS_THIRDPARTY}/installed)
  SET(LZ4_INCLUDE_DIR ${STARROCKS_THIRDPARTY}/installed/include/lz4)
  SET(LZ4_LIBRARY ${STARROCKS_THIRDPARTY}/installed/lib)
  include_directories(${STARROCKS_THIRDPARTY}/installed ${STARROCKS_THIRDPARTY}/installed/include/zstd ${STARROCKS_THIRDPARTY}/installed/include/lz4)
endif ()

# Make sure that a build type is selected
if (NOT CMAKE_BUILD_TYPE)
  message(STATUS "No build type selected, default to ReleaseWithDebugInfo")
  set (CMAKE_BUILD_TYPE "RELWITHDEBINFO")
else ()
  message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
endif ()

# Set the package format
SET(CPACK_GENERATOR "TGZ")
SET(CPACK_PACKAGE_VENDOR "Apache ORC")
SET(CPACK_PACKAGE_CONTACT "Apache ORC <dev@orc.apache.org>")

INCLUDE(CPack)
INCLUDE(ExternalProject)

if (BUILD_POSITION_INDEPENDENT_LIB)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif ()

#
# Compiler specific flags
#
if (NOT MSVC)
  set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g -DNDEBUG")
  set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
endif ()
message(STATUS "compiler ${CMAKE_CXX_COMPILER_ID} version ${CMAKE_CXX_COMPILER_VERSION}")
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set (CMAKE_CXX_STANDARD 17)
  set (WARN_FLAGS "-Weverything -Wno-c++98-compat -Wno-missing-prototypes")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-c++98-compat-pedantic -Wno-padded")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-covered-switch-default")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-missing-noreturn -Wno-unknown-pragmas")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-gnu-zero-variadic-macro-arguments")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-conversion -Wno-ignored-qualifiers -Wno-shadow")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-sign-conversion -Wno-double-promotion")
  if (CMAKE_HOST_APPLE AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "11.0")
    set (WARN_FLAGS "${WARN_FLAGS} -Wno-c++2a-compat")
  endif ()
  if (STOP_BUILD_ON_WARNING)
    set (WARN_FLAGS "${WARN_FLAGS} -Werror")
  endif ()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set (WARN_FLAGS "-Wall -Wno-unknown-pragmas -Wno-conversion")
  if (STOP_BUILD_ON_WARNING)
    set (WARN_FLAGS "${WARN_FLAGS} -Werror")
  endif ()
  if (CMAKE_CXX_COMPILER_VERSION STREQUAL "" OR
      CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.7")
    set (CXX11_FLAGS "-std=c++0x")
  else ()
    set (CMAKE_CXX_STANDARD 17)
  endif ()
elseif (MSVC)
  add_definitions (-D_SCL_SECURE_NO_WARNINGS)
  add_definitions (-D_CRT_SECURE_NO_WARNINGS)
  add_definitions (-D_CRT_NONSTDC_NO_DEPRECATE) # The POSIX name for this item is deprecated
  set (WARN_FLAGS "${WARN_FLAGS} -wd4521") # multiple copy constructors specified
  set (WARN_FLAGS "${WARN_FLAGS} -wd4146") # unary minus operator applied to unsigned type, result still unsigned
endif ()

enable_testing()

INCLUDE(CheckSourceCompiles)
INCLUDE(ThirdpartyToolchain)

set (EXAMPLE_DIRECTORY ${CMAKE_SOURCE_DIR}/examples)

add_subdirectory(c++)

if (NOT DEFINED ENV{STARROCKS_HOME}) # starrocks: no need to install doc
install(
  FILES LICENSE NOTICE
  DESTINATION "share/doc/orc")
endif()

if (BUILD_TOOLS)
  add_subdirectory(tools)
endif ()

if (BUILD_CPP_TESTS)
  # Add another target called test-out that prints the results on failure
  if (CMAKE_CONFIGURATION_TYPES)
    add_custom_target (test-out
      COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
        --output-on-failure --build-config "$<CONFIGURATION>"
    )
  else ()
    add_custom_target (test-out
      COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
        --output-on-failure
    )
  endif ()
endif ()
