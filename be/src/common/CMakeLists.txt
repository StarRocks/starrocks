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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/common")

if (CLANG_TIDY_PATH)
    set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif()

add_library(Common STATIC
  daemon.cpp
  greplog.cpp
  status.cpp
  statusor.cpp
  logconfig.cpp
  configbase.cpp
  s3_uri.cpp
  tracer.cpp
)

if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    target_sources(Common PRIVATE minidump.cpp)
endif()

# Generate env_config.h according to env_config.h.in
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/env_config.h.in ${GENSRC_DIR}/common/env_config.h)
