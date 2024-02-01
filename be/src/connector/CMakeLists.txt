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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/connector")

if (CLANG_TIDY_PATH)
    set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif()

add_library(Connector STATIC
    connector.cpp
    hive_connector.cpp
    iceberg_connector.cpp
    es_connector.cpp
    jdbc_connector.cpp
    lake_connector.cpp
    mysql_connector.cpp
    file_connector.cpp
    binlog_connector.cpp
)
