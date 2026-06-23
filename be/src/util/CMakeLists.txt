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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/util")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/util")

set(UTIL_FILES
  arrow/row_batch.cpp
  arrow/starrocks_column_to_arrow.cpp
  flat_json_internal.cpp
  hyper_json_transformer.cpp
  json_flat_path.cpp
  json_flattener.cpp
  json_merger.cpp
  json_path_deriver.cpp
# TODO: not supported on RHEL 5
# perf-counters.cpp
  static_asserts.cpp
  system_metrics.cpp
# coding_util.cpp
  memory_lock.cpp
  byte_buffer.cpp
  hdfs_util.cpp
  jvm_metrics.cpp
)

ADD_BE_LIB(Util
    ${UTIL_FILES}
)

target_link_libraries(Util PUBLIC RuntimeEnv)
