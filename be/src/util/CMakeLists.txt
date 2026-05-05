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
  bloom_filter.cpp
  block_split_bloom_filter.cpp
  download_util.cpp
  json_util.cpp
  json_flattener.cpp
# TODO: not supported on RHEL 5
# perf-counters.cpp
  static_asserts.cpp
  time_guard.cpp
  system_metrics.cpp
# coding_util.cpp
  thrift_rpc_helper.cpp
  debug/query_trace_impl.cpp
  memory_lock.cpp
  byte_buffer.cpp
  hdfs_util.cpp
  jvm_metrics.cpp
  llm_cache.cpp
  llm_query_service.cpp
)

ADD_BE_LIB(Util
    ${UTIL_FILES}
)
