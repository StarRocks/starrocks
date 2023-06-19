# This file is made available under Elastic License 2.0.
# This file is based on code available under the Apache license here:
#   https://github.com/apache/incubator-doris/blob/master/be/src/util/CMakeLists.txt

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

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/util")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/util")

set(UTIL_FILES
  bthreads/executor.cpp
  arrow/row_batch.cpp
  arrow/starrocks_column_to_arrow.cpp
  arrow/utils.cpp
  bfd_parser.cpp
  bitmap.cpp
  compression/block_compression.cpp
  compression/compression_context_pool_singletons.cpp
  compression/stream_compression.cpp
  coding.cpp
  cpu_info.cpp
  cpu_usage_info.cpp
  crc32c.cpp
  date_func.cpp
  dynamic_util.cpp
  debug_util.cpp
  disk_info.cpp
  download_util.cpp
  errno.cpp
  hash_util.hpp
  json_util.cpp
  json.cpp
  json_converter.cpp
  starrocks_metrics.cpp
  mem_info.cpp
  metrics.cpp
  murmur_hash3.cpp
  network_util.cpp
  parse_util.cpp
  path_builder.cpp
# TODO: not supported on RHEL 5
# perf-counters.cpp
  runtime_profile.cpp
  static_asserts.cpp
  string_parser.cpp
  thrift_util.cpp
  thrift_client.cpp
  thrift_server.cpp
  stack_util.cpp
  system_metrics.cpp
  url_parser.cpp
  url_coding.cpp
  mysql_row_buffer.cpp
  error_util.cc
  spinlock.cc
  file_util.cpp
  filesystem_util.cc
  time.cpp
# coding_util.cpp
  cidr.cpp
  core_local.cpp
  uid_util.cpp
  aes_util.cpp
  string_util.cpp
  md5.cpp
  thrift_rpc_helper.cpp
  faststring.cc
  slice.cpp
  sm3.cpp
  frame_of_reference_coding.cpp
  utf8_check.cpp
  path_util.cpp
  monotime.cpp
  thread.cpp
  threadpool.cpp
  trace.cpp
  trace_metrics.cpp
  timezone_utils.cpp
  easy_json.cc
  mustache/mustache.cc
  int96.cpp
  hdfs_util.cpp
  gc_helper.cpp
  gc_helper_smoothstep.cpp
  sha.cpp
  lru_cache.cpp
  tdigest.cpp
  debug/query_trace_impl.cpp
)

# simdjson Runtime Implement Dispatch: https://github.com/simdjson/simdjson/blob/master/doc/implementation-selection.md#runtime-cpu-detection
# Unset architecture-specific flags to avoid breaking implement runtime dispatch.
if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    set_source_files_properties(json.cpp PROPERTIES COMPILE_FLAGS "-mno-avx -mno-avx2")
    set_source_files_properties(json_converter.cpp PROPERTIES COMPILE_FLAGS "-mno-avx -mno-avx2")
endif()

add_library(Util STATIC
    ${UTIL_FILES}
)

