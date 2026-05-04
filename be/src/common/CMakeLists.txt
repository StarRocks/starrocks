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

set(COMMON_FILES
  bthread_timer.cpp
  brpc/brpc_stub_cache.cpp
  brpc/internal_service_recoverable_stub.cpp
  brpc/lake_service_recoverable_stub.cpp
  brpc_helper.cpp
  configbase.cpp
  config.cpp
  memory/column_allocator.cpp
  metrics/process_metrics_registry.cpp
  runtime_profile.cpp
  stack_util.cpp
  system/backend_options.cpp
  system/cpu_usage_info.cpp
  system/disk_info.cpp
  system/cpu_info.cpp
  system/cpu_info_debug.cpp
  system/master_info.cpp
  system/mem_info.cpp
  thread/cpu_util.cpp
  thread/thread.cpp
  thread/threadpool.cpp
  s3_uri.cpp
  process_exit.cpp
  util/bthreads/executor.cpp
  util/debug_util.cpp
  util/misc.cpp
  util/stack_trace_mutex.cpp
  util/table_metrics.cpp
  util/thrift_client.cpp
  util/thrift_server.cpp
  util/thrift_util.cpp
  vlog_cntl.cpp
)

if (APPLE)
    list(APPEND COMMON_FILES tracer_mac_shim.cpp)
else()
    list(APPEND COMMON_FILES tracer.cpp)
endif()

if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
    list(APPEND COMMON_FILES util/minidump.cpp)
endif()

ADD_BE_LIB(Common
  ${COMMON_FILES}
)

# Generate env_config.h according to env_config.h.in
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/env_config.h.in ${GENSRC_DIR}/common/env_config.h)
