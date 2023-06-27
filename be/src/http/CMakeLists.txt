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

if (CLANG_TIDY_PATH)
    set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif()

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/http")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/http")

add_library(Webserver STATIC
  download_action.cpp
  http_headers.cpp
  http_method.cpp
  http_request.cpp
  http_channel.cpp
  http_status.cpp
  http_parser.cpp
  http_stream_channel.cpp
  web_page_handler.cpp
  monitor_action.cpp
  default_path_handlers.cpp
  utils.cpp
  ev_http_server.cpp
  http_client.cpp
  action/health_action.cpp
  action/checksum_action.cpp
  action/greplog_action.cpp
  action/snapshot_action.cpp
  action/reload_tablet_action.cpp
  action/restore_tablet_action.cpp
  action/pprof_actions.cpp
  action/memory_metrics_action.cpp
  action/metrics_action.cpp
  action/stream_load.cpp
  action/transaction_stream_load.cpp
  action/meta_action.cpp
  action/compact_rocksdb_meta_action.cpp
  action/compaction_action.cpp
  action/update_config_action.cpp
  action/runtime_filter_cache_action.cpp
  action/query_cache_action.cpp
  action/pipeline_blocking_drivers_action.cpp
  action/lake/dump_tablet_metadata_action.cpp
  action/stop_be_action.cpp)

# target_link_libraries(Webserver pthread dl Util)
#ADD_BE_TEST(integer-array-test)
#ADD_BE_TEST(runtime-profile-test)
#ADD_BE_TEST(benchmark-test)
#ADD_BE_TEST(decompress-test)
#ADD_BE_TEST(metrics-test)
#ADD_BE_TEST(debug-util-test)
#ADD_BE_TEST(url-coding-test)
#ADD_BE_TEST(thrift-util-test)
#ADD_BE_TEST(bit-util-test)
#ADD_BE_TEST(rle-test)
##ADD_BE_TEST(perf-counters-test)
