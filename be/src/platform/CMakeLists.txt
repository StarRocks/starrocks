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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/platform")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/platform")

set(PLATFORM_FILES
  aws/aws_sdk_guard.cpp
  aws/poco_common.cpp
  aws/poco_http_client.cpp
  aws/poco_http_client_factory.cpp
  download_util.cpp
  key_cache.cpp
  llm/llm_cache.cpp
  llm/llm_query_service.cpp
  llm/model_config.cpp
  platform_env.cpp
  platform_metrics.cpp
  path_rw.cpp
  python/env.cpp
  query_timeout_hook.cpp
  store_path.cpp
  thrift_rpc_helper.cpp
  udf_downloader.cpp
  user_function_cache.cpp
)

ADD_BE_LIB(Platform
    ${PLATFORM_FILES}
)

target_link_libraries(Platform PUBLIC HttpCore FSCore IO Types Common Base Gutil StarRocksGen)
