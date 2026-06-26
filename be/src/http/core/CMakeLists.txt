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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/http/core")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/http/core")

set(HTTP_CORE_FILES
  http_metrics.cpp
  http_auth.cpp
  http_headers.cpp
  http_method.cpp
  http_request.cpp
  http_response.cpp
  http_channel.cpp
  http_status.cpp
  http_parser.cpp
  http_stream_channel.cpp
  ev_http_server.cpp
  http_client.cpp
)

ADD_BE_LIB(HttpCore ${HTTP_CORE_FILES})
