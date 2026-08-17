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

set(BE_TEST_MACOS_LINK_STUBS)
if (APPLE)
    list(APPEND BE_TEST_MACOS_LINK_STUBS ../base/macos_link_stubs.cpp)
endif()

add_executable(orchestration_test
    ${BE_TEST_MACOS_LINK_STUBS}
    external_scan_context_mgr_test.cpp
    external_scan_orchestrator_test.cpp
    fragment_executor_test.cpp
    fragment_mgr_test.cpp
    orchestration_metrics_test.cpp
    query_orchestrator_test.cpp
    stream_load_orchestrator_test.cpp
)
target_link_libraries(orchestration_test Orchestration ${TEST_LINK_LIBS} gtest_main)
set_target_properties(orchestration_test PROPERTIES COMPILE_FLAGS "-fno-access-control")
