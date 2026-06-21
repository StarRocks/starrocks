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

set(AGENT_SERVER_TEST_LINK_LIBS
    AgentServer
    Runtime
    Storage
    Cache
    ComputeEnv
    ExecRuntime
    StoragePrimitive
    RuntimeCore
    Platform
    FSCore
    IO
    Common
    Base
    Gutil
    StarRocksGen
    ${STARROCKS_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

add_executable(agent_server_test
    ${BE_TEST_MACOS_LINK_STUBS}
    agent_server_test.cpp
)
target_link_libraries(agent_server_test ${AGENT_SERVER_TEST_LINK_LIBS} gtest_main)
set_target_properties(agent_server_test PROPERTIES COMPILE_FLAGS "-fno-access-control")
