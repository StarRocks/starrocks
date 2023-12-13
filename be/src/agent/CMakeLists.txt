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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/agent")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/agent")

add_library(Agent STATIC
        agent_server.cpp
        heartbeat_server.cpp
        task_worker_pool.cpp
        utils.cpp
        publish_version.cpp
        master_info.cpp
        finish_task.cpp
        report_task.cpp
        agent_task.cpp
        task_signatures_manager.cpp
        resource_group_usage_recorder.cpp
        )
