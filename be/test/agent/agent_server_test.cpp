// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "agent/agent_server.h"

#include <gtest/gtest.h>

#include <type_traits>
#include <utility>

#include "gen_cpp/AgentService_types.h"

namespace starrocks {

TEST(AgentServerTest, PublicSurfaceIsAvailableThroughAgentServerTarget) {
    static_assert(!std::is_copy_constructible_v<AgentServer>);
    static_assert(!std::is_move_constructible_v<AgentServer>);
    static_assert(
            std::is_same_v<decltype(std::declval<AgentServer&>().get_thread_pool(TTaskType::CREATE)), ThreadPool*>);
    static_assert(
            std::is_same_v<decltype(std::declval<AgentServer&>().publish_version_manager()), PublishVersionManager*>);
    static_assert(
            std::is_same_v<decltype(std::declval<AgentServer&>().get_lake_replicate_file_thread_pool()), ThreadPool*>);

    AgentServer* server = nullptr;
    EXPECT_EQ(nullptr, server);
    EXPECT_EQ(TaskWorkerType::PUBLISH_VERSION, TaskWorkerType::PUBLISH_VERSION);
}

} // namespace starrocks
