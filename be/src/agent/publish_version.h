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

#pragma once

#include <unordered_set>

#include "agent/agent_common.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

class ThreadPoolToken;
class DataDir;

void run_publish_version_task(ThreadPoolToken* token, const TPublishVersionRequest& publish_version_task,
                              TFinishTaskRequest& finish_task, std::unordered_set<DataDir*>& affected_dirs,
                              uint32_t wait_time);

} // namespace starrocks
