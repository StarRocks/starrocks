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

#include <mutex>
#include <set>

#include "gen_cpp/AgentService_types.h"

namespace starrocks {

bool register_task_info(TTaskType::type task_type, int64_t signature);
std::vector<uint8_t> batch_register_task_info(const std::vector<const TAgentTaskRequest*>& tasks);
void remove_task_info(TTaskType::type task_type, int64_t signature);
std::map<TTaskType::type, std::set<int64_t>> count_all_tasks();

} // namespace starrocks
