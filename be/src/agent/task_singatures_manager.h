// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
