// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "agent/task_singatures_manager.h"

namespace starrocks {

static std::mutex g_task_signatures_locks[TTaskType::type::NUM_TASK_TYPE];
static std::set<int64_t> g_task_signatures[TTaskType::type::NUM_TASK_TYPE];

bool register_task_info(TTaskType::type task_type, int64_t signature) {
    std::lock_guard task_signatures_lock(g_task_signatures_locks[task_type]);
    std::set<int64_t>& signature_set = g_task_signatures[task_type];
    return signature_set.insert(signature).second;
}

std::vector<uint8_t> batch_register_task_info(const std::vector<const TAgentTaskRequest*>& tasks) {
    size_t task_count = tasks.size();
    std::vector<uint8_t> failed_task(task_count);
    const TTaskType::type task_type = tasks[0]->task_type;
    std::lock_guard task_signatures_lock(g_task_signatures_locks[task_type]);
    for (size_t i = 0; i < tasks.size(); i++) {
        int64_t signature = tasks[i]->signature;

        // batch register task info
        std::set<int64_t>& signature_set = g_task_signatures[task_type];
        if (signature_set.insert(signature).second) {
            failed_task[i] = 0;
        } else {
            failed_task[i] = 1;
        }
    }
    return failed_task;
}

void remove_task_info(TTaskType::type task_type, int64_t signature) {
    std::lock_guard task_signatures_lock(g_task_signatures_locks[task_type]);
    g_task_signatures[task_type].erase(signature);
}

std::map<TTaskType::type, std::set<int64_t>> count_all_tasks() {
    std::map<TTaskType::type, std::set<int64_t>> tasks;
    for (int i = 0; i < TTaskType::type::NUM_TASK_TYPE; i++) {
        std::lock_guard task_signatures_lock(g_task_signatures_locks[i]);
        if (!g_task_signatures[i].empty()) {
            tasks.emplace(static_cast<TTaskType::type>(i), g_task_signatures[i]);
        }
    }
    return tasks;
}

} // namespace starrocks
