// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "multi_worker_pool.h"

namespace starrocks {

MultiWorkerPool::MultiWorkerPool(const TaskWorkerType worker_type, ExecEnv* env, const TMasterInfo& master_info,
                                 int worker_num)
        : TaskWorkerPool(worker_type, env, master_info, worker_num) {
    DCHECK(worker_num > 0);
    for (int i = 0; i < worker_num; i++) {
        auto pool = std::make_shared<TaskWorkerPool>(worker_type, env, master_info, 1);
        _pools.push_back(pool);
    }
}

void MultiWorkerPool::start() {
    for (const auto& pool : _pools) {
        pool->start();
    }
}

void MultiWorkerPool::submit_publish_version_task(const TAgentTaskRequest& task) {
    auto req = task.publish_version_req;
    for (auto& partition : req.partition_version_infos) {
        auto pool_id = partition.partition_id % _pools.size();
        auto pool = _pools[pool_id];
        pool->submit_task(task);
    }
}

void MultiWorkerPool::submit_task(const TAgentTaskRequest& task) {
    switch (task.task_type) {
    case ::starrocks::TTaskType::PUBLISH_VERSION:
        submit_publish_version_task(task);
        break;
    default:
        LOG(INFO) << "task type " << task.task_type << " is not supported current";
        return;
    }
}
} // namespace starrocks
