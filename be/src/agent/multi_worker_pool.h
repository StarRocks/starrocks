// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "task_worker_pool.h"

namespace starrocks {

// MultiWorkerPool contains multiple task pool. Each pool processed by one single worker thread.
// We create MultiWorkerPool for processing publish version task, these tasks are
// submitted to one task pool according to its partition id, so the tasks belong to
// the same partition will be processed by the same worker thread.
class MultiWorkerPool {
public:
    MultiWorkerPool(const TaskWorkerPool::TaskWorkerType worker_type, ExecEnv* env, const TMasterInfo& master_info,
                    int worker_num);

    ~MultiWorkerPool() = default;

    void start();

    void stop();

    // submit task to queue and wait to be executed
    void submit_task(const TAgentTaskRequest& task);

private:
    void submit_publish_version_task(const TAgentTaskRequest& task);

    std::vector<std::shared_ptr<TaskWorkerPool>> _pools;
};
} // namespace starrocks
