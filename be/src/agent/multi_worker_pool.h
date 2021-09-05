// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "task_worker_pool.h"

namespace starrocks {

class MultiWorkerPool : public TaskWorkerPool {
public:
    MultiWorkerPool(const TaskWorkerType worker_type, ExecEnv* env, const TMasterInfo& master_info, int worker_num);

    virtual ~MultiWorkerPool(){};

    virtual void start();

    // submit task to queue and wait to be executed
    virtual void submit_task(const TAgentTaskRequest& task);

private:
    void submit_publish_version_task(const TAgentTaskRequest& task);

    std::vector<std::shared_ptr<TaskWorkerPool>> _pools;
};
} // namespace starrocks