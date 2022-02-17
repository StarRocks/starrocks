// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#pragma once
#include <memory>

#include "util/limit_setter.h"
#include "util/threadpool.h"
#include "work_group.h"

namespace starrocks {
namespace workgroup {
class IoDispatcher;
class WorkGroupManager;

class IoDispatcher {
public:
    explicit IoDispatcher(std::unique_ptr<ThreadPool> thread_pool);
    virtual ~IoDispatcher();

    void initialize(int32_t num_threads);
    void change_num_threads(int32_t num_threads);

private:
    void run();

private:
    LimitSetter _num_threads_setter;
    std::unique_ptr<ThreadPool> _thread_pool;
    std::atomic<int> _next_id = 0;
};

} // namespace workgroup
} // namespace starrocks