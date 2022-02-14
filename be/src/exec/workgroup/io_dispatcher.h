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
    explicit IoDispatcher(std::unique_ptr<ThreadPool> thread_pool, bool is_real_time = false);
    virtual ~IoDispatcher();
    void initialize(int32_t num_threads);
    void change_num_threads(int32_t num_threads);
    void set_os_priority(int32_t priority);

private:
    void run();

private:
    LimitSetter _num_threads_setter;
    bool _is_real_time;
    std::unique_ptr<ThreadPool> _thread_pool;
};

} // namespace workgroup
} // namespace starrocks