// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#include "util/limit_setter.h"
#include "util/threadpool.h"
#include "work_group.h"

namespace starrocks {
namespace workgroup {

class ScanExecutor;
class WorkGroupManager;
struct ScanTask;
class ScanTaskQueue;

class ScanExecutor {
public:
    explicit ScanExecutor(std::unique_ptr<ThreadPool> thread_pool, std::unique_ptr<ScanTaskQueue> task_queue);
    virtual ~ScanExecutor();

    void initialize(int32_t num_threads);
    void change_num_threads(int32_t num_threads);

    bool submit(ScanTask task);

private:
    void worker_thread();

    LimitSetter _num_threads_setter;
    std::unique_ptr<ScanTaskQueue> _task_queue;
    // _thread_pool must be placed after _task_queue, because worker threads in _thread_pool use _task_queue.
    std::unique_ptr<ThreadPool> _thread_pool;
    std::atomic<int> _next_id = 0;
};

} // namespace workgroup
} // namespace starrocks
