// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include <bthread/execution_queue.h>

#include "common/config.h"
#include "util/threadpool.h"

namespace starrocks::vectorized {

// Used to run bthread::ExecutionQueue task in pthread instead of bthread.
// Reference: https://github.com/apache/incubator-brpc/blob/master/docs/cn/execution_queue.md
class AsyncDeltaWriterExecutor : public bthread::Executor {
public:
    static AsyncDeltaWriterExecutor* Instance() {
        static AsyncDeltaWriterExecutor instance;
        return &instance;
    }

    int submit(void* (*fn)(void*), void* args) override {
        auto st = _thread_pool->submit_func([=]() { fn(args); });
        LOG_IF(WARNING, !st.ok()) << st;
        return st.ok() ? 0 : -1;
    }

private:
    AsyncDeltaWriterExecutor() {
        CHECK(ThreadPoolBuilder("AsyncDeltaWriterExecutor")
                      .set_min_threads(config::number_tablet_writer_threads / 2)
                      .set_max_threads(config::number_tablet_writer_threads)
                      .set_max_queue_size(40960)
                      .set_idle_timeout(MonoDelta::FromMilliseconds(5 * 60 * 1000))
                      .build(&_thread_pool)
                      .ok());
    }

    std::unique_ptr<ThreadPool> _thread_pool;
};

} // namespace starrocks::vectorized
