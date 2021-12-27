// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <bthread/execution_queue.h>
DIAGNOSTIC_POP

#include "common/config.h"
#include "util/threadpool.h"

namespace starrocks {

// Used to run bthread::ExecutionQueue task in pthread instead of bthread.
// Reference: https://github.com/apache/incubator-brpc/blob/master/docs/cn/execution_queue.md
class AsyncDeltaWriterExecutor : public bthread::Executor {
public:
    Status init() {
        if (_thread_pool != nullptr) {
            return Status::InternalError("already initialized");
        }
        return ThreadPoolBuilder("delta_writer")
                .set_min_threads(config::number_tablet_writer_threads / 2)
                .set_max_threads(std::max<int>(1, config::number_tablet_writer_threads))
                .set_max_queue_size(40960)
                .set_idle_timeout(MonoDelta::FromMilliseconds(5 * 60 * 1000))
                .build(&_thread_pool);
    }

    int submit(void* (*fn)(void*), void* args) override {
        auto st = _thread_pool->submit_func([=]() { fn(args); });
        LOG_IF(WARNING, !st.ok()) << st;
        return st.ok() ? 0 : -1;
    }

private:
    std::unique_ptr<ThreadPool> _thread_pool;
};

} // namespace starrocks
