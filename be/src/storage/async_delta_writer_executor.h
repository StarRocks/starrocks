// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <bthread/bthread.h>
#include <bthread/execution_queue.h>
DIAGNOSTIC_POP

#include "common/config.h"
#include "util/threadpool.h"

namespace starrocks {
const int64_t kRetryIntervalMs = 50;

// Used to run bthread::ExecutionQueue task in pthread instead of bthread.
// Reference: https://github.com/apache/incubator-brpc/blob/master/docs/cn/execution_queue.md
class AsyncDeltaWriterExecutor : public bthread::Executor {
public:
    Status init(int max_queue_size = 40960) {
        if (_thread_pool != nullptr) {
            return Status::InternalError("already initialized");
        }
        return ThreadPoolBuilder("delta_writer")
                .set_min_threads(config::number_tablet_writer_threads / 2)
                .set_max_threads(std::max<int>(1, config::number_tablet_writer_threads))
                .set_max_queue_size(max_queue_size)
                .set_idle_timeout(MonoDelta::FromMilliseconds(5 * 60 * 1000))
                .build(&_thread_pool);
    }

    int submit(void* (*fn)(void*), void* args) override {
        Status st;
        while (true) {
            st = _thread_pool->submit_func([=]() { fn(args); });
            if (!st.is_service_unavailable()) break;
            LOG(INFO) << "async_delta_writer is busy, retry after " << kRetryIntervalMs << "ms";
            bthread_usleep(kRetryIntervalMs * 1000);
        }
        LOG_IF(WARNING, !st.ok()) << st;
        return st.ok() ? 0 : -1;
    }

private:
    std::unique_ptr<ThreadPool> _thread_pool;
};

} // namespace starrocks
