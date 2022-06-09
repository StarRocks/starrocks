// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <bthread/execution_queue.h>
DIAGNOSTIC_POP

#include "common/ownership.h"
#include "gutil/macros.h"
#include "util/threadpool.h"

namespace starrocks::bthreads {

// Used to run bthread::ExecutionQueue task in pthread instead of bthread.
// Reference: https://github.com/apache/incubator-brpc/blob/master/docs/cn/execution_queue.md
class ThreadPoolExecutor : public bthread::Executor {
public:
    constexpr static int64_t kDefaultBusySleepMs = 50;

    ThreadPoolExecutor() : _thread_pool(nullptr), _ownership(kDontTakeOwnership), _busy_sleep_ms(kDefaultBusySleepMs) {}

    explicit ThreadPoolExecutor(ThreadPool* pool, Ownership ownership)
            : _thread_pool(pool), _ownership(ownership), _busy_sleep_ms(kDefaultBusySleepMs) {}

    DISALLOW_COPY_AND_MOVE(ThreadPoolExecutor);

    ~ThreadPoolExecutor() override {
        if (_ownership == kTakesOwnership) delete _thread_pool;
    }

    void set_thread_pool(ThreadPool* thread_pool) {
        CHECK(_thread_pool == nullptr);
        _thread_pool = thread_pool;
    }

    void set_ownership(Ownership ownership) { _ownership = ownership; }

    void set_busy_sleep_ms(int64_t value) { _busy_sleep_ms = value; }

    int submit(void* (*fn)(void*), void* args) override;

private:
    ThreadPool* _thread_pool;
    Ownership _ownership;
    int64_t _busy_sleep_ms;
};

inline int ThreadPoolExecutor::submit(void* (*fn)(void*), void* args) {
    Status st;
    while (true) {
        st = _thread_pool->submit_func([=]() { fn(args); });
        if (!st.is_service_unavailable()) break;
        LOG(INFO) << "async_delta_writer is busy, retry after " << _busy_sleep_ms << "ms";
        bthread_usleep(_busy_sleep_ms * 1000);
    }
    LOG_IF(WARNING, !st.ok()) << st;
    return st.ok() ? 0 : -1;
}

} // namespace starrocks::bthreads
