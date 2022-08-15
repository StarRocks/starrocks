// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "util/bthreads/executor.h"

#include "util/threadpool.h"

namespace starrocks::bthreads {

ThreadPoolExecutor::~ThreadPoolExecutor() {
    if (_ownership == kTakesOwnership) delete _thread_pool;
}

int ThreadPoolExecutor::submit(void* (*fn)(void*), void* args) {
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
