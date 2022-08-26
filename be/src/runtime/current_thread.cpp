// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "runtime/current_thread.h"

#include "runtime/exec_env.h"
#include "storage/storage_engine.h"

namespace starrocks {

CurrentThread::~CurrentThread() {
    StorageEngine* storage_engine = StorageEngine::instance();
    if (UNLIKELY(storage_engine != nullptr && storage_engine->bg_worker_stopped())) {
        tls_is_thread_status_init = false;
        return;
    }
    commit();
    tls_is_thread_status_init = false;
}

starrocks::MemTracker* CurrentThread::mem_tracker() {
    if (UNLIKELY(tls_mem_tracker == nullptr)) {
        tls_mem_tracker = ExecEnv::GetInstance()->process_mem_tracker();
    }
    return tls_mem_tracker;
}

CurrentThread& CurrentThread::current() {
    return tls_thread_status;
}

} // namespace starrocks
