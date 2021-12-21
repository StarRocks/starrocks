// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "runtime/current_thread.h"

#include "storage/storage_engine.h"

namespace starrocks {

CurrentThread::~CurrentThread() {
    StorageEngine* storage_engine = ExecEnv::GetInstance()->storage_engine();
    if (UNLIKELY(storage_engine != nullptr && storage_engine->bg_worker_stopped())) {
        tls_is_thread_status_init = false;
        return;
    }
    commit();
    tls_is_thread_status_init = false;
}

} // namespace starrocks