// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "runtime/current_thread.h"

#include "storage/storage_engine.h"

namespace starrocks {

CurrentThread::~CurrentThread() {
    StorageEngine* storage_engine = ExecEnv::GetInstance()->storage_engine();
    if (UNLIKELY(storage_engine != nullptr && storage_engine->bg_worker_stopped())) {
        return;
    }
    commit();
}

} // namespace starrocks