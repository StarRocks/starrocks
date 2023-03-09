// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "udf/java/utils.h"

#include <memory>

#include "bthread/bthread.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/scan_task_queue.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks {
PromiseStatusPtr call_function_in_pthread(RuntimeState* state, std::function<Status()> func) {
    PromiseStatusPtr ms = std::make_unique<PromiseStatus>();
    if (bthread_self()) {
        state->exec_env()->udf_call_pool()->offer([promise = ms.get(), state, func]() {
            MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(state->instance_mem_tracker());
            DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });
            promise->set_value(func());
        });
    } else {
        ms->set_value(func());
    }
    return ms;
}

PromiseStatusPtr call_hdfs_scan_function_in_pthread(std::function<Status()> func) {
    PromiseStatusPtr ms = std::make_unique<PromiseStatus>();
    if (bthread_self()) {
        ExecEnv::GetInstance()->hdfs_scan_executor_without_workgroup()->submit(
                workgroup::ScanTask([promise = ms.get(), func]() { promise->set_value(func()); }));
    } else {
        ms->set_value(func());
    }
    return ms;
}

} // namespace starrocks