// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "udf/java/utils.h"

#include <bthread/bthread.h>

#include <memory>

#include "common/compiler_util.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exec/workgroup/work_group.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks {
PromiseStatusPtr call_function_in_pthread(RuntimeState* state, const std::function<Status()>& func) {
    PromiseStatusPtr ms = std::make_unique<PromiseStatus>();
    if (bthread_self()) {
        state->exec_env()->udf_call_pool()->offer([promise = ms.get(), state, func]() {
            MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(state->instance_mem_tracker());
            SCOPED_SET_TRACE_INFO({}, state->query_id(), state->fragment_instance_id());
            DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });
            promise->set_value(func());
        });
    } else {
        ms->set_value(func());
    }
    return ms;
}

PromiseStatusPtr call_hdfs_scan_function_in_pthread(const std::function<Status()>& func) {
    PromiseStatusPtr ms = std::make_unique<PromiseStatus>();
    if (bthread_self()) {
        ExecEnv::GetInstance()->connector_scan_executor()->submit(workgroup::ScanTask(
                workgroup::WorkGroupManager::instance()->get_default_workgroup().get(),
                [promise = ms.get(), func](workgroup::YieldContext&) { promise->set_value(func()); }));
    } else {
        ms->set_value(func());
    }
    return ms;
}

} // namespace starrocks
