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

#include "exec/pipeline/schedule/timeout_tasks.h"

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/schedule/common.h"
#include "util/stack_util.h"

namespace starrocks::pipeline {
void CheckFragmentTimeout::Run() {
    auto query_ctx = _fragment_ctx->runtime_state()->query_ctx();
    size_t expire_seconds = query_ctx->get_query_expire_seconds();
    TRACE_SCHEDULE_LOG << "fragment_instance_id:" << print_id(_fragment_ctx->fragment_instance_id());
    _fragment_ctx->cancel(Status::TimedOut(fmt::format("Query reached its timeout of {} seconds", expire_seconds)));

    _fragment_ctx->iterate_drivers([](const DriverPtr& driver) {
        driver->set_need_check_reschedule(true);
        if (driver->is_in_blocked()) {
            LOG(WARNING) << "[Driver] Timeout " << driver->to_readable_string();
            driver->observer()->cancel_trigger();
        }
    });
}

void RFScanWaitTimeout::Run() {
    if (_all_rf_timeout) {
        _timeout.notify_runtime_filter_timeout();
    } else {
        _timeout.notify_source_observers();
    }
}

} // namespace starrocks::pipeline
