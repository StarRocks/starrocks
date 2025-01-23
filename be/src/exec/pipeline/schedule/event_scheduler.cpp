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

#include "exec/pipeline/schedule/event_scheduler.h"

#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_driver_queue.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/schedule/common.h"
#include "exec/pipeline/schedule/utils.h"

namespace starrocks::pipeline {

void EventScheduler::add_blocked_driver(const DriverRawPtr driver) {
    // Capture query-context is needed before calling reschedule to avoid UAF
    auto query_ctx = driver->fragment_ctx()->runtime_state()->query_ctx()->shared_from_this();
    SCHEDULE_CHECK(!driver->is_in_blocked());
    driver->set_in_blocked(true);
    TRACE_SCHEDULE_LOG << "TRACE add to block queue:" << driver << "," << driver->to_readable_string();
    auto token = driver->acquire_schedule_token();
    // The driver is ready put to block queue. but is_in_block_queue is false, but the driver is active.
    // set this flag to make the block queue should check the driver is active
    if (!token.acquired() || driver->need_check_reschedule()) {
        driver->observer()->cancel_trigger();
    }
}

// For a single driver try_schedule has no concurrency.
void EventScheduler::try_schedule(const DriverRawPtr driver) {
    SCHEDULE_CHECK(driver->is_in_blocked());
    bool add_to_ready_queue = false;
    RACE_DETECT(driver->schedule);

    // The logic in the pipeline poller is basically the same.
    auto fragment_ctx = driver->fragment_ctx();
    if (fragment_ctx->is_canceled() && !driver->is_operator_cancelled()) {
        add_to_ready_queue = true;
    } else if (driver->need_report_exec_state()) {
        add_to_ready_queue = true;
    } else if (driver->pending_finish()) {
        if (!driver->is_still_pending_finish()) {
            driver->set_driver_state(fragment_ctx->is_canceled() ? DriverState::CANCELED : DriverState::FINISH);
            add_to_ready_queue = true;
        }
    } else if (driver->is_finished()) {
        add_to_ready_queue = true;
    } else {
        auto status_or_is_not_blocked = driver->is_not_blocked();
        if (!status_or_is_not_blocked.ok()) {
            fragment_ctx->cancel(status_or_is_not_blocked.status());
            add_to_ready_queue = true;
        } else if (status_or_is_not_blocked.value()) {
            driver->set_driver_state(DriverState::READY);
            add_to_ready_queue = true;
        }
    }

    if (add_to_ready_queue) {
        TRACE_SCHEDULE_LOG << "TRACE schedule driver:" << driver << " to ready queue";
        driver->set_need_check_reschedule(false);
        driver->set_in_blocked(false);
        _driver_queue->put_back(driver);
    }
}
} // namespace starrocks::pipeline