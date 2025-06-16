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

#include "exec/pipeline/schedule/observer.h"

#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/schedule/common.h"

namespace starrocks::pipeline {
static void on_update(PipelineDriver* driver) {
    auto sink = driver->sink_operator();
    auto source = driver->source_operator();
    if (sink->is_finished() || sink->need_input() || source->is_finished() || source->has_output()) {
        driver->fragment_ctx()->event_scheduler()->try_schedule(driver);
    }
}

static void on_sink_update(PipelineDriver* driver) {
    auto sink = driver->sink_operator();
    if (sink->is_finished() || sink->need_input()) {
        driver->fragment_ctx()->event_scheduler()->try_schedule(driver);
    }
}

static void on_source_update(PipelineDriver* driver) {
    auto source = driver->source_operator();
    if (source->is_finished() || source->has_output()) {
        driver->fragment_ctx()->event_scheduler()->try_schedule(driver);
    }
}

void PipelineObserver::_do_update(int event) {
    auto driver = _driver;
    auto token = driver->acquire_schedule_token();
    auto sink = driver->sink_operator();
    auto source = driver->source_operator();

    if (!driver->is_finished() && !driver->pending_finish()) {
        TRACE_SCHEDULE_LOG << "notify driver:" << driver << " state:" << driver->driver_state() << " event:" << event
                           << " in_block_queue:" << driver->is_in_blocked()
                           << " source finished:" << source->is_finished()
                           << " operator has output:" << source->has_output()
                           << " sink finished:" << sink->is_finished() << " sink need input:" << sink->need_input()
                           << ":" << driver->to_readable_string();
    }

    if (driver->is_in_blocked()) {
        // In PRECONDITION state, has_output need_input may return false. In this case,
        // we need to schedule the driver to INPUT_EMPTY/OUTPUT_FULL state.
        bool pipeline_block = driver->driver_state() != DriverState::INPUT_EMPTY ||
                              driver->driver_state() != DriverState::OUTPUT_FULL;
        if (pipeline_block || _is_cancel_changed(event)) {
            driver->fragment_ctx()->event_scheduler()->try_schedule(driver);
        } else if (_is_all_changed(event)) {
            on_update(driver);
        } else if (_is_source_changed(event)) {
            on_source_update(driver);
        } else if (_is_sink_changed(event)) {
            on_sink_update(driver);
        } else {
            // nothing to do
        }
    } else {
        driver->set_need_check_reschedule(true);
    }
}

std::string Observable::to_string() const {
    std::string str;
    for (auto* observer : _observers) {
        str += observer->driver()->to_readable_string() + "\n";
    }
    return str;
}

void Observable::notify_runtime_filter_timeout() {
    for (auto* observer : _observers) {
        observer->driver()->set_all_global_rf_timeout();
        observer->source_trigger();
    }
}

} // namespace starrocks::pipeline