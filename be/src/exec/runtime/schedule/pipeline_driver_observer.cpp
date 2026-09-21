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

#include "exec/runtime/schedule/pipeline_driver_observer.h"

#include "exec/runtime/pipeline_driver.h"
#include "exec/runtime/schedule/common.h"
#include "exec/runtime/schedule/event_scheduler.h"
#include "exec/runtime/schedule/utils.h"
#include "exec_primitive/pipeline/primitives/driver_state.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

static bool is_any_ready(PipelineDriver* driver) {
    auto sink = driver->sink_operator();
    auto source = driver->source_operator();
    return sink->is_finished() || sink->need_input() || source->is_finished() || source->has_output();
}

static bool is_sink_ready(PipelineDriver* driver) {
    auto sink = driver->sink_operator();
    return sink->is_finished() || sink->need_input();
}

static bool is_source_ready(PipelineDriver* driver) {
    auto source = driver->source_operator();
    return source->is_finished() || source->has_output();
}

void PipelineDriverObserver::_update() {
    int event = 0;
    AtomicRequestControler(_pending_event_cnt, [&]() {
        RACE_DETECT(detect_do_update);
        event |= _fetch_event();
        _do_update(event);
    });
}

void PipelineDriverObserver::_do_update(int event) {
    auto driver = _driver;
    auto token = driver->acquire_schedule_token();
    bool ready = false;
    {
        // Fast-path checks and trace logging can allocate, free buffers or submit spill I/O.
        // Restore the previous tracker before publishing the driver: an executor may immediately
        // finish its fragment and destroy the runtime state and its memory counters.
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(driver->runtime_state()->instance_mem_tracker());
        auto sink = driver->sink_operator();
        auto source = driver->source_operator();

        if (!driver->is_finished() && !driver->pending_finish()) {
            TRACE_SCHEDULE_LOG << "notify driver:" << driver << " state:" << driver->driver_state()
                               << " event:" << event << " in_block_queue:" << driver->is_in_blocked()
                               << " source finished:" << source->is_finished()
                               << " operator has output:" << source->has_output()
                               << " sink finished:" << sink->is_finished() << " sink need input:" << sink->need_input()
                               << ":" << driver->to_readable_string();
        }

        if (driver->is_in_blocked()) {
            // When the driver is blocked for a reason other than INPUT_EMPTY / OUTPUT_FULL
            // (e.g. PRECONDITION_BLOCK), source->has_output() / sink->need_input() may
            // return false while the driver still needs to be rescheduled. In that case
            // we try_schedule directly; for INPUT_EMPTY / OUTPUT_FULL we dispatch to the
            // event-specific handlers below.
            bool pipeline_block = driver->driver_state() != DriverState::INPUT_EMPTY &&
                                  driver->driver_state() != DriverState::OUTPUT_FULL;
            if (pipeline_block || _is_cancel_changed(event)) {
                ready = true;
            } else if (_is_all_changed(event)) {
                ready = is_any_ready(driver);
            } else if (_is_source_changed(event)) {
                ready = is_source_ready(driver);
            } else if (_is_sink_changed(event)) {
                ready = is_sink_ready(driver);
            } else {
                // nothing to do
            }
        } else {
            driver->set_need_check_reschedule(true);
        }
    }
    if (ready) {
        _event_scheduler->try_schedule(driver);
    }
}

void PipelineDriverObserver::runtime_filter_timeout_trigger() {
    _driver->set_all_global_rf_timeout();
    source_trigger();
}

std::string PipelineDriverObserver::debug_string() const {
    return _driver->to_readable_string();
}

} // namespace starrocks::pipeline
