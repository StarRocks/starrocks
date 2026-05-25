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

#pragma once

#include <atomic>

#include "base/concurrency/race_detect.h"
#include "compute_env/pipeline/observer.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "gutil/macros.h"

namespace starrocks::pipeline {

// Pipeline driver observer objects. The Observable issues an event to the observer.
// Thread-safe. If the driver satisfies the ready state, the observer adds the
// corresponding driver to the ready driver queue. If the pipeline driver is not
// in the block queue, then the pipeline driver tries to tag it.
// We need to handle this in the event scheduler.
class PipelineDriverObserver final : public PipelineObserver {
public:
    explicit PipelineDriverObserver(DriverRawPtr driver) : _driver(driver) {}

    DISALLOW_COPY_AND_MOVE(PipelineDriverObserver);

    void source_trigger() override {
        _active_event(SOURCE_CHANGE_EVENT);
        _update();
    }

    void sink_trigger() override {
        _active_event(SINK_CHANGE_EVENT);
        _update();
    }

    void cancel_trigger() override {
        _active_event(CANCEL_EVENT);
        _update();
    }

    void all_trigger() override {
        _active_event(SOURCE_CHANGE_EVENT | SINK_CHANGE_EVENT);
        _update();
    }

    void runtime_filter_timeout_trigger() override;
    std::string debug_string() const override;

private:
    static constexpr inline int32_t CANCEL_EVENT = 1 << 2;
    static constexpr inline int32_t SINK_CHANGE_EVENT = 1 << 1;
    static constexpr inline int32_t SOURCE_CHANGE_EVENT = 1;

    void _update();
    void _do_update(int event);
    // fetch event
    int _fetch_event() { return _events.fetch_and(0, std::memory_order_acq_rel); }

    bool _is_sink_changed(int event) { return event & SINK_CHANGE_EVENT; }
    bool _is_source_changed(int event) { return event & SOURCE_CHANGE_EVENT; }
    bool _is_cancel_changed(int event) { return event & CANCEL_EVENT; }
    bool _is_all_changed(int event) { return _is_source_changed(event) && _is_sink_changed(event); }

    void _active_event(int event) { _events.fetch_or(event, std::memory_order_acq_rel); }

private:
    DECLARE_RACE_DETECTOR(detect_do_update)
    DriverRawPtr _driver = nullptr;
    std::atomic_int32_t _pending_event_cnt{};
    std::atomic_int32_t _events{};
};

} // namespace starrocks::pipeline
