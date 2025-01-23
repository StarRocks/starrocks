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
#include <vector>

#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/schedule/common.h"
#include "exec/pipeline/schedule/utils.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"
#include "util/race_detect.h"

namespace starrocks::pipeline {
class SourceOperator;

// Pipeline observer objects. the Observable issues an event to the Observer.
// thread-safe.
// If the driver satisfies the ready state,
// the observer adds the corresponding driver to the ready driver queue.
// If the pipeline driver is not in the block queue, then the pipeline driver tries to tag it.
// We need to handle this in the event scheduler.
class PipelineObserver {
public:
    PipelineObserver(DriverRawPtr driver) : _driver(driver) {}

    DISALLOW_COPY_AND_MOVE(PipelineObserver);

    void source_trigger() {
        _active_event(SOURCE_CHANGE_EVENT);
        _update([this](int event) { _do_update(event); });
    }

    void sink_trigger() {
        _active_event(SINK_CHANGE_EVENT);
        _update([this](int event) { _do_update(event); });
    }

    void cancel_trigger() {
        _active_event(CANCEL_EVENT);
        _update([this](int event) { _do_update(event); });
    }

    void all_trigger() {
        _active_event(SOURCE_CHANGE_EVENT | SINK_CHANGE_EVENT);
        _update([this](int event) { _do_update(event); });
    }

    DriverRawPtr driver() const { return _driver; }

private:
    template <class DoUpdate>
    void _update(DoUpdate&& callback) {
        int event = 0;
        AtomicRequestControler(_pending_event_cnt, [&]() {
            RACE_DETECT(detect_do_update);
            event |= _fetch_event();
            callback(event);
        });
    }

private:
    static constexpr inline int32_t CANCEL_EVENT = 1 << 2;
    static constexpr inline int32_t SINK_CHANGE_EVENT = 1 << 1;
    static constexpr inline int32_t SOURCE_CHANGE_EVENT = 1;

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

class Observable;
class Observable {
public:
    Observable() = default;
    Observable(const Observable&) = delete;
    Observable& operator=(const Observable&) = delete;

    // Non-thread-safe, we only allow the need to do this in the fragment->prepare phase.
    void add_observer(RuntimeState* state, PipelineObserver* observer) {
        if (state->enable_event_scheduler()) {
            DCHECK(observer != nullptr);
            _observers.push_back(observer);
        }
    }

    void notify_source_observers() {
        for (auto* observer : _observers) {
            observer->source_trigger();
        }
    }
    void notify_sink_observers() {
        for (auto* observer : _observers) {
            observer->sink_trigger();
        }
    }

    void notify_runtime_filter_timeout();

    size_t num_observers() const { return _observers.size(); }

    std::string to_string() const;

private:
    std::vector<PipelineObserver*> _observers;
};

// Lots of simple operators use a sink -> source one-to-one pipeline .
// We use this to simplify the development of this class of operators.
class PipeObservable {
public:
    void attach_sink_observer(RuntimeState* state, pipeline::PipelineObserver* observer) {
        _sink_observable.add_observer(state, observer);
    }
    void attach_source_observer(RuntimeState* state, pipeline::PipelineObserver* observer) {
        _source_observable.add_observer(state, observer);
    }

    auto defer_notify_source() {
        return DeferOp([this]() { _source_observable.notify_source_observers(); });
    }
    auto defer_notify_sink() {
        return DeferOp([this]() { _sink_observable.notify_source_observers(); });
    }

private:
    Observable _sink_observable;
    Observable _source_observable;
};

} // namespace starrocks::pipeline
