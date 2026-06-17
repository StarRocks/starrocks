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

#include <mutex>
#include <shared_mutex>
#include <vector>

#include "exec/pipeline/primitives/pipeline_observer.h"
#include "runtime/runtime_state.h"

namespace starrocks::spill {

// The notification layer a Spiller owns. It keeps two observer lists -- sink-side and source-side -- and
// wakes them when spill IO completes. A sink-side sleeper (an operator parked OUTPUT_FULL, e.g. on a full
// writer or a busy spill channel) is woken with sink_trigger(); a source-side sleeper (an operator parked
// INPUT_EMPTY waiting on restore/flush IO) is woken with source_trigger(). Each list must be woken with its
// own trigger; the wrong trigger does not wake the sleeper.
//
// notify_*_observers() runs under a shared lock, so several notifies can run at once. detach_observers()
// takes the exclusive lock to clear the lists, so it cannot run while a notify is still iterating -- a
// notify never touches an observer that detach is removing. The lock is taken once per IO completion (not
// per chunk) and the fan-out is small (<= 3 observers per spiller), so it is cheap.
//
// This is deliberately not pipeline::PipeObservable: its defer_notify_sink() wakes the sink list with
// source_trigger(), the wrong trigger here, so reusing it would lose every OUTPUT_FULL wakeup.
class SpillEventObservable {
public:
    // Subscribes a sink observer. A no-op in poller mode (event scheduler off): there the driver's observer
    // is never set, so subscribing a null pointer would crash the later notify. This gate lets the operator
    // call subscribe unconditionally. Prepare-phase, single-threaded.
    void subscribe_sink(RuntimeState* state, pipeline::PipelineObserver* o) {
        if (!state->enable_event_scheduler()) {
            return;
        }
        DCHECK(o != nullptr);
        std::unique_lock l(_mu);
        _sink_observers.push_back(o);
    }

    void subscribe_source(RuntimeState* state, pipeline::PipelineObserver* o) {
        if (!state->enable_event_scheduler()) {
            return;
        }
        DCHECK(o != nullptr);
        std::unique_lock l(_mu);
        _source_observers.push_back(o);
    }

    // Callable from any thread, but only after the state this predicate reads has been published.
    void notify_sink_observers() {
        std::shared_lock l(_mu);
        for (auto* o : _sink_observers) {
            o->sink_trigger();
        }
    }

    void notify_source_observers() {
        std::shared_lock l(_mu);
        for (auto* o : _source_observers) {
            o->source_trigger();
        }
    }

    // Takes the exclusive lock so it cannot race a live notify.
    //
    // On the normal spill path detach_observers is deliberately not called: a notify can fire on the
    // observer of an already-finished driver, and that is safe. Memory-wise the observer is alive because
    // every notify runs inside the IO task's query pin (the ResourceMemTrackerGuard window), and drivers
    // are torn down only with the QueryContext; behavior-wise a trigger on a non-blocked driver only sets
    // need_check_reschedule. The exclusive lock here protects the off-path callers (reset_state / explicit
    // teardown); the normal spill path never reaches it.
    void detach_observers() {
        std::unique_lock l(_mu);
        _sink_observers.clear();
        _source_observers.clear();
    }

    // test accessor: total subscriptions across both lists.
    size_t observer_count() const {
        std::shared_lock l(_mu);
        return _sink_observers.size() + _source_observers.size();
    }

private:
    mutable std::shared_mutex _mu;
    std::vector<pipeline::PipelineObserver*> _sink_observers;
    std::vector<pipeline::PipelineObserver*> _source_observers;
};

} // namespace starrocks::spill
