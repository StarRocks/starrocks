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

#include <string>
#include <vector>

#include "base/utility/defer_op.h"
#include "gutil/macros.h"
#include "runtime/runtime_fwd.h"

namespace starrocks::pipeline {

class PipelineObserver {
public:
    virtual ~PipelineObserver() = default;

    virtual void source_trigger() = 0;
    virtual void sink_trigger() = 0;
    virtual void cancel_trigger() = 0;
    virtual void all_trigger() = 0;
    virtual void runtime_filter_timeout_trigger() = 0;
    virtual std::string debug_string() const = 0;
};

class Observable {
public:
    Observable() = default;
    Observable(const Observable&) = delete;
    Observable& operator=(const Observable&) = delete;

    // Non-thread-safe, we only allow the need to do this in the fragment->prepare phase.
    void add_observer(RuntimeState* state, PipelineObserver* observer);

    void detach_observers() { _observers.clear(); }

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

// Lots of simple operators use a sink -> source one-to-one pipeline.
// We use this to simplify the development of this class of operators.
class PipeObservable {
public:
    void attach_sink_observer(RuntimeState* state, pipeline::PipelineObserver* observer);
    void attach_source_observer(RuntimeState* state, pipeline::PipelineObserver* observer);

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
