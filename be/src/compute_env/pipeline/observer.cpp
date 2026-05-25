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

#include "compute_env/pipeline/observer.h"

#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

std::string Observable::to_string() const {
    std::string str;
    for (auto* observer : _observers) {
        str += observer->debug_string() + "\n";
    }
    return str;
}

void Observable::add_observer(RuntimeState* state, PipelineObserver* observer) {
    if (state->enable_event_scheduler()) {
        DCHECK(observer != nullptr);
        _observers.push_back(observer);
    }
}

void PipeObservable::attach_sink_observer(RuntimeState* state, pipeline::PipelineObserver* observer) {
    _sink_observable.add_observer(state, observer);
}

void PipeObservable::attach_source_observer(RuntimeState* state, pipeline::PipelineObserver* observer) {
    _source_observable.add_observer(state, observer);
}

void Observable::notify_runtime_filter_timeout() {
    for (auto* observer : _observers) {
        observer->runtime_filter_timeout_trigger();
    }
}

} // namespace starrocks::pipeline
