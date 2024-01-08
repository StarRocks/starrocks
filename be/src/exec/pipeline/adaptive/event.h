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
#include <memory>
#include <vector>

#include "exec/pipeline/adaptive/adaptive_fwd.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

/// Event is used to drive the execution of pipelines.
/// Event::process is invoked when its dependencies are all finished.
class Event : public std::enable_shared_from_this<Event> {
public:
    Event() = default;
    virtual ~Event() = default;

    /// This is not thread-safe.
    virtual void process(RuntimeState* state) {}

    /// This is thread-safe.
    void finish(RuntimeState* state);

    /// This is thread-safe.
    void finish_dependency(RuntimeState* state);

    /// This is not thread-safe.
    void add_dependency(Event* event);

    std::string to_string() const;
    virtual std::string name() const { return "base_event"; }

public:
    static EventPtr create_event();
    static EventPtr create_collect_stats_source_initialize_event(DriverExecutor* executor,
                                                                 std::vector<Pipeline*>&& pipelines);
    static EventPtr depends_all(const std::vector<EventPtr>& events);

protected:
    size_t _num_dependencies{0};
    std::vector<std::weak_ptr<Event>> _dependees;

    std::atomic<bool> _finished{false};
    std::atomic<size_t> _num_finished_dependencies{0};
};

} // namespace starrocks::pipeline
