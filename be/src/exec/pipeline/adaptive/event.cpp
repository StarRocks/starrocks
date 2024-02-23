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

#include "exec/pipeline/adaptive/event.h"

#include <utility>

#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/source_operator.h"
#include "util/failpoint/fail_point.h"

namespace starrocks::pipeline {

// ------------------------------------------------------------------------------------
// Event
// ------------------------------------------------------------------------------------

void Event::finish(RuntimeState* state) {
    if (bool expected_finished = false; !_finished.compare_exchange_strong(expected_finished, true)) {
        return;
    }

    for (auto& dependee_entry : _dependees) {
        auto dependee = dependee_entry.lock();
        if (dependee == nullptr) {
            continue;
        }
        dependee->finish_dependency(state);
    }
}

void Event::finish_dependency(RuntimeState* state) {
    if (_num_finished_dependencies.fetch_add(1) + 1 == _num_dependencies) {
        process(state);
    }
}

void Event::add_dependency(Event* event) {
    _num_dependencies++;
    event->_dependees.emplace_back(shared_from_this());
}

std::string Event::to_string() const {
    return std::string("Event{") + "name=" + name() + ",_num_dependencies=" + std::to_string(_num_dependencies) +
           ",_dependees" + std::to_string(_dependees.size()) +
           ",_num_finished_dependencies=" + std::to_string(_num_finished_dependencies.load()) +
           ",_finished=" + std::to_string(_finished.load()) + "}";
}

// ------------------------------------------------------------------------------------
// DependsAllEventt
// ------------------------------------------------------------------------------------

class DependsAllEvent final : public Event {
public:
    explicit DependsAllEvent(std::vector<EventPtr> events) : _events(std::move(events)) {}

    ~DependsAllEvent() override = default;

    void process(RuntimeState* state) override { finish(state); }

    std::string name() const override { return "depends_all_event"; }

private:
    std::vector<EventPtr> _events;
};

// ------------------------------------------------------------------------------------
// Event factory methods.
// ------------------------------------------------------------------------------------

EventPtr Event::create_event() {
    return std::make_shared<Event>();
}

EventPtr Event::depends_all(const std::vector<EventPtr>& events) {
    EventPtr merged_event = std::make_shared<DependsAllEvent>(events);
    for (const auto& event : events) {
        merged_event->add_dependency(event.get());
    }
    return merged_event;
}

} // namespace starrocks::pipeline
