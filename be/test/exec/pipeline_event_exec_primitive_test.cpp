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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "exec/pipeline/primitives/event.h"

namespace starrocks::pipeline {
namespace {

class CountingEvent final : public Event {
public:
    void process(RuntimeState* state) override {
        last_state = state;
        ++process_count;
    }

    std::string name() const override { return "counting_event"; }

    int process_count = 0;
    RuntimeState* last_state = nullptr;
};

TEST(PipelineEventExecPrimitiveTest, DependentEventProcessesAfterDependencyFinishes) {
    auto dependency = Event::create_event();
    auto dependent = std::make_shared<CountingEvent>();
    dependent->add_dependency(dependency.get());

    EXPECT_FALSE(dependent->dependencies_finished());

    dependency->finish(nullptr);

    EXPECT_TRUE(dependent->dependencies_finished());
    EXPECT_EQ(dependent->process_count, 1);
    EXPECT_EQ(dependent->last_state, nullptr);
}

TEST(PipelineEventExecPrimitiveTest, FinishingDependencyTwiceDoesNotProcessDependentTwice) {
    auto dependency = Event::create_event();
    auto dependent = std::make_shared<CountingEvent>();
    dependent->add_dependency(dependency.get());

    dependency->finish(nullptr);
    dependency->finish(nullptr);

    EXPECT_TRUE(dependent->dependencies_finished());
    EXPECT_EQ(dependent->process_count, 1);
}

TEST(PipelineEventExecPrimitiveTest, DependsAllWaitsForAllEventsBeforeFinishing) {
    auto first = Event::create_event();
    auto second = Event::create_event();
    auto merged = Event::depends_all(std::vector<EventPtr>{first, second});
    auto dependent = std::make_shared<CountingEvent>();
    dependent->add_dependency(merged.get());

    first->finish(nullptr);
    EXPECT_FALSE(dependent->dependencies_finished());
    EXPECT_EQ(dependent->process_count, 0);

    second->finish(nullptr);
    EXPECT_TRUE(dependent->dependencies_finished());
    EXPECT_EQ(dependent->process_count, 1);
}

} // namespace
} // namespace starrocks::pipeline
