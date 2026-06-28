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

#include <set>
#include <string>
#include <utility>

#include "exec/pipeline/primitives/pipeline_observer.h"
#include "exec/pipeline/runtime_filter_hub.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
namespace {

class FakePipelineObserver final : public PipelineObserver {
public:
    explicit FakePipelineObserver(std::string debug_string) : _debug_string(std::move(debug_string)) {}

    void source_trigger() override { ++source_trigger_count; }
    void sink_trigger() override { ++sink_trigger_count; }
    void cancel_trigger() override { ++cancel_trigger_count; }
    void all_trigger() override { ++all_trigger_count; }
    void runtime_filter_timeout_trigger() override { ++runtime_filter_timeout_count; }
    std::string debug_string() const override { return _debug_string; }

    int source_trigger_count = 0;
    int sink_trigger_count = 0;
    int cancel_trigger_count = 0;
    int all_trigger_count = 0;
    int runtime_filter_timeout_count = 0;

private:
    std::string _debug_string;
};

TEST(PipelineObserverExecPrimitiveTest, ObservableNotifiesObservers) {
    RuntimeState state;
    state.set_enable_event_scheduler(true);
    Observable observable;
    FakePipelineObserver observer("fake_driver");

    observable.add_observer(&state, &observer);
    EXPECT_EQ(observable.num_observers(), 1);
    EXPECT_EQ(observable.to_string(), "fake_driver\n");

    observable.notify_source_observers();
    observable.notify_sink_observers();
    observable.notify_runtime_filter_timeout();
    EXPECT_EQ(observer.source_trigger_count, 1);
    EXPECT_EQ(observer.sink_trigger_count, 1);
    EXPECT_EQ(observer.runtime_filter_timeout_count, 1);

    observable.detach_observers();
    EXPECT_EQ(observable.num_observers(), 0);
    observable.notify_source_observers();
    EXPECT_EQ(observer.source_trigger_count, 1);
}

TEST(PipelineObserverExecPrimitiveTest, ObservableSkipsObserversWhenEventSchedulerDisabled) {
    RuntimeState state;
    state.set_enable_event_scheduler(false);
    Observable observable;
    FakePipelineObserver observer("fake_driver");

    observable.add_observer(&state, &observer);
    EXPECT_EQ(observable.num_observers(), 0);
}

TEST(PipelineObserverExecPrimitiveTest, PipeObservableNotifiesAttachedSides) {
    RuntimeState state;
    state.set_enable_event_scheduler(true);
    PipeObservable pipe_observable;
    FakePipelineObserver sink_observer("sink_driver");
    FakePipelineObserver source_observer("source_driver");

    pipe_observable.attach_sink_observer(&state, &sink_observer);
    pipe_observable.attach_source_observer(&state, &source_observer);

    { auto notify = pipe_observable.defer_notify_source(); }
    EXPECT_EQ(source_observer.source_trigger_count, 1);
    EXPECT_EQ(sink_observer.source_trigger_count, 0);

    { auto notify = pipe_observable.defer_notify_sink(); }
    EXPECT_EQ(sink_observer.source_trigger_count, 1);
    EXPECT_EQ(sink_observer.sink_trigger_count, 0);
}

TEST(PipelineObserverExecPrimitiveTest, RuntimeFilterHubInstallsCollectorAndNotifiesObserver) {
    RuntimeState state;
    state.set_enable_event_scheduler(true);
    RuntimeFilterHub hub;
    hub.add_holder(7);

    auto holders = hub.gather_holders(std::set<TPlanNodeId>{7}, 0);
    ASSERT_EQ(holders.size(), 1);
    EXPECT_FALSE(holders[0]->is_ready());

    FakePipelineObserver observer("runtime_filter_waiter");
    holders[0]->add_observer(&state, &observer);

    hub.set_collector(7, std::make_unique<RuntimeFilterCollector>(RuntimeInFilterList{}));

    EXPECT_TRUE(holders[0]->is_ready());
    EXPECT_EQ(observer.source_trigger_count, 1);
}

TEST(PipelineObserverExecPrimitiveTest, RuntimeFilterHubSupportsPipelineLevelHolders) {
    RuntimeFilterHub hub;
    hub.add_holder(11, 2);

    EXPECT_TRUE(hub.is_colocate_runtime_filters(11));
    EXPECT_EQ(hub.gather_holders(std::set<TPlanNodeId>{11}, 0, true).size(), 1);
    EXPECT_EQ(hub.gather_holders(std::set<TPlanNodeId>{11}, 1, true).size(), 1);
    EXPECT_TRUE(hub.gather_holders(std::set<TPlanNodeId>{11}, 2, true).empty());
}

} // namespace
} // namespace starrocks::pipeline
