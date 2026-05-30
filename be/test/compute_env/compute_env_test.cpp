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

#include "compute_env/compute_env.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>

#include "base/testutil/assert.h"
#include "compute_env/pipeline/driver_limiter.h"
#include "compute_env/pipeline/observer.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {

class FakePipelineObserver final : public pipeline::PipelineObserver {
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

} // namespace

TEST(ComputeEnvTest, DriverLimiterLifecycle) {
    ComputeEnv env;
    ComputeEnvOptions options;
    options.max_num_pipeline_drivers = 4;

    ASSERT_OK(env.init(options));
    ASSERT_NE(env.driver_limiter(), nullptr);
    ASSERT_NE(env.pipeline_timer(), nullptr);
    ASSERT_NE(env.stream_mgr(), nullptr);
    ASSERT_NE(env.result_mgr(), nullptr);
    ASSERT_NE(env.result_queue_mgr(), nullptr);

    auto token_or = env.driver_limiter()->try_acquire(3);
    ASSERT_TRUE(token_or.ok()) << token_or.status();
    auto token = std::move(token_or).value();
    EXPECT_EQ(env.driver_limiter()->num_total_drivers(), 3);

    auto overloaded_token_or = env.driver_limiter()->try_acquire(2);
    ASSERT_FALSE(overloaded_token_or.ok());
    EXPECT_EQ(overloaded_token_or.status().code(), TStatusCode::TOO_MANY_TASKS);
    EXPECT_EQ(env.driver_limiter()->num_total_drivers(), 3);

    token.reset();
    EXPECT_EQ(env.driver_limiter()->num_total_drivers(), 0);

    env.destroy();
    EXPECT_EQ(env.driver_limiter(), nullptr);
    EXPECT_EQ(env.pipeline_timer(), nullptr);
    EXPECT_EQ(env.stream_mgr(), nullptr);
    EXPECT_EQ(env.result_mgr(), nullptr);
    EXPECT_EQ(env.result_queue_mgr(), nullptr);
}

TEST(ComputeEnvTest, ObservableNotifiesObservers) {
    RuntimeState state;
    state.set_enable_event_scheduler(true);
    pipeline::Observable observable;
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

TEST(ComputeEnvTest, ObservableSkipsObserversWhenEventSchedulerDisabled) {
    RuntimeState state;
    state.set_enable_event_scheduler(false);
    pipeline::Observable observable;
    FakePipelineObserver observer("fake_driver");

    observable.add_observer(&state, &observer);
    EXPECT_EQ(observable.num_observers(), 0);
}

TEST(ComputeEnvTest, PipeObservableNotifiesAttachedSides) {
    RuntimeState state;
    state.set_enable_event_scheduler(true);
    pipeline::PipeObservable pipe_observable;
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

} // namespace starrocks
