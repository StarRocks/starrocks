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

#include <functional>
#include <optional>
#include <thread>

#include "base/testutil/assert.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "compute_env/query/query_runtime_state.h"
#include "compute_env/spill/mem_tracker_guard.h"
#include "exec/runtime/pipeline_driver.h"
#include "exec/runtime/schedule/event_scheduler.h"
#include "exec_primitive/pipeline/operator_factory.h"
#include "exec_primitive/pipeline/primitives/driver_queue.h"
#include "runtime/current_thread.h"
#include "runtime/query_context_lifetime.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
namespace {

class MemoryCallbackOperator final : public SourceOperator {
public:
    MemoryCallbackOperator(OperatorFactory* factory, int id)
            : SourceOperator(factory, id, "memory_callback", id, false, 0) {}

    bool has_output() const override { return on_has_output ? on_has_output() : true; }
    bool need_input() const override { return on_need_input ? on_need_input() : true; }
    bool is_finished() const override { return on_is_finished ? on_is_finished() : false; }
    bool pending_finish() const override { return on_pending_finish ? on_pending_finish() : false; }
    Status set_cancelled(RuntimeState*) override {
        if (on_cancel) on_cancel();
        return Status::OK();
    }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState*) override { return nullptr; }

    std::function<bool()> on_has_output;
    std::function<bool()> on_need_input;
    std::function<bool()> on_is_finished;
    std::function<bool()> on_pending_finish;
    std::function<void()> on_cancel;
};

class MemoryCallbackOperatorFactory final : public SourceOperatorFactory {
public:
    MemoryCallbackOperatorFactory() : SourceOperatorFactory(0, "memory_callback", 0) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MemoryCallbackOperator>(this, driver_sequence);
    }
};

class RecordingDriverQueue final : public DriverQueue {
public:
    RecordingDriverQueue() : DriverQueue(nullptr) {}

    void close() override { is_closed = true; }

    void put_back(const DriverRawPtr driver) override {
        if (on_put_back) on_put_back();
        queued_drivers.emplace_back(driver);
        driver->set_in_ready(true);
        driver->set_in_queue(this);
    }

    void put_back(const std::vector<DriverRawPtr>& drivers) override {
        for (auto* driver : drivers) {
            put_back(driver);
        }
    }

    void put_back_from_executor(const DriverRawPtr driver) override { put_back(driver); }

    StatusOr<DriverRawPtr> take(const bool block) override {
        if (queued_drivers.empty()) {
            return Status::InternalError("empty driver queue");
        }
        auto* driver = queued_drivers.front();
        queued_drivers.erase(queued_drivers.begin());
        driver->set_in_ready(false);
        return driver;
    }

    void cancel(DriverRawPtr driver) override {}
    void update_statistics(const DriverRawPtr driver) override {}
    size_t size() const override { return queued_drivers.size(); }
    bool should_yield(const DriverRawPtr driver, int64_t unaccounted_runtime_ns) const override { return false; }

    std::function<void()> on_put_back;
    bool is_closed = false;
    std::vector<DriverRawPtr> queued_drivers;
};

class PipelineDriverMemoryTest : public ::testing::Test {
protected:
    void SetUp() override {
        // This focused binary uses gtest_main rather than ExecEnv, so install the memory-tracker
        // provider just as runtime_test does. Without it, CurrentThread ignores explicit TLS trackers.
        tls_mem_tracker = nullptr;
        CurrentThread::set_mem_tracker_source([]() { return true; }, nullptr);
        _state = std::make_shared<RuntimeState>(TQueryGlobals{});
        _query_tracker = std::make_shared<MemTracker>(-1, "scheduled_query");
        _state->init_mem_trackers(_query_tracker);
        _state->set_query_runtime_state(&_query_runtime_state);
        _state->set_fragment_runtime_state(&_fragment_runtime_state);
        _state->set_query_ctx_lifetime(_query_lifetime);
        _source = std::make_shared<MemoryCallbackOperator>(&_factory, 0);
        _sink = std::make_shared<MemoryCallbackOperator>(&_factory, 1);
        _driver = std::make_unique<PipelineDriver>(Operators{_source, _sink}, &_query_runtime_state,
                                                   &_fragment_runtime_state, nullptr, nullptr, nullptr, -1);
        // These tests exercise scheduler callbacks without preparing a query plan.
        _driver->_runtime_state = _state.get();
        _driver->prepare_profile();
        _driver->_input_empty_timer_sw = _state->obj_pool()->add(new MonotonicStopWatch());
        _driver->_output_full_timer_sw = _state->obj_pool()->add(new MonotonicStopWatch());
        _driver->set_driver_state(DriverState::INPUT_EMPTY);
    }

    void TearDown() override {
        CurrentThread::set_mem_tracker_source(nullptr, nullptr);
        tls_mem_tracker = nullptr;
    }

    // Use the explicit accounting API so this also catches attribution errors in ASAN builds,
    // where malloc hooks are disabled. The async task uses the same guard as spill I/O.
    void check_readiness(bool event_scheduler, bool source_callback, bool ready) {
        constexpr int64_t bytes = 8 * 1024 * 1024;
        auto* owner = _state->instance_mem_tracker();
        if (!source_callback) owner->consume(bytes);
        const int64_t before = owner->consumption();
        std::optional<decltype(RESOURCE_TLS_MEMTRACER_GUARD(_state.get()))> io_guard;
        auto callback = [&]() {
            io_guard.emplace(RESOURCE_TLS_MEMTRACER_GUARD(_state.get()));
            return ready;
        };
        if (source_callback) {
            _source->on_has_output = callback;
        } else {
            _sink->on_need_input = callback;
        }
        MemTracker unrelated(-1, "other_query_or_poller");
        {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&unrelated);
            if (event_scheduler) {
                EXPECT_EQ(ready, _driver->check_is_ready());
            } else {
                auto result = _driver->is_not_blocked();
                ASSERT_OK(result.status());
                EXPECT_EQ(ready, result.value());
            }
            EXPECT_EQ(&unrelated, tls_mem_tracker);
        }
        ASSERT_TRUE(io_guard.has_value());
        std::thread io_thread([&]() {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&unrelated);
            {
                ASSERT_TRUE(io_guard->scoped_begin());
                DEFER_GUARD_END((*io_guard));
                if (source_callback) {
                    CurrentThread::mem_consume_without_cache(bytes);
                } else {
                    CurrentThread::mem_release_without_cache(bytes);
                }
            }
            EXPECT_EQ(&unrelated, tls_mem_tracker);
        });
        io_thread.join();
        EXPECT_EQ(before + (source_callback ? bytes : -bytes), owner->consumption());
        if (source_callback) {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(owner);
            CurrentThread::mem_release_without_cache(bytes);
        }
        EXPECT_EQ(0, unrelated.consumption());
    }

    MemoryCallbackOperatorFactory _factory;
    QueryRuntimeState _query_runtime_state;
    FragmentRuntimeState _fragment_runtime_state;
    std::shared_ptr<QueryContextLifetime> _query_lifetime = std::make_shared<QueryContextLifetime>();
    std::shared_ptr<MemTracker> _query_tracker;
    std::shared_ptr<RuntimeState> _state;
    std::shared_ptr<MemoryCallbackOperator> _source;
    std::shared_ptr<MemoryCallbackOperator> _sink;
    std::unique_ptr<PipelineDriver> _driver;
};

TEST_F(PipelineDriverMemoryTest, PollerSpillFlush) {
    check_readiness(false, false, false);
}

TEST_F(PipelineDriverMemoryTest, PollerSpillRestore) {
    check_readiness(false, true, false);
}

TEST_F(PipelineDriverMemoryTest, EventSpillFlush) {
    check_readiness(true, false, false);
}

TEST_F(PipelineDriverMemoryTest, EventSpillRestore) {
    check_readiness(true, true, true);
}

TEST_F(PipelineDriverMemoryTest, CompletionAndCancellation) {
    auto* owner = _state->instance_mem_tracker();
    MemTracker unrelated(-1, "poller");
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&unrelated);
    auto release = [&]() {
        EXPECT_EQ(owner, tls_mem_tracker);
        return false;
    };
    _source->on_pending_finish = release;
    _sink->on_pending_finish = release;
    EXPECT_FALSE(_driver->is_still_pending_finish());
    EXPECT_EQ(&unrelated, tls_mem_tracker);
    _source->on_cancel = [&]() { EXPECT_EQ(owner, tls_mem_tracker); };
    _sink->on_cancel = _source->on_cancel;
    _driver->cancel_operators(_state.get());
    EXPECT_EQ(&unrelated, tls_mem_tracker);
}

TEST_F(PipelineDriverMemoryTest, FinishedSinkEarlyReturn) {
    MemTracker unrelated(-1, "poller");
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&unrelated);
    _sink->on_is_finished = [&]() {
        EXPECT_EQ(_state->instance_mem_tracker(), tls_mem_tracker);
        return true;
    };
    EXPECT_TRUE(_driver->is_not_blocked().value());
    EXPECT_EQ(&unrelated, tls_mem_tracker);
    EXPECT_TRUE(_driver->check_is_ready());
    EXPECT_EQ(&unrelated, tls_mem_tracker);
}

TEST_F(PipelineDriverMemoryTest, ObserverFromAnotherFragment) {
    RecordingDriverQueue queue;
    EventScheduler scheduler;
    scheduler.attach_queue(&queue);
    _driver->set_observer(scheduler.create_driver_observer(_driver.get()));
    _driver->set_in_blocked(true);
    MemTracker unrelated(-1, "notifying_fragment");
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&unrelated);
    // The observer must restore TLS before publication can let an executor destroy the fragment.
    queue.on_put_back = [&]() { EXPECT_EQ(&unrelated, tls_mem_tracker); };
    // Trace logging also evaluates operator readiness, before EventScheduler::try_schedule.
    const auto old_v = FLAGS_v;
    FLAGS_v = 10;
    DeferOp restore_logging([&]() { FLAGS_v = old_v; });
    int checks = 0;
    _source->on_has_output = [&]() {
        ++checks;
        EXPECT_EQ(_state->instance_mem_tracker(), tls_mem_tracker);
        return true;
    };
    _sink->on_need_input = _source->on_has_output;
    _driver->observer()->all_trigger();
    EXPECT_GT(checks, 0);
    EXPECT_EQ(&unrelated, tls_mem_tracker);
    ASSERT_FALSE(_driver->is_in_blocked());
    ASSERT_OK(queue.take(false).status());
}

} // namespace
} // namespace starrocks::pipeline
