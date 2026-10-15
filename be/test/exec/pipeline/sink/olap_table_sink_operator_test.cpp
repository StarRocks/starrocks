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

#include "exec/pipeline/sink/olap_table_sink_operator.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <new>
#include <thread>

#include "base/concurrency/await.h"
#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "common/thread/threadpool.h"
#include "exec/exec_env.h"
#include "exec/pipeline/fragment_context.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"

namespace starrocks::pipeline {
namespace {

using namespace std::chrono_literals;

// An AsyncDataSink whose channels are closed as soon as is_close_done() is asked, and whose final close() can be
// held until the test releases it, standing in for a slow combined txn log write.
class SlowCloseSink final : public AsyncDataSink {
public:
    Status prepare(RuntimeState* state) override { return Status::OK(); }
    Status open(RuntimeState* state) override { return Status::OK(); }
    RuntimeProfile* profile() override { return &_profile; }
    Status try_open(RuntimeState* state) override { return Status::OK(); }
    bool is_open_done() override { return true; }
    Status open_wait() override { return Status::OK(); }
    bool is_full() override { return false; }
    Status send_chunk_nonblocking(RuntimeState* state, Chunk* chunk) override { return Status::OK(); }
    Status try_close(RuntimeState* state) override { return Status::OK(); }
    Status close_wait(RuntimeState* state, Status close_status) override { return close_status; }
    bool is_close_done() override { return channels_closed; }
    void set_profile(RuntimeProfile* profile) override {}

    Status close(RuntimeState* state, const Status& exec_status) override {
        if (closes_in_flight.fetch_add(1) > 0) {
            concurrent_close = true;
        }
        close_calls++;
        last_exec_status = exec_status;
        close_thread = std::this_thread::get_id();
        close_started.set_value();
        if (hold_close) {
            // Bounded, so that a close which wrongly runs on the caller's thread fails the test instead of hanging it.
            release_close_future.wait_for(10s);
        }
        closes_in_flight--;
        return close_result;
    }

    std::atomic<bool> channels_closed = true;
    bool hold_close = false;
    Status close_result = Status::OK();

    std::promise<void> close_started;
    std::promise<void> release_close;
    std::shared_future<void> release_close_future = release_close.get_future().share();

    std::atomic<int> close_calls = 0;
    std::atomic<int> closes_in_flight = 0;
    std::atomic<bool> concurrent_close = false;
    Status last_exec_status;
    std::thread::id close_thread;

private:
    RuntimeProfile _profile{"SlowCloseSink"};
};

class NoopOperatorFactory final : public OperatorFactory {
public:
    NoopOperatorFactory() : OperatorFactory(0, "olap_table_sink", Operator::s_pseudo_plan_node_id_for_final_sink) {}
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override { return nullptr; }
};

class OlapTableSinkOperatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto* exec_env = ExecEnv::GetInstance();
        _fragment_ctx = _pool.add(new FragmentContext);
        _fragment_ctx->set_runtime_state(
                std::make_shared<RuntimeState>(TUniqueId(), TUniqueId(), TQueryOptions(), TQueryGlobals(), exec_env));
        _runtime_state = _fragment_ctx->runtime_state();
        _runtime_state->set_fragment_ctx(_fragment_ctx, &_fragment_ctx->fragment_runtime_state());
        _runtime_state->set_query_execution_services(&exec_env->query_execution_services());
        _query_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY, -1, "olap_table_sink_operator_ut",
                                                      RuntimeEnv::GetInstance()->process_mem_tracker());
        _runtime_state->init_mem_trackers(_query_tracker);

        _op = std::make_shared<OlapTableSinkOperator>(&_factory, 0, Operator::s_pseudo_plan_node_id_for_final_sink, 0,
                                                      0, &_sink, _fragment_ctx, _num_sinkers);
        ASSERT_OK(_op->prepare(_runtime_state));
    }

    // Polls pending_finish() the way the pipeline poller does, until it reports the driver can finish.
    bool wait_until_close_finished() {
        for (int i = 0; i < 10000; ++i) {
            if (!_op->pending_finish()) {
                return true;
            }
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    ObjectPool _pool;
    FragmentContext* _fragment_ctx = nullptr;
    RuntimeState* _runtime_state = nullptr;
    std::shared_ptr<MemTracker> _query_tracker;
    NoopOperatorFactory _factory;
    std::atomic<int32_t> _num_sinkers = 1;
    SlowCloseSink _sink;
    std::shared_ptr<OlapTableSinkOperator> _op;
};

TEST_F(OlapTableSinkOperatorTest, waits_for_channels_before_closing) {
    _sink.channels_closed = false;
    EXPECT_TRUE(_op->pending_finish());
    EXPECT_TRUE(_op->pending_finish());
    EXPECT_EQ(0, _sink.close_calls);

    _sink.channels_closed = true;
    ASSERT_TRUE(wait_until_close_finished());
    EXPECT_EQ(1, _sink.close_calls);
    EXPECT_FALSE(_runtime_state->is_cancelled());
}

// The close writes the combined txn log to object storage. It must not run on the thread that calls
// pending_finish(), which is the pipeline poller shared by every blocked driver.
TEST_F(OlapTableSinkOperatorTest, close_does_not_block_the_caller) {
    _sink.hold_close = true;

    EXPECT_TRUE(_op->pending_finish());
    ASSERT_EQ(std::future_status::ready, _sink.close_started.get_future().wait_for(10s));
    EXPECT_NE(std::this_thread::get_id(), _sink.close_thread);
    // Still closing: the driver stays pending, and the sink is not closed a second time.
    EXPECT_TRUE(_op->pending_finish());
    EXPECT_TRUE(_op->pending_finish());

    _sink.release_close.set_value();
    ASSERT_TRUE(wait_until_close_finished());
    EXPECT_EQ(1, _sink.close_calls);
    EXPECT_FALSE(_sink.concurrent_close);
    EXPECT_FALSE(_runtime_state->is_cancelled());
}

TEST_F(OlapTableSinkOperatorTest, close_failure_cancels_the_fragment) {
    _sink.close_result = Status::IOError("injected combined txn log write failure");

    ASSERT_TRUE(wait_until_close_finished());
    EXPECT_TRUE(_runtime_state->is_cancelled());
    EXPECT_TRUE(_fragment_ctx->final_status().is_io_error()) << _fragment_ctx->final_status();
}

// A cancellation that arrives while the close runs must leave the sink to the close task, and the driver must stay
// pending until that task is done, because finishing the driver frees the sink.
TEST_F(OlapTableSinkOperatorTest, cancel_while_closing_waits_for_the_close) {
    _sink.hold_close = true;

    EXPECT_TRUE(_op->pending_finish());
    ASSERT_EQ(std::future_status::ready, _sink.close_started.get_future().wait_for(10s));

    _fragment_ctx->cancel(Status::Cancelled("cancelled by test"));
    EXPECT_OK(_op->set_cancelled(_runtime_state));
    EXPECT_TRUE(_op->pending_finish());
    EXPECT_EQ(1, _sink.close_calls);

    _sink.release_close.set_value();
    ASSERT_TRUE(wait_until_close_finished());
    EXPECT_EQ(1, _sink.close_calls);
    EXPECT_FALSE(_sink.concurrent_close);
}

// A fragment cancelled before its close runs must not write the combined txn log: the close task has to hand the
// cancellation to the sink instead of closing it successfully.
TEST_F(OlapTableSinkOperatorTest, close_of_a_cancelled_fragment_passes_the_cancellation) {
    _fragment_ctx->cancel(Status::Cancelled("cancelled by test"));

    ASSERT_TRUE(wait_until_close_finished());
    EXPECT_EQ(1, _sink.close_calls);
    EXPECT_TRUE(_sink.last_exec_status.is_cancelled()) << _sink.last_exec_status;
}

TEST_F(OlapTableSinkOperatorTest, closes_inline_without_a_close_pool) {
    _runtime_state->set_query_execution_services(nullptr);

    EXPECT_FALSE(_op->pending_finish());
    EXPECT_EQ(1, _sink.close_calls);
    EXPECT_EQ(std::this_thread::get_id(), _sink.close_thread);
}

TEST_F(OlapTableSinkOperatorTest, enqueue_exception_returns_close_ownership_to_cancellation) {
    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:before_push_entry",
                                          [](void*) { throw std::bad_alloc(); });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp clear_sync_point([]() {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:before_push_entry");
    });

    EXPECT_THROW((void)_op->pending_finish(), std::bad_alloc);
    SyncPoint::GetInstance()->DisableProcessing();
    EXPECT_EQ(0, _sink.close_calls);
    _fragment_ctx->cancel(Status::Cancelled("close submission allocation failed"));
    EXPECT_OK(_op->set_cancelled(_runtime_state));
    EXPECT_EQ(1, _sink.close_calls);
    EXPECT_TRUE(_sink.last_exec_status.is_cancelled()) << _sink.last_exec_status;
}

TEST_F(OlapTableSinkOperatorTest, enqueue_exception_allows_a_later_close_to_finish) {
    SyncPoint::GetInstance()->SetCallBack("ThreadPool::do_submit:before_push_entry",
                                          [](void*) { throw std::bad_alloc(); });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp clear_sync_point([]() {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearCallBack("ThreadPool::do_submit:before_push_entry");
    });

    EXPECT_THROW((void)_op->pending_finish(), std::bad_alloc);
    SyncPoint::GetInstance()->DisableProcessing();
    ASSERT_TRUE(wait_until_close_finished());
    EXPECT_EQ(1, _sink.close_calls);
    EXPECT_FALSE(_runtime_state->is_cancelled());
}

TEST_F(OlapTableSinkOperatorTest, shutdown_joins_running_close_and_cancels_queued_close_before_context_destruction) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_OK(ThreadPoolBuilder("load_sink_close_shutdown_ut").set_min_threads(1).set_max_threads(1).build(&pool));
    auto* original_services = _runtime_state->query_execution_services();
    auto execution = *original_services->execution;
    execution.load_sink_close_thread_pool = pool.get();
    auto services = *original_services;
    services.execution = &execution;
    _runtime_state->set_query_execution_services(&services);

    SlowCloseSink queued_sink;
    auto queued_op =
            std::make_shared<OlapTableSinkOperator>(&_factory, 0, Operator::s_pseudo_plan_node_id_for_final_sink, 0, 0,
                                                    &queued_sink, _fragment_ctx, _num_sinkers);
    std::future<void> shutdown;
    bool released = false;
    auto release_running_close = [&]() {
        if (!released) {
            released = true;
            _sink.release_close.set_value();
        }
    };
    DeferOp cleanup([&]() {
        release_running_close();
        if (shutdown.valid()) shutdown.wait();
        pool->shutdown();
        if (_runtime_state != nullptr) _runtime_state->set_query_execution_services(original_services);
    });
    ASSERT_OK(queued_op->prepare(_runtime_state));
    _sink.hold_close = true;

    EXPECT_TRUE(_op->pending_finish());
    ASSERT_EQ(std::future_status::ready, _sink.close_started.get_future().wait_for(10s));
    EXPECT_TRUE(queued_op->pending_finish());
    EXPECT_EQ(0, queued_sink.close_calls);

    // As at process shutdown, submissions have stopped but the query's objects are still alive.
    shutdown = std::async(std::launch::async, [&]() { pool->shutdown(); });
    ASSERT_TRUE(Awaitility().timeout(5000000).until([&]() { return !pool->is_pool_status_ok(); }));
    EXPECT_EQ(std::future_status::timeout, shutdown.wait_for(20ms));
    EXPECT_TRUE(_op->pending_finish());
    release_running_close();
    ASSERT_EQ(std::future_status::ready, shutdown.wait_for(10s));
    shutdown.get();

    EXPECT_FALSE(_op->pending_finish());
    EXPECT_FALSE(queued_op->pending_finish());
    EXPECT_EQ(1, _sink.close_calls);
    EXPECT_EQ(0, queued_sink.close_calls);
    EXPECT_TRUE(_fragment_ctx->final_status().is_cancelled()) << _fragment_ctx->final_status();
    // Only after shutdown has joined the task and run its queued cancellation callback may contexts be freed.
    _runtime_state->set_query_execution_services(original_services);
    queued_op.reset();
    _op.reset();
    _pool.clear();
    _runtime_state = nullptr;
    _fragment_ctx = nullptr;
}

} // namespace
} // namespace starrocks::pipeline
