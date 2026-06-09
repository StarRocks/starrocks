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

#include "exec/pipeline/pipeline_driver_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <functional>
#include <thread>

#include "base/testutil/parallel_test.h"
#include "compute_env/workgroup/work_group.h"
#include "compute_env/workgroup/work_group_manager.h"
#include "compute_env/workgroup/work_group_schedule_policy.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/primitives/pipeline_metrics.h"
#include "exec/pipeline/query_context.h"
#include "runtime/exec_env.h"

namespace starrocks::pipeline {

namespace {

class FakeWorkGroupSchedulePolicy final : public workgroup::WorkGroupSchedulePolicy {
public:
    std::function<bool(const workgroup::WorkGroup*)> should_yield_func = [](const workgroup::WorkGroup*) {
        return false;
    };
    size_t num_workgroups_value = 2;

    bool should_yield(const workgroup::WorkGroup* wg) const override { return should_yield_func(wg); }
    size_t num_workgroups() const override { return num_workgroups_value; }
};

} // namespace

class EmptyOperatorRuntimeAccess final : public OperatorRuntimeAccess {
public:
    void bind_runtime_in_filters(RuntimeState* state, int32_t driver_sequence,
                                 std::vector<ExprContext*>* runtime_in_filters) override {}
    RuntimeFilterProbeCollector* get_runtime_bloom_filters() override { return nullptr; }
    const RuntimeFilterProbeCollector* get_runtime_bloom_filters() const override { return nullptr; }
    const std::vector<SlotId>& get_filter_null_value_columns() const override { return _filter_null_value_columns; }

private:
    std::vector<SlotId> _filter_null_value_columns;
};

EmptyOperatorRuntimeAccess* empty_operator_runtime_access() {
    static EmptyOperatorRuntimeAccess runtime_access;
    return &runtime_access;
}

class MockEmptyOperator final : public Operator {
public:
    MockEmptyOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : Operator(factory, id, "mock_empty_operator", plan_node_id, false, driver_sequence,
                       empty_operator_runtime_access()) {}

    ~MockEmptyOperator() override = default;

    bool has_output() const override { return true; }
    bool need_input() const override { return true; }
    bool is_finished() const override { return true; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }
};

Operators _gen_operators() {
    Operators operators;
    operators.emplace_back(std::make_shared<MockEmptyOperator>(nullptr, 1, 1, 0));
    return operators;
}

void _set_driver_level(DriverRawPtr driver, int level) {
    driver->set_driver_queue_level(level % QuerySharedDriverQueue::QUEUE_SIZE);
}

QueryRuntimeState* _query_runtime_state(QueryContext* query_ctx) {
    return query_ctx == nullptr ? nullptr : &query_ctx->query_runtime_state();
}

PipelineDriverRuntimeContext _driver_runtime_context(QueryContext* query_ctx) {
    PipelineDriverRuntimeContext runtime_context;
    runtime_context.query_ctx = query_ctx;
    runtime_context.query_runtime_state = _query_runtime_state(query_ctx);
    return runtime_context;
}

DriverPtr _make_driver(QueryContext* query_ctx = nullptr) {
    return std::make_shared<PipelineDriver>(_gen_operators(), _driver_runtime_context(query_ctx), -1);
}

PARALLEL_TEST(QuerySharedDriverQueueTest, test_basic) {
    PipelineExecutorMetrics metrics;
    QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());

    // Prepare drivers.
    QueryContext query_context;
    auto driver71 = _make_driver(&query_context);
    _set_driver_level(driver71.get(), 7);
    driver71->driver_acct().update_last_time_spent(5'000'000L * 1);

    auto driver72 = _make_driver(&query_context);
    _set_driver_level(driver72.get(), 7 + QuerySharedDriverQueue::QUEUE_SIZE);
    driver72->driver_acct().update_last_time_spent(5'000'000L * 1);

    auto driver61 = _make_driver(&query_context);
    _set_driver_level(driver61.get(), 6);
    driver61->driver_acct().update_last_time_spent(30'000'000L * QuerySharedDriverQueue::ratio_of_adjacent_queue());

    auto driver51 = _make_driver(&query_context);
    _set_driver_level(driver51.get(), 5);
    driver51->driver_acct().update_last_time_spent(20'000'000L * QuerySharedDriverQueue::ratio_of_adjacent_queue() *
                                                   QuerySharedDriverQueue::ratio_of_adjacent_queue());

    std::vector<DriverRawPtr> in_drivers = {driver71.get(), driver72.get(), driver61.get(), driver51.get()};
    std::vector<DriverRawPtr> out_drivers = {driver71.get(), driver72.get(), driver51.get(), driver61.get()};

    // Put back drivers to queue.
    for (auto* in_driver : in_drivers) {
        queue.update_statistics(in_driver);
        queue.put_back(in_driver);
    }

    // Take drivers from queue.
    for (auto* out_driver : out_drivers) {
        auto maybe_driver = queue.take(true);
        ASSERT_TRUE(maybe_driver.ok());
        ASSERT_EQ(out_driver, maybe_driver.value());
    }
}

PARALLEL_TEST(QuerySharedDriverQueueTest, test_cancel) {
    PipelineExecutorMetrics metrics;
    QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());

    // prepare drivers
    auto driver1 = _make_driver();
    _set_driver_level(driver1.get(), 1);
    auto driver2 = _make_driver();
    _set_driver_level(driver2.get(), 1);
    auto driver3 = _make_driver();
    _set_driver_level(driver3.get(), 1);
    auto driver4 = _make_driver();
    _set_driver_level(driver4.get(), 1);

    auto cancel_operation = [&queue](DriverRawPtr driver) {
        if (driver == nullptr) {
            return;
        }
        queue.cancel(driver);
    };

    std::vector<DriverRawPtr> in_drivers = {driver1.get(), driver2.get(), driver3.get(), driver4.get()};

    std::vector<std::function<void()>> ops_before_get = {
            [cancel_operation] { return cancel_operation(nullptr); },
            [cancel_operation, capture0 = driver4.get()] { return cancel_operation(capture0); },
            [cancel_operation, capture0 = driver3.get()] { return cancel_operation(capture0); },
            [cancel_operation] { return cancel_operation(nullptr); }};

    std::vector<DriverRawPtr> out_drivers = {driver1.get(), driver4.get(), driver3.get(), driver2.get()};

    for (auto* in_driver : in_drivers) {
        queue.put_back(in_driver);
    }

    for (size_t i = 0; i < out_drivers.size(); i++) {
        ops_before_get[i]();
        auto maybe_driver = queue.take(true);
        ASSERT_TRUE(maybe_driver.ok());
        ASSERT_EQ(out_drivers[i], maybe_driver.value());
    }
}

PARALLEL_TEST(QuerySharedDriverQueueTest, test_take_block) {
    PipelineExecutorMetrics metrics;
    QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());

    // Prepare drivers.
    QueryContext query_context;
    auto driver1 = _make_driver(&query_context);
    _set_driver_level(driver1.get(), 1);

    auto consumer_thread = std::make_shared<std::thread>([&queue, &driver1] {
        auto maybe_driver = queue.take(true);
        ASSERT_TRUE(maybe_driver.ok());
        ASSERT_EQ(driver1.get(), maybe_driver.value());
    });

    sleep(1);
    queue.update_statistics(driver1.get());
    queue.put_back(driver1.get());

    consumer_thread->join();
}

PARALLEL_TEST(QuerySharedDriverQueueTest, test_take_close) {
    PipelineExecutorMetrics metrics;
    QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());

    auto consumer_thread = std::make_shared<std::thread>([&queue] {
        auto maybe_driver = queue.take(true);
        ASSERT_TRUE(maybe_driver.status().is_cancelled());
    });

    sleep(1);
    queue.close();

    consumer_thread->join();
}

class WorkGroupDriverQueueTest : public ::testing::Test {
public:
    void SetUp() override {
        _wg1 = std::make_shared<workgroup::WorkGroup>("wg100", 100, workgroup::WorkGroup::DEFAULT_VERSION, 1, 0.5, 10,
                                                      1.0, workgroup::WorkGroupType::WG_NORMAL,
                                                      workgroup::WorkGroup::DEFAULT_MEM_POOL);
        _wg2 = std::make_shared<workgroup::WorkGroup>("wg200", 200, workgroup::WorkGroup::DEFAULT_VERSION, 2, 0.5, 10,
                                                      1.0, workgroup::WorkGroupType::WG_NORMAL,
                                                      workgroup::WorkGroup::DEFAULT_MEM_POOL);
        _wg3 = std::make_shared<workgroup::WorkGroup>("wg300", 300, workgroup::WorkGroup::DEFAULT_VERSION, 1, 0.5, 10,
                                                      1.0, workgroup::WorkGroupType::WG_NORMAL,
                                                      workgroup::WorkGroup::DEFAULT_MEM_POOL);
        _wg4 = std::make_shared<workgroup::WorkGroup>("wg400", 400, workgroup::WorkGroup::DEFAULT_VERSION, 1, 0.5, 10,
                                                      1.0, workgroup::WorkGroupType::WG_NORMAL,
                                                      workgroup::WorkGroup::DEFAULT_MEM_POOL);
        _wg1 = ExecEnv::GetInstance()->workgroup_manager()->add_workgroup(_wg1);
        _wg2 = ExecEnv::GetInstance()->workgroup_manager()->add_workgroup(_wg2);
        _wg3 = ExecEnv::GetInstance()->workgroup_manager()->add_workgroup(_wg3);
        _wg4 = ExecEnv::GetInstance()->workgroup_manager()->add_workgroup(_wg4);
    }

protected:
    workgroup::WorkGroupPtr _wg1 = nullptr;
    workgroup::WorkGroupPtr _wg2 = nullptr;
    workgroup::WorkGroupPtr _wg3 = nullptr;
    workgroup::WorkGroupPtr _wg4 = nullptr;
    FakeWorkGroupSchedulePolicy _schedule_policy;
};

TEST_F(WorkGroupDriverQueueTest, test_basic) {
    QueryContext query_ctx;
    PipelineExecutorMetrics metrics;
    WorkGroupDriverQueue queue(metrics.get_driver_queue_metrics(), _schedule_policy);

    // Prepare drivers for _wg2.
    int64_t sum_wg2_time_spent = 0;
    auto driver271 = _make_driver(&query_ctx);
    _set_driver_level(driver271.get(), 7);
    driver271->driver_acct().update_last_time_spent(5'000'000L * 1);
    driver271->set_workgroup(_wg2);
    sum_wg2_time_spent += 5'000'000L * 1;

    auto driver272 = _make_driver(&query_ctx);
    _set_driver_level(driver272.get(), 7 + QuerySharedDriverQueue::QUEUE_SIZE);
    driver272->driver_acct().update_last_time_spent(5'000'000L * 1);
    driver272->set_workgroup(_wg2);
    sum_wg2_time_spent += 5'000'000L * 1;

    auto driver261 = _make_driver(&query_ctx);
    _set_driver_level(driver261.get(), 6);
    driver261->driver_acct().update_last_time_spent(30'000'000L * QuerySharedDriverQueue::ratio_of_adjacent_queue());
    driver261->set_workgroup(_wg2);
    sum_wg2_time_spent += 30'000'000L * QuerySharedDriverQueue::ratio_of_adjacent_queue();

    auto driver251 = _make_driver(&query_ctx);
    _set_driver_level(driver251.get(), 5);
    driver251->driver_acct().update_last_time_spent(20'000'000L * QuerySharedDriverQueue::ratio_of_adjacent_queue() *
                                                    QuerySharedDriverQueue::ratio_of_adjacent_queue());
    driver251->set_workgroup(_wg2);
    sum_wg2_time_spent += 20'000'000L * QuerySharedDriverQueue::ratio_of_adjacent_queue() *
                          QuerySharedDriverQueue::ratio_of_adjacent_queue();

    // Prepare drivers for _wg1.
    auto driver1 = _make_driver(&query_ctx);
    _set_driver_level(driver1.get(), 1);
    driver1->driver_acct().update_last_time_spent(sum_wg2_time_spent / 2 - 10'000'000L);
    driver1->set_workgroup(_wg1);

    // Prepare drivers for _wg3.
    auto driver3 = _make_driver(&query_ctx);
    _set_driver_level(driver3.get(), 2);
    driver3->driver_acct().update_last_time_spent(sum_wg2_time_spent * 2 + 1);
    driver3->set_workgroup(_wg3);

    // Prepare drivers for _wg4.
    auto driver4 = _make_driver(&query_ctx);
    _set_driver_level(driver4.get(), 2);
    driver4->driver_acct().update_last_time_spent(sum_wg2_time_spent * 2 + 1);
    driver4->set_workgroup(_wg4);

    std::vector<DriverRawPtr> in_drivers = {driver271.get(), driver272.get(), driver261.get(), driver251.get(),
                                            driver1.get(),   driver3.get(),   driver4.get()};
    // wg1.vruntime < wg2.vruntime < wg4.vruntime = wg3.vruntime
    std::vector<DriverRawPtr> out_drivers = {driver1.get(), driver271.get(), driver272.get(), driver251.get(),
                                             driver261.get()};
    // a<b = (a.vruntime!=b.vruntime) ? a.vruntime<b.vruntime : a_ptr < b_ptr
    auto* sched_entity3 = driver3->workgroup()->driver_sched_entity();
    auto* sched_entity4 = driver4->workgroup()->driver_sched_entity();
    if (sched_entity3 < sched_entity4) {
        out_drivers.emplace_back(driver3.get());
        out_drivers.emplace_back(driver4.get());
    } else {
        out_drivers.emplace_back(driver4.get());
        out_drivers.emplace_back(driver3.get());
    }

    // Put back drivers to queue.
    for (auto* in_driver : in_drivers) {
        queue.update_statistics(in_driver);
        queue.put_back(in_driver);
    }

    // Take drivers from queue.
    for (auto* out_driver : out_drivers) {
        auto maybe_driver = queue.take(true);
        ASSERT_TRUE(maybe_driver.ok());
        ASSERT_EQ(out_driver, maybe_driver.value());
    }
}

TEST_F(WorkGroupDriverQueueTest, test_take_block) {
    QueryContext query_ctx;
    PipelineExecutorMetrics metrics;
    WorkGroupDriverQueue queue(metrics.get_driver_queue_metrics(), _schedule_policy);

    // Prepare drivers.
    auto driver1 = _make_driver(&query_ctx);
    _set_driver_level(driver1.get(), 1);
    driver1->set_workgroup(_wg1);

    auto consumer_thread = std::make_shared<std::thread>([&queue, &driver1] {
        auto maybe_driver = queue.take(true);
        ASSERT_TRUE(maybe_driver.ok());
        ASSERT_EQ(driver1.get(), maybe_driver.value());
    });

    sleep(1);
    queue.update_statistics(driver1.get());
    queue.put_back(driver1.get());

    consumer_thread->join();
}

TEST_F(WorkGroupDriverQueueTest, test_take_uses_injected_policy) {
    QueryContext query_ctx;
    PipelineExecutorMetrics metrics;
    std::atomic<bool> block_wg2{true};
    _schedule_policy.should_yield_func = [&](const workgroup::WorkGroup* wg) {
        return wg == _wg2.get() && block_wg2.load(std::memory_order_acquire);
    };
    WorkGroupDriverQueue queue(metrics.get_driver_queue_metrics(), _schedule_policy);

    auto driver = _make_driver(&query_ctx);
    _set_driver_level(driver.get(), 1);
    driver->set_workgroup(_wg2);

    queue.update_statistics(driver.get());
    queue.put_back(driver.get());

    auto blocked = queue.take(false);
    ASSERT_TRUE(blocked.ok());
    ASSERT_EQ(nullptr, blocked.value());

    block_wg2.store(false, std::memory_order_release);

    auto unblocked = queue.take(false);
    ASSERT_TRUE(unblocked.ok());
    ASSERT_EQ(driver.get(), unblocked.value());
}

TEST_F(WorkGroupDriverQueueTest, test_update_statistics_uses_injected_workgroup_count_policy) {
    QueryContext query_ctx;
    PipelineExecutorMetrics metrics;
    _schedule_policy.num_workgroups_value = 1;
    WorkGroupDriverQueue queue(metrics.get_driver_queue_metrics(), _schedule_policy);

    auto driver = _make_driver(&query_ctx);
    _set_driver_level(driver.get(), 1);
    driver->driver_acct().update_last_time_spent(100'000'000L);
    driver->set_workgroup(_wg1);

    const int64_t before = _wg1->driver_sched_entity()->vruntime_ns();
    queue.update_statistics(driver.get());

    ASSERT_EQ(before, _wg1->driver_sched_entity()->vruntime_ns());
}

TEST_F(WorkGroupDriverQueueTest, test_take_close) {
    PipelineExecutorMetrics metrics;
    WorkGroupDriverQueue queue(metrics.get_driver_queue_metrics(), _schedule_policy);

    auto consumer_thread = std::make_shared<std::thread>([&queue] {
        auto maybe_driver = queue.take(true);
        ASSERT_TRUE(maybe_driver.status().is_cancelled());
    });

    sleep(1);
    queue.close();

    consumer_thread->join();
}

} // namespace starrocks::pipeline
