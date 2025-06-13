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

#include <thread>

#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/pipeline_metrics.h"
#include "exec/workgroup/work_group.h"
#include "testutil/parallel_test.h"

namespace starrocks::pipeline {

class MockEmptyOperator final : public SourceOperator {
public:
    MockEmptyOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "mock_empty_operator", plan_node_id, false, driver_sequence) {}

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

PARALLEL_TEST(QuerySharedDriverQueueTest, test_basic) {
    PipelineExecutorMetrics metrics;
    QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());

    // Prepare drivers.
    QueryContext query_context;
    auto driver71 = std::make_shared<PipelineDriver>(_gen_operators(), &query_context, nullptr, nullptr, -1);
    _set_driver_level(driver71.get(), 7);
    driver71->driver_acct().update_last_time_spent(5'000'000L * 1);

    auto driver72 = std::make_shared<PipelineDriver>(_gen_operators(), &query_context, nullptr, nullptr, -1);
    _set_driver_level(driver72.get(), 7 + QuerySharedDriverQueue::QUEUE_SIZE);
    driver72->driver_acct().update_last_time_spent(5'000'000L * 1);

    auto driver61 = std::make_shared<PipelineDriver>(_gen_operators(), &query_context, nullptr, nullptr, -1);
    _set_driver_level(driver61.get(), 6);
    driver61->driver_acct().update_last_time_spent(30'000'000L * QuerySharedDriverQueue::ratio_of_adjacent_queue());

    auto driver51 = std::make_shared<PipelineDriver>(_gen_operators(), &query_context, nullptr, nullptr, -1);
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
    auto driver1 = std::make_shared<PipelineDriver>(_gen_operators(), nullptr, nullptr, nullptr, -1);
    _set_driver_level(driver1.get(), 1);
    auto driver2 = std::make_shared<PipelineDriver>(_gen_operators(), nullptr, nullptr, nullptr, -1);
    _set_driver_level(driver2.get(), 1);
    auto driver3 = std::make_shared<PipelineDriver>(_gen_operators(), nullptr, nullptr, nullptr, -1);
    _set_driver_level(driver3.get(), 1);
    auto driver4 = std::make_shared<PipelineDriver>(_gen_operators(), nullptr, nullptr, nullptr, -1);
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
    auto driver1 = std::make_shared<PipelineDriver>(_gen_operators(), &query_context, nullptr, nullptr, -1);
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
                                                      1.0, workgroup::WorkGroupType::WG_NORMAL);
        _wg2 = std::make_shared<workgroup::WorkGroup>("wg200", 200, workgroup::WorkGroup::DEFAULT_VERSION, 2, 0.5, 10,
                                                      1.0, workgroup::WorkGroupType::WG_NORMAL);
        _wg3 = std::make_shared<workgroup::WorkGroup>("wg300", 300, workgroup::WorkGroup::DEFAULT_VERSION, 1, 0.5, 10,
                                                      1.0, workgroup::WorkGroupType::WG_NORMAL);
        _wg4 = std::make_shared<workgroup::WorkGroup>("wg400", 400, workgroup::WorkGroup::DEFAULT_VERSION, 1, 0.5, 10,
                                                      1.0, workgroup::WorkGroupType::WG_NORMAL);
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
};

TEST_F(WorkGroupDriverQueueTest, test_basic) {
    QueryContext query_ctx;
    PipelineExecutorMetrics metrics;
    WorkGroupDriverQueue queue(metrics.get_driver_queue_metrics());

    // Prepare drivers for _wg2.
    int64_t sum_wg2_time_spent = 0;
    auto driver271 = std::make_shared<PipelineDriver>(_gen_operators(), &query_ctx, nullptr, nullptr, -1);
    _set_driver_level(driver271.get(), 7);
    driver271->driver_acct().update_last_time_spent(5'000'000L * 1);
    driver271->set_workgroup(_wg2);
    sum_wg2_time_spent += 5'000'000L * 1;

    auto driver272 = std::make_shared<PipelineDriver>(_gen_operators(), &query_ctx, nullptr, nullptr, -1);
    _set_driver_level(driver272.get(), 7 + QuerySharedDriverQueue::QUEUE_SIZE);
    driver272->driver_acct().update_last_time_spent(5'000'000L * 1);
    driver272->set_workgroup(_wg2);
    sum_wg2_time_spent += 5'000'000L * 1;

    auto driver261 = std::make_shared<PipelineDriver>(_gen_operators(), &query_ctx, nullptr, nullptr, -1);
    _set_driver_level(driver261.get(), 6);
    driver261->driver_acct().update_last_time_spent(30'000'000L * QuerySharedDriverQueue::ratio_of_adjacent_queue());
    driver261->set_workgroup(_wg2);
    sum_wg2_time_spent += 30'000'000L * QuerySharedDriverQueue::ratio_of_adjacent_queue();

    auto driver251 = std::make_shared<PipelineDriver>(_gen_operators(), &query_ctx, nullptr, nullptr, -1);
    _set_driver_level(driver251.get(), 5);
    driver251->driver_acct().update_last_time_spent(20'000'000L * QuerySharedDriverQueue::ratio_of_adjacent_queue() *
                                                    QuerySharedDriverQueue::ratio_of_adjacent_queue());
    driver251->set_workgroup(_wg2);
    sum_wg2_time_spent += 20'000'000L * QuerySharedDriverQueue::ratio_of_adjacent_queue() *
                          QuerySharedDriverQueue::ratio_of_adjacent_queue();

    // Prepare drivers for _wg1.
    auto driver1 = std::make_shared<PipelineDriver>(_gen_operators(), &query_ctx, nullptr, nullptr, -1);
    _set_driver_level(driver1.get(), 1);
    driver1->driver_acct().update_last_time_spent(sum_wg2_time_spent / 2 - 10'000'000L);
    driver1->set_workgroup(_wg1);

    // Prepare drivers for _wg3.
    auto driver3 = std::make_shared<PipelineDriver>(_gen_operators(), &query_ctx, nullptr, nullptr, -1);
    _set_driver_level(driver3.get(), 2);
    driver3->driver_acct().update_last_time_spent(sum_wg2_time_spent * 2 + 1);
    driver3->set_workgroup(_wg3);

    // Prepare drivers for _wg4.
    auto driver4 = std::make_shared<PipelineDriver>(_gen_operators(), &query_ctx, nullptr, nullptr, -1);
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
    WorkGroupDriverQueue queue(metrics.get_driver_queue_metrics());

    // Prepare drivers.
    auto driver1 = std::make_shared<PipelineDriver>(_gen_operators(), &query_ctx, nullptr, nullptr, -1);
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

TEST_F(WorkGroupDriverQueueTest, test_take_close) {
    PipelineExecutorMetrics metrics;
    WorkGroupDriverQueue queue(metrics.get_driver_queue_metrics());

    auto consumer_thread = std::make_shared<std::thread>([&queue] {
        auto maybe_driver = queue.take(true);
        ASSERT_TRUE(maybe_driver.status().is_cancelled());
    });

    sleep(1);
    queue.close();

    consumer_thread->join();
}

} // namespace starrocks::pipeline
