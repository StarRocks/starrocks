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

<<<<<<< HEAD
=======
#include <atomic>
#include <chrono>
#include <cmath>
#include <functional>
#include <limits>
>>>>>>> 5467453 ([BugFix] Keep pipeline driver queue levels from starving level 0 (#80760))
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

PARALLEL_TEST(QuerySharedDriverQueueTest, test_level_catches_up_after_being_empty) {
    PipelineExecutorMetrics metrics;
    QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());
    QueryContext query_context;
    auto new_driver = [&query_context](int level) {
        auto driver = std::make_shared<PipelineDriver>(_gen_operators(), _query_runtime_state(&query_context), nullptr,
                                                       nullptr, nullptr, nullptr, -1);
        _set_driver_level(driver.get(), level);
        return driver;
    };

    // Level 0 has a long history, such as of loads and short queries.
    auto history0 = new_driver(0);
    history0->driver_acct().update_last_time_spent(100'000'000'000L);
    queue.update_statistics(history0.get());
    auto driver0 = new_driver(0);
    queue.put_back(driver0.get());
    ASSERT_EQ(driver0.get(), queue.take(true).value());

    // A heavy query reaches level 4 for the first time and runs for 1s.
    auto driver4 = new_driver(4);
    queue.put_back(driver4.get());
    ASSERT_EQ(driver4.get(), queue.take(true).value());
    driver4->driver_acct().update_last_time_spent(1'000'000'000L);
    queue.update_statistics(driver4.get());

    // Level 4 caught up to level 0 when it became non-empty, so after running 1s it is behind level 0.
    // Without the catch-up, level 4 would be picked first until it ran about 48s more.
    queue.put_back(driver4.get());
    queue.put_back(driver0.get());
    ASSERT_EQ(driver0.get(), queue.take(true).value());
    ASSERT_EQ(driver4.get(), queue.take(true).value());
}

PARALLEL_TEST(QuerySharedDriverQueueTest, test_level_of_driver_running_beyond_int32_ns) {
    PipelineExecutorMetrics metrics;
    QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());
    QueryContext query_context;
    auto driver = std::make_shared<PipelineDriver>(_gen_operators(), _query_runtime_state(&query_context), nullptr,
                                                   nullptr, nullptr, nullptr, -1);
    _set_driver_level(driver.get(), 4);

    // 3.5s is beyond INT32_MAX ns, and is in the time slice of level 5, which is [3.0s, 4.2s).
    driver->driver_acct().update_last_time_spent(3'500'000'000L);
    queue.put_back(driver.get());
    ASSERT_EQ(5, driver->get_driver_queue_level());
    ASSERT_EQ(driver.get(), queue.take(true).value());

    driver->driver_acct().update_last_time_spent(5'000'000'000L);
    queue.put_back(driver.get());
    ASSERT_EQ(QuerySharedDriverQueue::QUEUE_SIZE - 1, driver->get_driver_queue_level());
    ASSERT_EQ(driver.get(), queue.take(true).value());
}

PARALLEL_TEST(QuerySharedDriverQueueTest, test_catch_up_rebases_before_scaled_time_overflows) {
    PipelineExecutorMetrics metrics;
    QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());
    // Set this queue's factors to ratio=2 without changing the config shared by parallel tests.
    for (int level = 0; level < QuerySharedDriverQueue::QUEUE_SIZE; ++level) {
        queue._queues[level].factor_for_normal = std::pow(2, 7 - level);
    }
    QueryContext query_context;
    auto new_driver = [&query_context](int level) {
        auto driver = std::make_shared<PipelineDriver>(_gen_operators(), _query_runtime_state(&query_context), nullptr,
                                                       nullptr, nullptr, nullptr, -1);
        _set_driver_level(driver.get(), level);
        return driver;
    };
    auto driver7 = new_driver(7);
    auto driver6 = new_driver(6);
    auto driver0 = new_driver(0);
    constexpr int64_t history = 80'000'000'000'000'000;
    constexpr int64_t lead = 1'000'000;
    queue._queues[7]._accu_consume_time = history;
    queue._queues[6]._accu_consume_time = (history + lead) * 2;
    queue.put_back(driver7.get());
    queue.put_back(driver6.get());
    ASSERT_EQ(driver7.get(), queue.take(true).value());

    // Catching L0 up to history * 128 would exceed INT64_MAX. Rebase the queued L6 too,
    // preserving its lead, and allow L7 to run again after L0 has consumed one slice.
    queue.put_back(driver0.get());
    auto stats = queue.level_stats();
    EXPECT_EQ(0, stats[0].accu_time_ns);
    EXPECT_EQ(lead, stats[6].accu_time_ns);
    EXPECT_EQ(0, stats[7].accu_time_ns);
    EXPECT_EQ(1, stats[7].schedule_count);
    ASSERT_EQ(driver0.get(), queue.take(true).value());
    driver0->driver_acct().update_last_time_spent(1'000'000);
    queue.update_statistics(driver0.get());
    queue.put_back(driver0.get());
    queue.put_back(driver7.get());
    ASSERT_EQ(driver7.get(), queue.take(true).value());
}

PARALLEL_TEST(QuerySharedDriverQueueTest, test_rebase_at_int64_limit) {
    PipelineExecutorMetrics metrics;
    QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());
    QueryContext query_context;
    auto driver = std::make_shared<PipelineDriver>(_gen_operators(), _query_runtime_state(&query_context), nullptr,
                                                   nullptr, nullptr, nullptr, -1);
    _set_driver_level(driver.get(), 7);
    queue._queues[7]._accu_consume_time = std::numeric_limits<int64_t>::max();
    // The metric scrape itself must not cast the rounded double value 2^63 to int64.
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), queue.level_stats()[7].accu_time_ns);
    queue.put_back(driver.get());
    ASSERT_EQ(driver.get(), queue.take(true).value());
    EXPECT_EQ(0, queue.level_stats()[7].accu_time_ns);
    driver->driver_acct().update_last_time_spent(1'000'000);
    queue.update_statistics(driver.get());
    EXPECT_EQ(1'000'000, queue.level_stats()[7].accu_time_ns);
}

PARALLEL_TEST(QuerySharedDriverQueueTest, test_rebase_keeps_concurrent_runtime) {
    SubQuerySharedDriverQueue queue;
    queue.factor_for_normal = 128;
    queue._accu_consume_time = 128'000 + 1234;
    QueryContext query_context;
    auto driver = std::make_shared<PipelineDriver>(_gen_operators(), _query_runtime_state(&query_context), nullptr,
                                                   nullptr, nullptr, nullptr, -1);
    driver->driver_acct().update_last_time_spent(1);
    std::thread updater([&] {
        for (int i = 0; i < 100'000; ++i) queue.update_accu_time(driver.get());
    });
    queue.rebase_accu_time(1000);
    updater.join();
    EXPECT_EQ(1234 + 100'000, queue._accu_consume_time.load());
}

PARALLEL_TEST(QuerySharedDriverQueueTest, test_level_stats) {
    PipelineExecutorMetrics metrics;
    QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());
    QueryContext query_context;
    auto driver = std::make_shared<PipelineDriver>(_gen_operators(), _query_runtime_state(&query_context), nullptr,
                                                   nullptr, nullptr, nullptr, -1);
    _set_driver_level(driver.get(), 2);
    driver->driver_acct().update_last_time_spent(12'000'000L);
    queue.update_statistics(driver.get());

    queue.put_back(driver.get());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_EQ(driver.get(), queue.take(true).value());

    const auto stats = queue.level_stats();
    ASSERT_EQ(QuerySharedDriverQueue::QUEUE_SIZE, stats.size());
    for (int level = 0; level < QuerySharedDriverQueue::QUEUE_SIZE; ++level) {
        if (level == 2) {
            const double factor = std::pow(QuerySharedDriverQueue::ratio_of_adjacent_queue(), 5);
            EXPECT_NEAR(12'000'000L / factor, stats[level].accu_time_ns, 1);
            EXPECT_GE(stats[level].wait_time_ns, 20'000'000L);
            EXPECT_EQ(1, stats[level].schedule_count);
        } else {
            EXPECT_EQ(0, stats[level].accu_time_ns);
            EXPECT_EQ(0, stats[level].wait_time_ns);
            EXPECT_EQ(0, stats[level].schedule_count);
        }
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
