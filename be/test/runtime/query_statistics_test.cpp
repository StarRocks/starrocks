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

#include "runtime/query_statistics.h"

#include <gtest/gtest.h>

#include <atomic>
#include <limits>
#include <thread>
#include <unordered_map>
#include <vector>

#include "gen_cpp/data.pb.h"

namespace starrocks {

class QueryStatisticsTest : public ::testing::Test {};

TEST_F(QueryStatisticsTest, pb) {
    QueryStatistics s;
    s.set_returned_rows(1);
    s.add_scan_stats(2, 3);
    s.add_cpu_costs(4);
    s.add_mem_costs(5);
    s.add_spill_bytes(6);
    s.add_read_stats(7, 8);
    s.add_transmitted_bytes(9);

    PQueryStatistics ps;
    s.to_pb(&ps);
    ASSERT_EQ(ps.returned_rows(), 1);
    ASSERT_EQ(ps.scan_rows(), 2);
    ASSERT_EQ(ps.scan_bytes(), 3);
    ASSERT_EQ(ps.cpu_cost_ns(), 4);
    ASSERT_EQ(ps.mem_cost_bytes(), 5);
    ASSERT_EQ(ps.spill_bytes(), 6);
    ASSERT_EQ(ps.read_local_cnt(), 7);
    ASSERT_EQ(ps.read_remote_cnt(), 8);
    ASSERT_EQ(ps.transmitted_bytes(), 9);

    s.merge_pb(ps);
    ASSERT_EQ(s.get_scan_rows(), 4);
    ASSERT_EQ(s.get_mem_bytes(), 5);
    ASSERT_EQ(s.get_transmitted_bytes(), 18);
    ASSERT_EQ(s.get_cpu_ns(), 8);
    ASSERT_EQ(s.get_read_local_cnt(), 14);
    ASSERT_EQ(s.get_read_remote_cnt(), 16);
}

TEST_F(QueryStatisticsTest, basic) {
    QueryStatistics s;
    s.set_returned_rows(1);
    s.add_scan_stats(2, 3);
    s.add_cpu_costs(4);
    s.add_mem_costs(5);
    s.add_spill_bytes(6);
    s.add_read_stats(7, 8);
    s.add_transmitted_bytes(9);

    QueryStatistics s2;
    s2.merge(0, s);
    ASSERT_EQ(s2.get_scan_rows(), 2);
    ASSERT_EQ(s2.get_mem_bytes(), 5);
    ASSERT_EQ(s2.get_transmitted_bytes(), 9);
    ASSERT_EQ(s2.get_cpu_ns(), 4);
    ASSERT_EQ(s2.get_read_local_cnt(), 7);
    ASSERT_EQ(s2.get_read_remote_cnt(), 8);

    s2.clear();
    ASSERT_EQ(s2.get_scan_rows(), 0);
    ASSERT_EQ(s2.get_mem_bytes(), 0);
    ASSERT_EQ(s2.get_transmitted_bytes(), 0);
    ASSERT_EQ(s2.get_cpu_ns(), 0);
    ASSERT_EQ(s2.get_read_local_cnt(), 0);
    ASSERT_EQ(s2.get_read_remote_cnt(), 0);
}

TEST_F(QueryStatisticsTest, AIStatisticsAbsentWithoutCompletedTasks) {
    QueryStatistics statistics;
    PQueryStatistics pb;
    TAuditStatistics thrift;
    statistics.to_pb(&pb);
    statistics.to_params(&thrift);
    EXPECT_FALSE(pb.has_ai_statistics());
    EXPECT_FALSE(thrift.__isset.ai_statistics);
}

TEST_F(QueryStatisticsTest, AIStatisticsRoundTripAndConsumingMerge) {
    AIExecutionStatistics task;
    task.task_count = 1;
    task.request_count = 2;
    task.retry_count = 3;
    task.timeout_count = 4;
    task.error_count = 5;
    task.http_time_ns = 6;
    task.prompt_tokens = 7;
    task.completion_tokens = 8;
    task.total_tokens = 9;
    task.prompt_usage_count = 10;
    task.completion_usage_count = 11;
    task.total_usage_count = 12;
    QueryStatistics source;
    source.add_ai_statistics(task);
    PQueryStatistics pb;
    source.to_pb(&pb);
    ASSERT_TRUE(pb.has_ai_statistics());
    EXPECT_EQ(1, pb.ai_statistics().task_count());
    EXPECT_EQ(2, pb.ai_statistics().request_count());
    EXPECT_EQ(3, pb.ai_statistics().retry_count());
    EXPECT_EQ(4, pb.ai_statistics().timeout_count());
    EXPECT_EQ(5, pb.ai_statistics().error_count());
    EXPECT_EQ(6, pb.ai_statistics().http_time_ns());
    EXPECT_EQ(7, pb.ai_statistics().prompt_tokens());
    EXPECT_EQ(8, pb.ai_statistics().completion_tokens());
    EXPECT_EQ(9, pb.ai_statistics().total_tokens());
    EXPECT_EQ(10, pb.ai_statistics().prompt_usage_count());
    EXPECT_EQ(11, pb.ai_statistics().completion_usage_count());
    EXPECT_EQ(12, pb.ai_statistics().total_usage_count());

    QueryStatistics destination;
    destination.merge_pb(pb);
    destination.merge(0, source);
    destination.merge(0, source); // The source has already been drained.
    TAuditStatistics thrift;
    destination.to_params(&thrift);
    ASSERT_TRUE(thrift.__isset.ai_statistics);
    EXPECT_EQ(2, thrift.ai_statistics.task_count);
    EXPECT_EQ(4, thrift.ai_statistics.request_count);
    EXPECT_EQ(6, thrift.ai_statistics.retry_count);
    EXPECT_EQ(8, thrift.ai_statistics.timeout_count);
    EXPECT_EQ(10, thrift.ai_statistics.error_count);
    EXPECT_EQ(12, thrift.ai_statistics.http_time_ns);
    EXPECT_EQ(14, thrift.ai_statistics.prompt_tokens);
    EXPECT_EQ(16, thrift.ai_statistics.completion_tokens);
    EXPECT_EQ(18, thrift.ai_statistics.total_tokens);
    EXPECT_EQ(20, thrift.ai_statistics.prompt_usage_count);
    EXPECT_EQ(22, thrift.ai_statistics.completion_usage_count);
    EXPECT_EQ(24, thrift.ai_statistics.total_usage_count);
    PQueryStatistics drained;
    source.to_pb(&drained);
    EXPECT_FALSE(drained.has_ai_statistics());

    destination.clear();
    // Reusing a destination must also remove the old optional payload.
    destination.to_pb(&pb);
    destination.to_params(&thrift);
    EXPECT_FALSE(pb.has_ai_statistics());
    EXPECT_FALSE(thrift.__isset.ai_statistics);
}

TEST_F(QueryStatisticsTest, AIStatisticsRejectNegativeAndSaturate) {
    QueryStatistics statistics;
    PQueryStatistics pb;
    auto* ai = pb.mutable_ai_statistics();
    ai->set_task_count(1);
    ai->set_request_count(-1);
    ai->set_total_tokens(std::numeric_limits<int64_t>::max());
    ai->set_total_usage_count(1);
    statistics.merge_pb(pb);
    statistics.merge_pb(pb);
    PQueryStatistics result;
    statistics.to_pb(&result);
    EXPECT_EQ(2, result.ai_statistics().task_count());
    EXPECT_EQ(0, result.ai_statistics().request_count());
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), result.ai_statistics().total_tokens());
    EXPECT_EQ(2, result.ai_statistics().total_usage_count());
}

TEST_F(QueryStatisticsTest, AIStatisticsConcurrentMergeDoesNotLoseTasks) {
    QueryStatistics source;
    QueryStatistics destination;
    constexpr int kTasks = 1000;
    std::thread producer([&] {
        AIExecutionStatistics task;
        task.task_count = 1;
        task.request_count = 2;
        for (int i = 0; i < kTasks; ++i) {
            source.add_ai_statistics(task);
        }
    });
    for (int i = 0; i < kTasks; ++i) {
        destination.merge(0, source);
    }
    producer.join();
    destination.merge(0, source);
    PQueryStatistics pb;
    destination.to_pb(&pb);
    EXPECT_EQ(kTasks, pb.ai_statistics().task_count());
    EXPECT_EQ(2 * kTasks, pb.ai_statistics().request_count());
}

TEST_F(QueryStatisticsTest, AIStatisticsUsageRequiresValidTokenAndCoveragePair) {
    QueryStatistics statistics;
    PQueryStatistics invalid;
    auto* ai = invalid.mutable_ai_statistics();
    ai->set_task_count(1);
    ai->set_prompt_tokens(-1);
    ai->set_prompt_usage_count(1);
    ai->set_completion_usage_count(1); // The token value is absent, not reported zero.
    ai->set_total_tokens(12);          // No usage count: not an observed token value.
    statistics.merge_pb(invalid);
    PQueryStatistics result;
    statistics.to_pb(&result);
    EXPECT_EQ(0, result.ai_statistics().prompt_usage_count());
    EXPECT_EQ(0, result.ai_statistics().completion_usage_count());
    EXPECT_EQ(0, result.ai_statistics().total_usage_count());
    EXPECT_EQ(0, result.ai_statistics().total_tokens());

    ai->set_prompt_tokens(0);
    ai->set_completion_tokens(4);
    ai->set_completion_usage_count(-1);
    ai->set_total_usage_count(0);
    statistics.merge_pb(invalid);
    statistics.to_pb(&result);
    EXPECT_EQ(1, result.ai_statistics().prompt_usage_count());
    EXPECT_EQ(0, result.ai_statistics().prompt_tokens());
    EXPECT_EQ(0, result.ai_statistics().completion_usage_count());
    EXPECT_EQ(0, result.ai_statistics().total_usage_count());
    EXPECT_EQ(0, result.ai_statistics().completion_tokens());
    EXPECT_EQ(0, result.ai_statistics().total_tokens());
}

TEST_F(QueryStatisticsTest, ConcurrentMergeConservesAllScalarAndMapCounters) {
    QueryStatistics source;
    constexpr int kWriters = 8;
    constexpr int kAggregators = 2;
    constexpr int kIterationsPerWriter = 4000;
    constexpr int64_t kMaxInjectedMem = 987654321LL;

    std::atomic<bool> start{false};
    std::atomic<int> active_writers{kWriters};

    std::vector<QueryStatistics> agg_outputs(kAggregators);
    std::vector<std::thread> threads;
    threads.reserve(kWriters + kAggregators);

    for (int w = 0; w < kWriters; ++w) {
        threads.emplace_back([&, w]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int i = 0; i < kIterationsPerWriter; ++i) {
                PQueryStatistics pb;
                pb.set_scan_rows(1);
                pb.set_scan_bytes(10);
                pb.set_cpu_cost_ns(100);
                pb.set_spill_bytes(2);
                pb.set_read_local_cnt(3);
                pb.set_read_remote_cnt(4);
                pb.set_transmitted_bytes(20);
                int64_t mem = (w == kWriters - 1 && i == kIterationsPerWriter / 2)
                                      ? kMaxInjectedMem
                                      : static_cast<int64_t>((w + 1) * 1000 + (i % 500));
                pb.set_mem_cost_bytes(mem);

                auto* item = pb.add_stats_items();
                item->set_table_id(100 + (i % 4));
                item->set_scan_rows(1);
                item->set_scan_bytes(10);

                auto* exec_item = pb.add_node_exec_stats_items();
                exec_item->set_node_id(static_cast<uint32_t>(i % 8));
                exec_item->set_push_rows(5);
                exec_item->set_pull_rows(4);
                exec_item->set_pred_filter_rows(1);
                exec_item->set_index_filter_rows(2);
                exec_item->set_rf_filter_rows(3);

                source.merge_pb(pb);
                source.add_exec_stats_item(static_cast<uint32_t>(i % 8), 1, 1, 0, 0, 0);
            }
            active_writers.fetch_sub(1, std::memory_order_release);
        });
    }

    for (int a = 0; a < kAggregators; ++a) {
        threads.emplace_back([&, a]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            while (active_writers.load(std::memory_order_acquire) > 0) {
                PQueryStatistics snap;
                source.to_pb(&snap);
                agg_outputs[a].merge(0, source);
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& t : threads) {
        t.join();
    }

    QueryStatistics final_total;
    for (int a = 0; a < kAggregators; ++a) {
        final_total.merge(0, agg_outputs[a]);
    }
    final_total.merge(0, source);

    const int64_t total_ops = static_cast<int64_t>(kWriters) * kIterationsPerWriter;
    PQueryStatistics total_pb;
    final_total.to_pb(&total_pb);

    EXPECT_EQ(total_ops * 1, total_pb.scan_rows());
    EXPECT_EQ(total_ops * 10, total_pb.scan_bytes());
    EXPECT_EQ(total_ops * 100, total_pb.cpu_cost_ns());
    EXPECT_EQ(total_ops * 2, total_pb.spill_bytes());
    EXPECT_EQ(total_ops * 3, total_pb.read_local_cnt());
    EXPECT_EQ(total_ops * 4, total_pb.read_remote_cnt());
    EXPECT_EQ(total_ops * 20, total_pb.transmitted_bytes());
    EXPECT_EQ(kMaxInjectedMem, total_pb.mem_cost_bytes());

    ASSERT_EQ(4, total_pb.stats_items_size());
    int64_t table_rows_sum = 0;
    int64_t table_bytes_sum = 0;
    for (int i = 0; i < total_pb.stats_items_size(); ++i) {
        table_rows_sum += total_pb.stats_items(i).scan_rows();
        table_bytes_sum += total_pb.stats_items(i).scan_bytes();
    }
    EXPECT_EQ(total_ops * 1, table_rows_sum);
    EXPECT_EQ(total_ops * 10, table_bytes_sum);

    ASSERT_EQ(8, total_pb.node_exec_stats_items_size());
    int64_t push_sum = 0;
    int64_t pull_sum = 0;
    int64_t pred_sum = 0;
    int64_t index_sum = 0;
    int64_t rf_sum = 0;
    for (int i = 0; i < total_pb.node_exec_stats_items_size(); ++i) {
        push_sum += total_pb.node_exec_stats_items(i).push_rows();
        pull_sum += total_pb.node_exec_stats_items(i).pull_rows();
        pred_sum += total_pb.node_exec_stats_items(i).pred_filter_rows();
        index_sum += total_pb.node_exec_stats_items(i).index_filter_rows();
        rf_sum += total_pb.node_exec_stats_items(i).rf_filter_rows();
    }
    EXPECT_EQ(total_ops * 6, push_sum);
    EXPECT_EQ(total_ops * 5, pull_sum);
    EXPECT_EQ(total_ops * 1, pred_sum);
    EXPECT_EQ(total_ops * 2, index_sum);
    EXPECT_EQ(total_ops * 3, rf_sum);

    // After draining source, serializing source must not emit zero-valued map entries.
    PQueryStatistics drained_pb;
    source.to_pb(&drained_pb);
    EXPECT_EQ(0, drained_pb.stats_items_size());
    EXPECT_EQ(0, drained_pb.node_exec_stats_items_size());

    TAuditStatistics drained_thrift;
    source.to_params(&drained_thrift);
    EXPECT_TRUE(drained_thrift.stats_items.empty());
}

TEST_F(QueryStatisticsTest, RecvrConcurrent90SendersAndEosBurstConservation) {
    static_assert(QueryStatisticsRecvr::kNumStripes >= 8);
    QueryStatisticsRecvr recvr;
    constexpr int kWriters = 16;
    constexpr int kAggregators = 4;
    constexpr int kIterationsPerWriter = 2000;
    constexpr int kNumSenders = 90;
    constexpr int64_t kGlobalPeakMem = 1234567890LL;

    std::atomic<bool> start{false};
    std::atomic<int> active_writers{kWriters};

    std::vector<QueryStatistics> agg_totals(kAggregators);
    std::vector<std::thread> threads;
    threads.reserve(kWriters + kAggregators);

    for (int w = 0; w < kWriters; ++w) {
        threads.emplace_back([&, w]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int i = 0; i < kIterationsPerWriter; ++i) {
                PQueryStatistics pb;
                pb.set_scan_rows(2);
                pb.set_scan_bytes(20);
                pb.set_cpu_cost_ns(50);
                pb.set_spill_bytes(1);
                pb.set_read_local_cnt(2);
                pb.set_read_remote_cnt(3);
                pb.set_transmitted_bytes(30);
                int64_t mem = (w == 0 && i == kIterationsPerWriter - 1) ? kGlobalPeakMem : (w + 1) * 1024;
                pb.set_mem_cost_bytes(mem);

                auto* item = pb.add_stats_items();
                item->set_table_id(200 + (i % 4));
                item->set_scan_rows(2);
                item->set_scan_bytes(20);

                auto* exec_item = pb.add_node_exec_stats_items();
                exec_item->set_node_id(static_cast<uint32_t>(i % 16));
                exec_item->set_push_rows(10);
                exec_item->set_pull_rows(8);
                exec_item->set_pred_filter_rows(2);
                exec_item->set_index_filter_rows(1);
                exec_item->set_rf_filter_rows(1);

                // Exercise 0..89 sender IDs as well as negative (-1) and large non-contiguous sender IDs.
                int sender_id = (i % kNumSenders);
                if (i == 0) {
                    sender_id = -1;
                } else if (i == 1) {
                    sender_id = 100000;
                }
                recvr.insert(pb, sender_id);
            }
            active_writers.fetch_sub(1, std::memory_order_release);
        });
    }

    for (int a = 0; a < kAggregators; ++a) {
        threads.emplace_back([&, a]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            while (active_writers.load(std::memory_order_acquire) > 0) {
                QueryStatistics batch;
                recvr.aggregate(&batch);
                agg_totals[a].merge(0, batch);
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& t : threads) {
        t.join();
    }

    QueryStatistics grand_total;
    for (int a = 0; a < kAggregators; ++a) {
        grand_total.merge(0, agg_totals[a]);
    }
    // Simulate a 90-destination EOS aggregate burst after writers finish.
    for (int eos = 0; eos < kNumSenders; ++eos) {
        QueryStatistics eos_batch;
        recvr.aggregate(&eos_batch);
        EXPECT_EQ(kGlobalPeakMem, eos_batch.get_mem_bytes());
        grand_total.merge(0, eos_batch);
    }

    const int64_t total_ops = static_cast<int64_t>(kWriters) * kIterationsPerWriter;
    PQueryStatistics total_pb;
    grand_total.to_pb(&total_pb);

    EXPECT_EQ(total_ops * 2, total_pb.scan_rows());
    EXPECT_EQ(total_ops * 20, total_pb.scan_bytes());
    EXPECT_EQ(total_ops * 50, total_pb.cpu_cost_ns());
    EXPECT_EQ(total_ops * 1, total_pb.spill_bytes());
    EXPECT_EQ(total_ops * 2, total_pb.read_local_cnt());
    EXPECT_EQ(total_ops * 3, total_pb.read_remote_cnt());
    EXPECT_EQ(total_ops * 30, total_pb.transmitted_bytes());
    EXPECT_EQ(kGlobalPeakMem, total_pb.mem_cost_bytes());

    ASSERT_EQ(4, total_pb.stats_items_size());
    int64_t table_rows_sum = 0;
    for (int i = 0; i < total_pb.stats_items_size(); ++i) {
        table_rows_sum += total_pb.stats_items(i).scan_rows();
    }
    EXPECT_EQ(total_ops * 2, table_rows_sum);

    ASSERT_EQ(16, total_pb.node_exec_stats_items_size());
    int64_t push_sum = 0;
    for (int i = 0; i < total_pb.node_exec_stats_items_size(); ++i) {
        push_sum += total_pb.node_exec_stats_items(i).push_rows();
    }
    EXPECT_EQ(total_ops * 10, push_sum);
}

TEST_F(QueryStatisticsTest, PreservesExplicitZeroNodeExecStatsAndOmitsDrainedEntries) {
    QueryStatistics source;
    source.add_exec_stats_item(7, 0, 0, 0, 0, 0);

    PQueryStatistics before_merge_pb;
    source.to_pb(&before_merge_pb);
    ASSERT_EQ(1, before_merge_pb.node_exec_stats_items_size());
    EXPECT_EQ(7U, before_merge_pb.node_exec_stats_items(0).node_id());
    EXPECT_EQ(0, before_merge_pb.node_exec_stats_items(0).push_rows());
    EXPECT_EQ(0, before_merge_pb.node_exec_stats_items(0).pull_rows());

    QueryStatistics dest;
    dest.merge(0, source);

    PQueryStatistics dest_pb;
    dest.to_pb(&dest_pb);
    ASSERT_EQ(1, dest_pb.node_exec_stats_items_size());
    EXPECT_EQ(7U, dest_pb.node_exec_stats_items(0).node_id());
    EXPECT_EQ(0, dest_pb.node_exec_stats_items(0).push_rows());

    // After draining source into dest, source must omit the drained entry.
    PQueryStatistics drained_source_pb;
    source.to_pb(&drained_source_pb);
    EXPECT_EQ(0, drained_source_pb.node_exec_stats_items_size());
}

} // namespace starrocks
