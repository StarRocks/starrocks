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

#include <limits>
#include <thread>

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

namespace {

const PerScanStatsItemPB* find_per_scan_item(const PQueryStatistics& ps, int32_t plan_node_id) {
    for (const auto& item : ps.per_scan_stats_items()) {
        if (item.plan_node_id() == plan_node_id) {
            return &item;
        }
    }
    return nullptr;
}

ScanAuditCounters make_counters(uint32_t mask, int64_t physical, int64_t logical, int64_t output, int64_t remote,
                                int64_t cache) {
    ScanAuditCounters c;
    c.mask = mask;
    c.physical_rows = physical;
    c.logical_rows = logical;
    c.output_rows = output;
    c.remote_bytes = remote;
    c.cache_bytes = cache;
    return c;
}

} // namespace

TEST_F(QueryStatisticsTest, PerScanStatsKeepScansApartAndSetOnlyTrackedCounters) {
    QueryStatistics s;
    constexpr uint32_t all = ScanAuditCounters::ROWS_AND_BYTES | ScanAuditCounters::OUTPUT_ROWS;
    // Two scans of the same table stay apart: they are keyed by plan node id.
    s.add_per_scan_stats(3, make_counters(all, 10, 8, 5, 100, 20));
    s.add_per_scan_stats(3, make_counters(all, 1, 1, 1, 10, 2));
    s.add_per_scan_stats(7, make_counters(ScanAuditCounters::OUTPUT_ROWS, 0, 0, 4, 0, 0));
    // An empty delta adds no item.
    s.add_per_scan_stats(9, ScanAuditCounters{});

    PQueryStatistics ps;
    s.to_pb(&ps);
    ASSERT_EQ(2, ps.per_scan_stats_items_size());
    // Per scan counters leave the query level scan counters alone.
    EXPECT_EQ(0, ps.scan_rows());
    const auto* scan3 = find_per_scan_item(ps, 3);
    ASSERT_NE(nullptr, scan3);
    EXPECT_EQ(11, scan3->physical_rows_read());
    EXPECT_EQ(9, scan3->logical_rows_read());
    EXPECT_EQ(6, scan3->output_rows());
    EXPECT_EQ(110, scan3->remote_bytes());
    EXPECT_EQ(22, scan3->cache_bytes());
    const auto* scan7 = find_per_scan_item(ps, 7);
    ASSERT_NE(nullptr, scan7);
    EXPECT_TRUE(scan7->has_output_rows());
    EXPECT_EQ(4, scan7->output_rows());
    EXPECT_FALSE(scan7->has_physical_rows_read());
    EXPECT_FALSE(scan7->has_logical_rows_read());
    EXPECT_FALSE(scan7->has_remote_bytes());
    EXPECT_FALSE(scan7->has_cache_bytes());

    TAuditStatistics params;
    s.to_params(&params);
    ASSERT_TRUE(params.__isset.per_scan_stats_items);
    ASSERT_EQ(2, params.per_scan_stats_items.size());
    for (const auto& item : params.per_scan_stats_items) {
        if (item.plan_node_id == 3) {
            EXPECT_EQ(11, item.physical_rows_read);
            EXPECT_EQ(22, item.cache_bytes);
        } else {
            EXPECT_EQ(7, item.plan_node_id);
            EXPECT_TRUE(item.__isset.output_rows);
            EXPECT_FALSE(item.__isset.physical_rows_read);
        }
    }

    // merge_pb sums the same plan node and keeps which counters are set.
    QueryStatistics receiver;
    receiver.merge_pb(ps);
    receiver.merge_pb(ps);
    PQueryStatistics merged;
    receiver.to_pb(&merged);
    ASSERT_EQ(2, merged.per_scan_stats_items_size());
    EXPECT_EQ(22, find_per_scan_item(merged, 3)->physical_rows_read());
    EXPECT_EQ(8, find_per_scan_item(merged, 7)->output_rows());
    EXPECT_FALSE(find_per_scan_item(merged, 7)->has_physical_rows_read());
}

TEST_F(QueryStatisticsTest, PerScanStatsMergeConsumesTheSender) {
    QueryStatistics sender;
    sender.add_per_scan_stats(1, make_counters(ScanAuditCounters::PHYSICAL_ROWS, 5, 0, 0, 0, 0));
    QueryStatistics receiver;
    receiver.add_per_scan_stats(1, make_counters(ScanAuditCounters::LOGICAL_ROWS, 0, 4, 0, 0, 0));
    receiver.merge(0, sender);

    PQueryStatistics ps;
    receiver.to_pb(&ps);
    ASSERT_EQ(1, ps.per_scan_stats_items_size());
    EXPECT_EQ(5, ps.per_scan_stats_items(0).physical_rows_read());
    EXPECT_EQ(4, ps.per_scan_stats_items(0).logical_rows_read());

    PQueryStatistics drained;
    sender.to_pb(&drained);
    EXPECT_EQ(0, drained.per_scan_stats_items_size());

    receiver.clear();
    PQueryStatistics cleared;
    receiver.to_pb(&cleared);
    EXPECT_EQ(0, cleared.per_scan_stats_items_size());
}

TEST_F(QueryStatisticsTest, ScanAuditCountersDeltaNeverGoesNegative) {
    ScanAuditCounters prev = make_counters(ScanAuditCounters::ROWS_AND_BYTES, 10, 10, 0, 100, 50);
    ScanAuditCounters now = make_counters(ScanAuditCounters::ROWS_AND_BYTES, 4, 12, 0, 120, 50);
    ScanAuditCounters d = ScanAuditCounters::delta(now, prev);
    EXPECT_EQ(ScanAuditCounters::ROWS_AND_BYTES, d.mask);
    EXPECT_EQ(0, d.physical_rows);
    EXPECT_EQ(2, d.logical_rows);
    EXPECT_EQ(20, d.remote_bytes);
    EXPECT_EQ(0, d.cache_bytes);
}

} // namespace starrocks
