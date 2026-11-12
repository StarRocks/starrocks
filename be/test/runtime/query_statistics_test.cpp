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

} // namespace starrocks
