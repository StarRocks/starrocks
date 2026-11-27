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

#include "runtime/kafka_scan_range_report.h"

#include <gtest/gtest.h>

#include <map>
#include <thread>
#include <vector>

#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

// An instance whose scan ranges have not reported yet still sets every field, so the FE can tell an empty
// consumption from a report without this struct.
TEST(KafkaScanRangeReportTest, empty_report_sets_every_field) {
    KafkaScanRangeReport report;
    TKafkaScanRangeReport t_report;
    report.to_thrift(&t_report);
    EXPECT_TRUE(t_report.__isset.partition_cmt_offset);
    EXPECT_TRUE(t_report.partition_cmt_offset.empty());
    EXPECT_TRUE(t_report.__isset.partition_cmt_offset_timestamp);
    EXPECT_TRUE(t_report.partition_cmt_offset_timestamp.empty());
    EXPECT_TRUE(t_report.__isset.received_bytes);
    EXPECT_EQ(0, t_report.received_bytes);
    EXPECT_TRUE(t_report.__isset.received_rows);
    EXPECT_EQ(0, t_report.received_rows);
    EXPECT_TRUE(t_report.__isset.reached_end);
    EXPECT_TRUE(t_report.reached_end);
}

// The scan ranges of one instance consume disjoint partitions: the offsets are united and the counters summed. A
// partition with nothing consumed keeps its begin offset - 1 and has no timestamp.
TEST(KafkaScanRangeReportTest, merge_unites_partitions_and_sums_counters) {
    KafkaScanRangeReport report;
    report.merge({{0, 99}, {1, -1}}, {{0, 1700000000000L}}, 1024, 100, true);
    report.merge({{2, 9}}, {{2, 1700000000001L}}, 512, 10, true);

    TKafkaScanRangeReport t_report;
    report.to_thrift(&t_report);
    std::map<int32_t, int64_t> expected_offsets{{0, 99}, {1, -1}, {2, 9}};
    EXPECT_EQ(expected_offsets, t_report.partition_cmt_offset);
    std::map<int32_t, int64_t> expected_timestamps{{0, 1700000000000L}, {2, 1700000000001L}};
    EXPECT_EQ(expected_timestamps, t_report.partition_cmt_offset_timestamp);
    EXPECT_EQ(1536, t_report.received_bytes);
    EXPECT_EQ(110, t_report.received_rows);
    EXPECT_TRUE(t_report.reached_end);
}

// One scan range that stopped at its limit with data left makes the instance report that it did not reach the end.
TEST(KafkaScanRangeReportTest, reached_end_needs_every_scan_range_at_its_end) {
    KafkaScanRangeReport report;
    report.merge({{0, 99}}, {}, 1024, 100, true);
    report.merge({{1, 199}}, {}, 2048, 200, false);
    report.merge({{2, 9}}, {}, 512, 10, true);

    TKafkaScanRangeReport t_report;
    report.to_thrift(&t_report);
    EXPECT_FALSE(t_report.reached_end);
}

// The consumptions of the scan ranges end on their own threads.
TEST(KafkaScanRangeReportTest, concurrent_merges) {
    constexpr int kNumScanRanges = 16;
    KafkaScanRangeReport report;
    std::vector<std::thread> threads;
    threads.reserve(kNumScanRanges);
    for (int i = 0; i < kNumScanRanges; ++i) {
        threads.emplace_back([&report, i] { report.merge({{i, i * 10L}}, {}, i, 1, true); });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    TKafkaScanRangeReport t_report;
    report.to_thrift(&t_report);
    ASSERT_EQ(kNumScanRanges, t_report.partition_cmt_offset.size());
    int64_t expected_bytes = 0;
    for (int i = 0; i < kNumScanRanges; ++i) {
        EXPECT_EQ(i * 10L, t_report.partition_cmt_offset[i]);
        expected_bytes += i;
    }
    EXPECT_EQ(expected_bytes, t_report.received_bytes);
    EXPECT_EQ(kNumScanRanges, t_report.received_rows);
}

} // namespace starrocks
