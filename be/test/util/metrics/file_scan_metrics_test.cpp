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

#include "util/metrics/file_scan_metrics.h"

#include <gtest/gtest.h>

#include "exec/file_scanner.h"

namespace starrocks {

class FileScanMetricsTest : public testing::Test {
public:
    FileScanMetricsTest() = default;
    ~FileScanMetricsTest() override = default;
};

TEST_F(FileScanMetricsTest, TestUpdate) {
    MetricRegistry registry("test_registry");
    FileScanMetrics metrics(&registry);

    ScannerCounter counter;
    counter.filtered_rows_read = 50;
    counter.num_rows_filtered = 10;
    counter.num_rows_unselected = 20;
    counter.num_rows_read = 30;
    counter.num_bytes_read = 1000;
    counter.num_files_read = 2;

    metrics.update("parquet", "query", counter);

    // Verify metrics
    MetricLabels labels;
    labels.add("file_format", "parquet");
    labels.add("scan_type", "query");

    auto* file_read = registry.get_metric("files_scan_num_files_read", labels);
    ASSERT_NE(nullptr, file_read);
    ASSERT_EQ("2", file_read->to_string());

    auto* bytes_read = registry.get_metric("files_scan_num_bytes_read", labels);
    ASSERT_NE(nullptr, bytes_read);
    ASSERT_EQ("1000", bytes_read->to_string());

    // raw_rows = 10 + 50
    auto* raw_rows = registry.get_metric("files_scan_num_raw_rows_read", labels);
    ASSERT_NE(nullptr, raw_rows);
    ASSERT_EQ("60", raw_rows->to_string());

    // valid_rows = 20 + 30 = 50 (filtered_rows_read)
    auto* valid_rows = registry.get_metric("files_scan_num_valid_rows_read", labels);
    ASSERT_NE(nullptr, valid_rows);
    ASSERT_EQ("50", valid_rows->to_string());

    auto* rows_return = registry.get_metric("files_scan_num_rows_return", labels);
    ASSERT_NE(nullptr, rows_return);
    ASSERT_EQ("30", rows_return->to_string());
}

TEST_F(FileScanMetricsTest, TestMixedUpdate) {
    MetricRegistry registry("test_registry");
    FileScanMetrics metrics(&registry);

    ScannerCounter counter1;
    counter1.num_rows_read = 100;
    counter1.filtered_rows_read = 100;
    metrics.update("orc", "insert", counter1);

    ScannerCounter counter2;
    counter2.num_rows_read = 50;
    counter2.filtered_rows_read = 50;
    metrics.update("orc", "insert", counter2);

    MetricLabels labels;
    labels.add("file_format", "orc");
    labels.add("scan_type", "insert");

    auto* rows_return = registry.get_metric("files_scan_num_rows_return", labels);
    ASSERT_NE(nullptr, rows_return);
    ASSERT_EQ("150", rows_return->to_string());
}

TEST_F(FileScanMetricsTest, TestAvroStreamIgnored) {
    MetricRegistry registry("test_registry");
    FileScanMetrics metrics(&registry);

    ScannerCounter counter;
    counter.num_rows_read = 10;

    // update with avro_stream, should be ignored
    metrics.update("avro_stream", "query", counter);

    MetricLabels labels;
    labels.add("file_format", "avro_stream");
    labels.add("scan_type", "query");

    // Metric should not exist
    auto* rows_return = registry.get_metric("files_scan_num_rows_return", labels);
    ASSERT_EQ(nullptr, rows_return);
}

} // namespace starrocks
