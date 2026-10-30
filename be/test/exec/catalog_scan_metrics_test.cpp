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

#include "exec/catalog_scan_metrics.h"

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "base/metrics.h"

namespace starrocks {

class CatalogScanMetricsTest : public ::testing::Test {
protected:
    void SetUp() override {
        _registry = std::make_unique<MetricRegistry>("test");
        _metrics = std::make_unique<CatalogScanMetrics>(_registry.get());
    }

    std::unique_ptr<MetricRegistry> _registry;
    std::unique_ptr<CatalogScanMetrics> _metrics;
};

TEST_F(CatalogScanMetricsTest, UpdateScanBytes) {
    _metrics->update_scan_bytes("hive", 1024);
    _metrics->update_scan_bytes("hive", 2048);
    _metrics->update_scan_bytes("iceberg", 512);

    MetricLabels hive_labels;
    hive_labels.add("catalog_type", "hive");
    auto* hive_bytes = _registry->get_metric("catalog_query_scan_bytes", hive_labels);
    ASSERT_NE(nullptr, hive_bytes);
    ASSERT_EQ("3072", hive_bytes->to_string());

    MetricLabels iceberg_labels;
    iceberg_labels.add("catalog_type", "iceberg");
    auto* iceberg_bytes = _registry->get_metric("catalog_query_scan_bytes", iceberg_labels);
    ASSERT_NE(nullptr, iceberg_bytes);
    ASSERT_EQ("512", iceberg_bytes->to_string());
}

TEST_F(CatalogScanMetricsTest, UpdateScanRows) {
    _metrics->update_scan_rows("default", 100);
    _metrics->update_scan_rows("hive", 200);

    MetricLabels default_labels;
    default_labels.add("catalog_type", "default");
    auto* default_rows = _registry->get_metric("catalog_query_scan_rows", default_labels);
    ASSERT_NE(nullptr, default_rows);
    ASSERT_EQ("100", default_rows->to_string());

    MetricLabels hive_labels;
    hive_labels.add("catalog_type", "hive");
    auto* hive_rows = _registry->get_metric("catalog_query_scan_rows", hive_labels);
    ASSERT_NE(nullptr, hive_rows);
    ASSERT_EQ("200", hive_rows->to_string());
}

TEST_F(CatalogScanMetricsTest, UpdateFilesScanMetrics) {
    _metrics->update_files_scan_bytes_read("deltalake", 4096);
    _metrics->update_files_scan_rows_return("deltalake", 50);

    MetricLabels labels;
    labels.add("catalog_type", "deltalake");

    auto* bytes_read = _registry->get_metric("catalog_files_scan_num_bytes_read", labels);
    ASSERT_NE(nullptr, bytes_read);
    ASSERT_EQ("4096", bytes_read->to_string());

    auto* rows_return = _registry->get_metric("catalog_files_scan_num_rows_return", labels);
    ASSERT_NE(nullptr, rows_return);
    ASSERT_EQ("50", rows_return->to_string());
}

TEST_F(CatalogScanMetricsTest, MultipleCatalogTypes) {
    _metrics->update_scan_bytes("hive", 100);
    _metrics->update_scan_bytes("iceberg", 200);
    _metrics->update_scan_bytes("jdbc", 300);

    MetricLabels hive_labels;
    hive_labels.add("catalog_type", "hive");
    auto* hive_bytes = _registry->get_metric("catalog_query_scan_bytes", hive_labels);
    ASSERT_NE(nullptr, hive_bytes);
    ASSERT_EQ("100", hive_bytes->to_string());

    MetricLabels iceberg_labels;
    iceberg_labels.add("catalog_type", "iceberg");
    auto* iceberg_bytes = _registry->get_metric("catalog_query_scan_bytes", iceberg_labels);
    ASSERT_NE(nullptr, iceberg_bytes);
    ASSERT_EQ("200", iceberg_bytes->to_string());

    MetricLabels jdbc_labels;
    jdbc_labels.add("catalog_type", "jdbc");
    auto* jdbc_bytes = _registry->get_metric("catalog_query_scan_bytes", jdbc_labels);
    ASSERT_NE(nullptr, jdbc_bytes);
    ASSERT_EQ("300", jdbc_bytes->to_string());
}

// Verify that concurrent updates from multiple threads produce the correct total,
// and that _get_or_create_metrics handles concurrent first-creation safely (shared_mutex
// double-check pattern: one thread wins the unique_lock and creates the entry, the other
// finds it on the double-check and returns without recreating it).
TEST_F(CatalogScanMetricsTest, ConcurrentUpdatesAreThreadSafe) {
    constexpr int num_threads = 8;
    constexpr int increments_per_thread = 100;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this]() {
            for (int j = 0; j < increments_per_thread; ++j) {
                // All threads race to create "concurrent_type" on the first call,
                // exercising the double-check path in _get_or_create_metrics.
                _metrics->update_scan_bytes("concurrent_type", 1);
                _metrics->update_scan_rows("concurrent_type", 1);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }

    const int64_t expected = num_threads * increments_per_thread;
    MetricLabels labels;
    labels.add("catalog_type", "concurrent_type");

    auto* bytes = _registry->get_metric("catalog_query_scan_bytes", labels);
    ASSERT_NE(nullptr, bytes);
    ASSERT_EQ(std::to_string(expected), bytes->to_string());

    auto* rows = _registry->get_metric("catalog_query_scan_rows", labels);
    ASSERT_NE(nullptr, rows);
    ASSERT_EQ(std::to_string(expected), rows->to_string());
}

// Verify that the fast read-path (shared_lock hit) works correctly after the metric
// is already created: subsequent calls should find the entry without acquiring unique_lock.
TEST_F(CatalogScanMetricsTest, SubsequentCallsUseSharedLockFastPath) {
    // First call: creates the entry (unique_lock path).
    _metrics->update_scan_bytes("fastpath_type", 10);
    // Remaining calls: all hit the shared_lock fast path.
    for (int i = 0; i < 50; ++i) {
        _metrics->update_scan_bytes("fastpath_type", 1);
    }

    MetricLabels labels;
    labels.add("catalog_type", "fastpath_type");
    auto* bytes = _registry->get_metric("catalog_query_scan_bytes", labels);
    ASSERT_NE(nullptr, bytes);
    ASSERT_EQ("60", bytes->to_string());
}

} // namespace starrocks
