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

#include "cache/datacache_metrics.h"

#include <gtest/gtest.h>

#include "cache/datacache.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "fs/fs_util.h"
#include "util/starrocks_metrics.h"

#ifdef WITH_STARCACHE
#include "base/testutil/assert.h"
#include "cache/disk_cache/starcache_engine.h"
#endif

namespace starrocks {

class DataCacheMetricsTest : public ::testing::Test {
public:
    DataCacheMetricsTest() = default;
    ~DataCacheMetricsTest() override = default;

protected:
    void SetUp() override {}

    void TearDown() override {}
};

// Test that register_datacache_metrics can be called without crashing
TEST_F(DataCacheMetricsTest, test_register_datacache_metrics_basic) {
    // This should not crash regardless of WITH_STARCACHE
    ASSERT_NO_THROW(register_datacache_metrics(false));
    ASSERT_NO_THROW(register_datacache_metrics(true));
}

#ifdef WITH_STARCACHE
// Test metrics registration with StarCache enabled
TEST_F(DataCacheMetricsTest, test_datacache_metrics_registration) {
    auto instance = StarRocksMetrics::instance();
    auto metrics = instance->metrics();

    // Verify that datacache metrics are registered
    ASSERT_NE(nullptr, metrics->get_metric("datacache_mem_quota_bytes"));
    ASSERT_NE(nullptr, metrics->get_metric("datacache_mem_used_bytes"));
    ASSERT_NE(nullptr, metrics->get_metric("datacache_disk_quota_bytes"));
    ASSERT_NE(nullptr, metrics->get_metric("datacache_disk_used_bytes"));
}

// Test that metrics have correct initial values
TEST_F(DataCacheMetricsTest, test_datacache_metrics_initial_values) {
    auto instance = StarRocksMetrics::instance();

    // Initial values should be 0 or positive integers
    ASSERT_GE(instance->datacache_mem_quota_bytes.value(), 0);
    ASSERT_GE(instance->datacache_mem_used_bytes.value(), 0);
    ASSERT_GE(instance->datacache_disk_quota_bytes.value(), 0);
    ASSERT_GE(instance->datacache_disk_used_bytes.value(), 0);
}

// Test metrics update through hook mechanism
TEST_F(DataCacheMetricsTest, test_metrics_update_hook) {
    auto instance = StarRocksMetrics::instance();
    auto metrics = instance->metrics();

    // Register the metrics hook
    register_datacache_metrics(false);

    // Trigger the hook manually
    metrics->trigger_hook();

    // After hook execution, values should still be non-negative
    ASSERT_GE(instance->datacache_mem_quota_bytes.value(), 0);
    ASSERT_GE(instance->datacache_mem_used_bytes.value(), 0);
    ASSERT_GE(instance->datacache_disk_quota_bytes.value(), 0);
    ASSERT_GE(instance->datacache_disk_used_bytes.value(), 0);

    // Used bytes should not exceed quota bytes
    ASSERT_LE(instance->datacache_mem_used_bytes.value(), instance->datacache_mem_quota_bytes.value());
    ASSERT_LE(instance->datacache_disk_used_bytes.value(), instance->datacache_disk_quota_bytes.value());
}

// Test metrics types are correct (should be INT_GAUGE)
TEST_F(DataCacheMetricsTest, test_metrics_types) {
    auto instance = StarRocksMetrics::instance();
    auto metrics = instance->metrics();

    auto mem_quota_metric = metrics->get_metric("datacache_mem_quota_bytes");
    auto mem_used_metric = metrics->get_metric("datacache_mem_used_bytes");
    auto disk_quota_metric = metrics->get_metric("datacache_disk_quota_bytes");
    auto disk_used_metric = metrics->get_metric("datacache_disk_used_bytes");

    ASSERT_NE(nullptr, mem_quota_metric);
    ASSERT_NE(nullptr, mem_used_metric);
    ASSERT_NE(nullptr, disk_quota_metric);
    ASSERT_NE(nullptr, disk_used_metric);

    // All should be gauge type metrics (not counters)
    ASSERT_EQ(MetricType::GAUGE, mem_quota_metric->type());
    ASSERT_EQ(MetricType::GAUGE, mem_used_metric->type());
    ASSERT_EQ(MetricType::GAUGE, disk_quota_metric->type());
    ASSERT_EQ(MetricType::GAUGE, disk_used_metric->type());

    // All should have BYTES unit
    ASSERT_EQ(MetricUnit::BYTES, mem_quota_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, mem_used_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, disk_quota_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, disk_used_metric->unit());
}

#else // !WITH_STARCACHE

// Test that without StarCache, registration is a no-op
TEST_F(DataCacheMetricsTest, test_without_starcache) {
    // When WITH_STARCACHE is not defined, these should be no-ops
    ASSERT_NO_THROW(register_datacache_metrics(false));
    ASSERT_NO_THROW(register_datacache_metrics(true));

    // The function should compile and execute successfully even without StarCache
}

#endif // WITH_STARCACHE

} // namespace starrocks
