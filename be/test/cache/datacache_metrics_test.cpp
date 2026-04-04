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

#include <mutex>

#include "common/metrics/process_metrics_registry.h"

#ifdef WITH_STARCACHE
#include "cache/datacache.h"
#endif

namespace starrocks {

namespace {

ProcessMetricsRegistry* backend_process_metrics_registry_for_test() {
    static auto* registry = new ProcessMetricsRegistry("starrocks_be");
    return registry;
}

MetricRegistry* backend_metrics_registry_for_test() {
    return backend_process_metrics_registry_for_test()->root_registry();
}

void init_backend_metrics_for_test() {
    static std::once_flag once;
    std::call_once(once, [] { DataCacheMetrics::instance()->install(backend_metrics_registry_for_test()); });
}

} // namespace

class DataCacheMetricsTest : public ::testing::Test {
public:
    DataCacheMetricsTest() = default;
    ~DataCacheMetricsTest() override = default;

protected:
    void SetUp() override { init_backend_metrics_for_test(); }

    void TearDown() override {}
};

// Test that DataCacheMetrics update hook registration can be called without crashing.
TEST_F(DataCacheMetricsTest, test_enable_update_hook_basic) {
    // This should not crash regardless of WITH_STARCACHE
    ASSERT_NO_THROW(DataCacheMetrics::instance()->enable_update_hook(false));
    ASSERT_NO_THROW(DataCacheMetrics::instance()->enable_update_hook(true));
}

#ifdef WITH_STARCACHE
// Test metrics registration with StarCache enabled
TEST_F(DataCacheMetricsTest, test_datacache_metrics_registration) {
    auto metrics = backend_metrics_registry_for_test();

    // Verify that datacache metrics are registered
    ASSERT_NE(nullptr, metrics->get_metric("datacache_mem_quota_bytes"));
    ASSERT_NE(nullptr, metrics->get_metric("datacache_mem_used_bytes"));
    ASSERT_NE(nullptr, metrics->get_metric("datacache_disk_quota_bytes"));
    ASSERT_NE(nullptr, metrics->get_metric("datacache_disk_used_bytes"));
    ASSERT_NE(nullptr, metrics->get_metric("datacache_meta_used_bytes"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_hit_bytes"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_miss_bytes"));

    // Block cache detailed metrics
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_hit_count"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_miss_count"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_hit_count_last_minute"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_miss_count_last_minute"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_hit_bytes_last_minute"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_miss_bytes_last_minute"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_read_mem_bytes"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_read_disk_bytes"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_write_bytes"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_write_success_count"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_write_fail_count"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_remove_bytes"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_remove_success_count"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_remove_fail_count"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_current_reading_count"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_current_writing_count"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_current_removing_count"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_buffer_item_count"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_buffer_item_bytes"));

    // Page cache metrics
    ASSERT_NE(nullptr, metrics->get_metric("datacache_page_hit_count"));
    ASSERT_NE(nullptr, metrics->get_metric("datacache_page_miss_count"));
    ASSERT_NE(nullptr, metrics->get_metric("datacache_page_hit_count_last_minute"));
    ASSERT_NE(nullptr, metrics->get_metric("datacache_page_miss_count_last_minute"));
}

// Test that metrics have correct initial values
TEST_F(DataCacheMetricsTest, test_datacache_metrics_initial_values) {
    auto instance = DataCacheMetrics::instance();

    // Initial values should be 0 or positive integers
    ASSERT_GE(instance->datacache_mem_quota_bytes.value(), 0);
    ASSERT_GE(instance->datacache_mem_used_bytes.value(), 0);
    ASSERT_GE(instance->datacache_disk_quota_bytes.value(), 0);
    ASSERT_GE(instance->datacache_disk_used_bytes.value(), 0);
    ASSERT_GE(instance->datacache_meta_used_bytes.value(), 0);
    ASSERT_GE(instance->block_cache_hit_bytes.value(), 0);
    ASSERT_GE(instance->block_cache_miss_bytes.value(), 0);
}

// Test metrics update through hook mechanism
TEST_F(DataCacheMetricsTest, test_metrics_update_hook) {
    auto instance = DataCacheMetrics::instance();
    auto metrics = backend_metrics_registry_for_test();

    // Register the metrics hook
    DataCacheMetrics::instance()->enable_update_hook(false);

    // Trigger the hook manually
    metrics->trigger_hook();

    // After hook execution, values should still be non-negative
    ASSERT_GE(instance->datacache_mem_quota_bytes.value(), 0);
    ASSERT_GE(instance->datacache_mem_used_bytes.value(), 0);
    ASSERT_GE(instance->datacache_disk_quota_bytes.value(), 0);
    ASSERT_GE(instance->datacache_disk_used_bytes.value(), 0);
    ASSERT_GE(instance->datacache_meta_used_bytes.value(), 0);
    ASSERT_GE(instance->block_cache_hit_bytes.value(), 0);
    ASSERT_GE(instance->block_cache_miss_bytes.value(), 0);

    // Used bytes should not exceed quota bytes
    ASSERT_LE(instance->datacache_mem_used_bytes.value(), instance->datacache_mem_quota_bytes.value());
    ASSERT_LE(instance->datacache_disk_used_bytes.value(), instance->datacache_disk_quota_bytes.value());
}

TEST_F(DataCacheMetricsTest, test_metrics_types) {
    auto metrics = backend_metrics_registry_for_test();

    // Quota/used metrics should be gauge type with BYTES unit
    auto mem_quota_metric = metrics->get_metric("datacache_mem_quota_bytes");
    auto mem_used_metric = metrics->get_metric("datacache_mem_used_bytes");
    auto disk_quota_metric = metrics->get_metric("datacache_disk_quota_bytes");
    auto disk_used_metric = metrics->get_metric("datacache_disk_used_bytes");
    auto meta_used_metric = metrics->get_metric("datacache_meta_used_bytes");
    ASSERT_EQ(MetricType::GAUGE, mem_quota_metric->type());
    ASSERT_EQ(MetricType::GAUGE, mem_used_metric->type());
    ASSERT_EQ(MetricType::GAUGE, disk_quota_metric->type());
    ASSERT_EQ(MetricType::GAUGE, disk_used_metric->type());
    ASSERT_EQ(MetricType::GAUGE, meta_used_metric->type());
    ASSERT_EQ(MetricUnit::BYTES, mem_quota_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, mem_used_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, disk_quota_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, disk_used_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, meta_used_metric->unit());

    // Cumulative byte counters should be counter type
    auto hit_bytes_metric = metrics->get_metric("block_cache_hit_bytes");
    auto miss_bytes_metric = metrics->get_metric("block_cache_miss_bytes");
    ASSERT_EQ(MetricType::COUNTER, hit_bytes_metric->type());
    ASSERT_EQ(MetricType::COUNTER, miss_bytes_metric->type());
    ASSERT_EQ(MetricUnit::BYTES, hit_bytes_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, miss_bytes_metric->unit());

    // Cumulative hit/miss count totals should be counter type
    auto hit_count_metric = metrics->get_metric("block_cache_hit_count");
    auto miss_count_metric = metrics->get_metric("block_cache_miss_count");
    ASSERT_EQ(MetricType::COUNTER, hit_count_metric->type());
    ASSERT_EQ(MetricType::COUNTER, miss_count_metric->type());
    ASSERT_EQ(MetricUnit::NOUNIT, hit_count_metric->unit());
    ASSERT_EQ(MetricUnit::NOUNIT, miss_count_metric->unit());

    // Sliding-window and snapshot metrics should be gauge type
    ASSERT_EQ(MetricType::GAUGE, metrics->get_metric("block_cache_hit_count_last_minute")->type());
    ASSERT_EQ(MetricType::GAUGE, metrics->get_metric("block_cache_miss_count_last_minute")->type());
    ASSERT_EQ(MetricType::GAUGE, metrics->get_metric("block_cache_hit_bytes_last_minute")->type());
    ASSERT_EQ(MetricType::GAUGE, metrics->get_metric("block_cache_miss_bytes_last_minute")->type());
    ASSERT_EQ(MetricType::GAUGE, metrics->get_metric("block_cache_current_reading_count")->type());
    ASSERT_EQ(MetricType::GAUGE, metrics->get_metric("block_cache_current_writing_count")->type());
    ASSERT_EQ(MetricType::GAUGE, metrics->get_metric("block_cache_current_removing_count")->type());
    ASSERT_EQ(MetricType::GAUGE, metrics->get_metric("block_cache_buffer_item_count")->type());
    ASSERT_EQ(MetricType::GAUGE, metrics->get_metric("block_cache_buffer_item_bytes")->type());

    // Byte-valued gauges should have BYTES unit
    ASSERT_EQ(MetricUnit::BYTES, metrics->get_metric("block_cache_hit_bytes_last_minute")->unit());
    ASSERT_EQ(MetricUnit::BYTES, metrics->get_metric("block_cache_miss_bytes_last_minute")->unit());
    ASSERT_EQ(MetricUnit::BYTES, metrics->get_metric("block_cache_read_mem_bytes")->unit());
    ASSERT_EQ(MetricUnit::BYTES, metrics->get_metric("block_cache_read_disk_bytes")->unit());
    ASSERT_EQ(MetricUnit::BYTES, metrics->get_metric("block_cache_write_bytes")->unit());
    ASSERT_EQ(MetricUnit::BYTES, metrics->get_metric("block_cache_remove_bytes")->unit());
    ASSERT_EQ(MetricUnit::BYTES, metrics->get_metric("block_cache_buffer_item_bytes")->unit());

    // Page cache metrics should be gauge type with NOUNIT
    ASSERT_EQ(MetricType::GAUGE, metrics->get_metric("datacache_page_hit_count")->type());
    ASSERT_EQ(MetricType::GAUGE, metrics->get_metric("datacache_page_miss_count")->type());
    ASSERT_EQ(MetricType::GAUGE, metrics->get_metric("datacache_page_hit_count_last_minute")->type());
    ASSERT_EQ(MetricType::GAUGE, metrics->get_metric("datacache_page_miss_count_last_minute")->type());
    ASSERT_EQ(MetricUnit::NOUNIT, metrics->get_metric("datacache_page_hit_count")->unit());
    ASSERT_EQ(MetricUnit::NOUNIT, metrics->get_metric("datacache_page_miss_count")->unit());
    ASSERT_EQ(MetricUnit::NOUNIT, metrics->get_metric("datacache_page_hit_count_last_minute")->unit());
    ASSERT_EQ(MetricUnit::NOUNIT, metrics->get_metric("datacache_page_miss_count_last_minute")->unit());
}

#else // !WITH_STARCACHE

// Test that without StarCache, registration is a no-op
TEST_F(DataCacheMetricsTest, test_without_starcache) {
    // When WITH_STARCACHE is not defined, these should be no-ops
    ASSERT_NO_THROW(DataCacheMetrics::instance()->enable_update_hook(false));
    ASSERT_NO_THROW(DataCacheMetrics::instance()->enable_update_hook(true));

    // The function should compile and execute successfully even without StarCache
}

#endif // WITH_STARCACHE

} // namespace starrocks
