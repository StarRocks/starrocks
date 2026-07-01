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

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/scoped_updater.h"
#include "base/utility/defer_op.h"
#include "cache/datacache.h"
#include "cache/mem_cache/lrucache_engine.h"
#include "cache/mem_cache/page_cache.h"
#include "common/config_cache_fwd.h"
#include "common/config_storage_fwd.h"
#include "common/metrics/process_metrics_registry.h"
#include "runtime/mem_tracker.h"

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

TEST_F(DataCacheMetricsTest, test_memory_tracker_hook_updates_cache_trackers) {
    const bool old_disable_storage_page_cache = config::disable_storage_page_cache;
    const bool old_block_cache_enable = config::block_cache_enable;
    SCOPED_UPDATE(bool, config::datacache_enable, false);
    SCOPED_UPDATE(bool, config::disable_storage_page_cache, old_disable_storage_page_cache);
    SCOPED_UPDATE(bool, config::block_cache_enable, old_block_cache_enable);
    DataCacheMetrics::instance()->enable_update_hook(true);

    auto* cache_env = DataCache::GetInstance();
    cache_env->destroy();

    MemTracker datacache_tracker(-1, "datacache");
    MemTracker page_cache_tracker(-1, "page_cache");
    DataCacheInitOptions options;
    options.datacache_mem_tracker = &datacache_tracker;
    options.page_cache_mem_tracker = &page_cache_tracker;
    ASSERT_OK(cache_env->init(options));
    DeferOp cleanup([cache_env] { cache_env->destroy(); });

    MemCacheOptions mem_cache_options{.mem_space_size = 4 * 1024 * 1024};
    auto lru_cache = std::make_shared<LRUCacheEngine>();
    ASSERT_OK(lru_cache->init(mem_cache_options));
    auto page_cache = std::make_shared<StoragePageCache>(lru_cache.get());
    cache_env->set_page_cache(page_cache);

    std::string key("cache_memory_tracker");
    auto data = std::make_unique<std::vector<uint8_t>>(1024);
    PageCacheHandle handle;
    MemCacheWriteOptions write_options;
    ASSERT_OK(page_cache->insert(key, data.get(), write_options, &handle));
    data.release();

    ASSERT_EQ(0, page_cache_tracker.consumption());
    backend_metrics_registry_for_test()->trigger_hook();
    ASSERT_EQ(0, datacache_tracker.consumption());
    ASSERT_EQ(page_cache->memory_usage(), page_cache_tracker.consumption());
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

// Test metrics types are correct (should be INT_GAUGE)
TEST_F(DataCacheMetricsTest, test_metrics_types) {
    auto metrics = backend_metrics_registry_for_test();

    auto mem_quota_metric = metrics->get_metric("datacache_mem_quota_bytes");
    auto mem_used_metric = metrics->get_metric("datacache_mem_used_bytes");
    auto disk_quota_metric = metrics->get_metric("datacache_disk_quota_bytes");
    auto disk_used_metric = metrics->get_metric("datacache_disk_used_bytes");
    auto meta_used_metric = metrics->get_metric("datacache_meta_used_bytes");
    auto hit_bytes_metric = metrics->get_metric("block_cache_hit_bytes");
    auto miss_bytes_metric = metrics->get_metric("block_cache_miss_bytes");

    ASSERT_NE(nullptr, mem_quota_metric);
    ASSERT_NE(nullptr, mem_used_metric);
    ASSERT_NE(nullptr, disk_quota_metric);
    ASSERT_NE(nullptr, disk_used_metric);
    ASSERT_NE(nullptr, meta_used_metric);
    ASSERT_NE(nullptr, hit_bytes_metric);
    ASSERT_NE(nullptr, miss_bytes_metric);

    // Quota/used metrics should be gauge type
    ASSERT_EQ(MetricType::GAUGE, mem_quota_metric->type());
    ASSERT_EQ(MetricType::GAUGE, mem_used_metric->type());
    ASSERT_EQ(MetricType::GAUGE, disk_quota_metric->type());
    ASSERT_EQ(MetricType::GAUGE, disk_used_metric->type());
    ASSERT_EQ(MetricType::GAUGE, meta_used_metric->type());

    // Hit/miss byte counters should be counter type
    ASSERT_EQ(MetricType::COUNTER, hit_bytes_metric->type());
    ASSERT_EQ(MetricType::COUNTER, miss_bytes_metric->type());

    // All should have BYTES unit
    ASSERT_EQ(MetricUnit::BYTES, mem_quota_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, mem_used_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, disk_quota_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, disk_used_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, meta_used_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, hit_bytes_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, miss_bytes_metric->unit());
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
