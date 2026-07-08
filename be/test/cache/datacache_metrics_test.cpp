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

#include "base/testutil/scoped_updater.h"
#include "base/utility/defer_op.h"
#include "cache/data_cache_hit_rate_counter.hpp"
#include "cache/datacache.h"
#include "cache/mem_cache/page_cache.h"
#include "common/config_cache_fwd.h"
#include "common/metrics/process_metrics_registry.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

namespace {

class FakeMemCacheEngine final : public LocalMemCacheEngine {
public:
    bool is_initialized() const override { return initialized; }

    Status insert(const std::string& key, void* value, size_t size, MemCacheDeleter deleter, MemCacheHandlePtr* handle,
                  const MemCacheWriteOptions& options) override {
        return Status::OK();
    }

    Status lookup(const std::string& key, MemCacheHandlePtr* handle, MemCacheReadOptions* options) override {
        return Status::NotFound("not found");
    }

    void release(MemCacheHandlePtr handle) override {}
    const void* value(MemCacheHandlePtr handle) override { return nullptr; }
    bool exist(const std::string& key) const override { return false; }
    Status remove(const std::string& key) override { return Status::OK(); }
    Status adjust_mem_quota(int64_t delta, size_t min_capacity) override { return Status::OK(); }
    Status update_mem_quota(size_t quota_bytes) override { return Status::OK(); }
    const DataCacheMemMetrics cache_metrics() const override { return metrics; }
    Status shutdown() override { return Status::OK(); }
    bool has_mem_cache() const override { return true; }
    bool available() const override { return initialized; }
    bool mem_cache_available() const override { return initialized; }
    size_t mem_quota() const override { return metrics.mem_quota_bytes; }
    size_t mem_usage() const override { return metrics.mem_used_bytes; }
    size_t lookup_count() const override { return 0; }
    size_t hit_count() const override { return 0; }
    size_t insert_count() const override { return 0; }
    size_t insert_evict_count() const override { return 0; }
    size_t release_evict_count() const override { return 0; }
    Status prune() override { return Status::OK(); }

    bool initialized = true;
    DataCacheMemMetrics metrics;
};

class FakeDiskCacheEngine final : public LocalDiskCacheEngine {
public:
    bool is_initialized() const override { return initialized; }
    Status write(const std::string& key, const IOBuffer& buffer, DiskCacheWriteOptions* options) override {
        return Status::OK();
    }
    Status read(const std::string& key, size_t off, size_t size, IOBuffer* buffer,
                DiskCacheReadOptions* options) override {
        return Status::OK();
    }
    bool exist(const std::string& key) const override { return false; }
    Status remove(const std::string& key) override { return Status::OK(); }
    Status update_disk_spaces(const std::vector<DirSpace>& spaces) override { return Status::OK(); }
    Status update_inline_cache_count_limit(int32_t limit) override { return Status::OK(); }
    const DataCacheDiskMetrics cache_metrics() const override { return metrics; }
    void record_read_remote(size_t size, int64_t latency_us) override {}
    void record_read_cache(size_t size, int64_t latency_us) override {}
    Status shutdown() override { return Status::OK(); }
    bool has_disk_cache() const override { return true; }
    bool available() const override { return initialized; }
    void disk_spaces(std::vector<DirSpace>* spaces) const override { spaces->clear(); }
    size_t lookup_count() const override { return lookup_count_val; }
    size_t hit_count() const override { return hit_count_val; }
    Status prune() override { return Status::OK(); }

    bool initialized = true;
    DataCacheDiskMetrics metrics;
    size_t lookup_count_val = 0;
    size_t hit_count_val = 0;
};

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

    void TearDown() override {
        auto* cache = DataCache::GetInstance();
        cache->disable_metrics_update_hook();
        cache->set_mem_trackers(nullptr, nullptr);
        cache->_local_mem_cache.reset();
        cache->set_local_disk_cache(nullptr);
        cache->set_page_cache(nullptr);
        DataCacheHitRateCounter::instance()->reset();
    }
};

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
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_hit_count"));
    ASSERT_NE(nullptr, metrics->get_metric("block_cache_miss_count"));
}

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
    ASSERT_GE(instance->block_cache_hit_count.value(), 0);
    ASSERT_GE(instance->block_cache_miss_count.value(), 0);
}

TEST_F(DataCacheMetricsTest, test_update_snapshot_sets_values) {
    auto instance = DataCacheMetrics::instance();
    DataCacheMetricsSnapshot snapshot{.mem_quota_bytes = 1,
                                      .mem_used_bytes = 2,
                                      .disk_quota_bytes = 3,
                                      .disk_used_bytes = 4,
                                      .meta_used_bytes = 5,
                                      .block_cache_hit_bytes = 6,
                                      .block_cache_miss_bytes = 7,
                                      .block_cache_hit_count = 8,
                                      .block_cache_miss_count = 9};

    instance->update(snapshot);

    ASSERT_EQ(1, instance->datacache_mem_quota_bytes.value());
    ASSERT_EQ(2, instance->datacache_mem_used_bytes.value());
    ASSERT_EQ(3, instance->datacache_disk_quota_bytes.value());
    ASSERT_EQ(4, instance->datacache_disk_used_bytes.value());
    ASSERT_EQ(5, instance->datacache_meta_used_bytes.value());
    ASSERT_EQ(6, instance->block_cache_hit_bytes.value());
    ASSERT_EQ(7, instance->block_cache_miss_bytes.value());
    ASSERT_EQ(8, instance->block_cache_hit_count.value());
    ASSERT_EQ(9, instance->block_cache_miss_count.value());
}

TEST_F(DataCacheMetricsTest, test_datacache_owns_metrics_update_hook) {
    SCOPED_UPDATE(bool, config::datacache_unified_instance_enable, true);
    auto* cache = DataCache::GetInstance();
    auto mem_cache = std::make_shared<FakeMemCacheEngine>();
    mem_cache->metrics = {.mem_quota_bytes = 100, .mem_used_bytes = 20};
    auto page_cache_engine = std::make_shared<FakeMemCacheEngine>();
    page_cache_engine->metrics = {.mem_quota_bytes = 300, .mem_used_bytes = 40};
    auto page_cache = std::make_shared<StoragePageCache>(page_cache_engine.get());
    auto disk_cache = std::make_shared<FakeDiskCacheEngine>();
    disk_cache->metrics = {
            .status = DataCacheStatus::NORMAL, .disk_quota_bytes = 1000, .disk_used_bytes = 200, .meta_used_bytes = 30};
    disk_cache->hit_count_val = 11;
    disk_cache->lookup_count_val = 15;
    MemTracker datacache_mem_tracker(MemTrackerType::DATACACHE, -1, "datacache");
    MemTracker pagecache_mem_tracker(MemTrackerType::PAGE_CACHE, -1, "page_cache");
    cache->_local_mem_cache = mem_cache;
    cache->set_local_disk_cache(disk_cache);
    cache->set_page_cache(page_cache);
    cache->set_mem_trackers(&datacache_mem_tracker, &pagecache_mem_tracker);
    DataCacheHitRateCounter::instance()->update_block_cache_stat(7, 9);

    auto status = cache->enable_metrics_update_hook(backend_metrics_registry_for_test(), true);
    ASSERT_TRUE(status.ok()) << status.to_string();

    backend_metrics_registry_for_test()->trigger_hook();

    auto* instance = DataCacheMetrics::instance();
    ASSERT_EQ(100, instance->datacache_mem_quota_bytes.value());
    ASSERT_EQ(20, instance->datacache_mem_used_bytes.value());
    ASSERT_EQ(1000, instance->datacache_disk_quota_bytes.value());
    ASSERT_EQ(200, instance->datacache_disk_used_bytes.value());
    ASSERT_EQ(30, instance->datacache_meta_used_bytes.value());
    ASSERT_EQ(7, instance->block_cache_hit_bytes.value());
    ASSERT_EQ(9, instance->block_cache_miss_bytes.value());
    ASSERT_EQ(11, instance->block_cache_hit_count.value());
    ASSERT_EQ(4, instance->block_cache_miss_count.value());
    ASSERT_EQ(20, datacache_mem_tracker.consumption());
    ASSERT_EQ(40, pagecache_mem_tracker.consumption());
}

TEST_F(DataCacheMetricsTest, test_update_mem_trackers_sets_cache_trackers) {
    SCOPED_UPDATE(bool, config::datacache_unified_instance_enable, true);
    auto* cache = DataCache::GetInstance();
    auto mem_cache = std::make_shared<FakeMemCacheEngine>();
    mem_cache->metrics = {.mem_quota_bytes = 100, .mem_used_bytes = 20};
    auto page_cache_engine = std::make_shared<FakeMemCacheEngine>();
    page_cache_engine->metrics = {.mem_quota_bytes = 300, .mem_used_bytes = 40};
    auto page_cache = std::make_shared<StoragePageCache>(page_cache_engine.get());
    MemTracker datacache_mem_tracker(MemTrackerType::DATACACHE, -1, "datacache");
    MemTracker pagecache_mem_tracker(MemTrackerType::PAGE_CACHE, -1, "page_cache");
    cache->_local_mem_cache = mem_cache;
    cache->set_page_cache(page_cache);
    cache->set_mem_trackers(&datacache_mem_tracker, &pagecache_mem_tracker);

    cache->update_mem_trackers();

    ASSERT_EQ(20, datacache_mem_tracker.consumption());
    ASSERT_EQ(40, pagecache_mem_tracker.consumption());
}

TEST_F(DataCacheMetricsTest, test_update_mem_trackers_handles_null_trackers) {
    auto* cache = DataCache::GetInstance();
    cache->set_mem_trackers(nullptr, nullptr);

    cache->update_mem_trackers();
}

TEST_F(DataCacheMetricsTest, test_datacache_hook_runs_before_process_memory_metrics) {
    SCOPED_UPDATE(bool, config::datacache_unified_instance_enable, true);
    auto* cache = DataCache::GetInstance();
    auto mem_cache = std::make_shared<FakeMemCacheEngine>();
    mem_cache->metrics = {.mem_quota_bytes = 100, .mem_used_bytes = 20};
    MemTracker datacache_mem_tracker(MemTrackerType::DATACACHE, -1, "datacache");
    cache->_local_mem_cache = mem_cache;
    cache->set_mem_trackers(&datacache_mem_tracker, nullptr);

    auto* registry = backend_metrics_registry_for_test();
    auto status = cache->enable_metrics_update_hook(registry, true);
    ASSERT_TRUE(status.ok()) << status.to_string();

    int64_t observed_datacache_mem_bytes = 0;
    ASSERT_TRUE(registry->register_hook("process_memory_metrics",
                                        [&] { observed_datacache_mem_bytes = datacache_mem_tracker.consumption(); }));
    DeferOp deregister_process_memory_metrics_hook([&] { registry->deregister_hook("process_memory_metrics"); });

    mem_cache->metrics.mem_used_bytes = 55;
    registry->trigger_hook();

    ASSERT_EQ(55, observed_datacache_mem_bytes);
}

TEST_F(DataCacheMetricsTest, test_metrics_types) {
    auto metrics = backend_metrics_registry_for_test();

    auto mem_quota_metric = metrics->get_metric("datacache_mem_quota_bytes");
    auto mem_used_metric = metrics->get_metric("datacache_mem_used_bytes");
    auto disk_quota_metric = metrics->get_metric("datacache_disk_quota_bytes");
    auto disk_used_metric = metrics->get_metric("datacache_disk_used_bytes");
    auto meta_used_metric = metrics->get_metric("datacache_meta_used_bytes");
    auto hit_bytes_metric = metrics->get_metric("block_cache_hit_bytes");
    auto miss_bytes_metric = metrics->get_metric("block_cache_miss_bytes");
    auto hit_count_metric = metrics->get_metric("block_cache_hit_count");
    auto miss_count_metric = metrics->get_metric("block_cache_miss_count");

    ASSERT_NE(nullptr, mem_quota_metric);
    ASSERT_NE(nullptr, mem_used_metric);
    ASSERT_NE(nullptr, disk_quota_metric);
    ASSERT_NE(nullptr, disk_used_metric);
    ASSERT_NE(nullptr, meta_used_metric);
    ASSERT_NE(nullptr, hit_bytes_metric);
    ASSERT_NE(nullptr, miss_bytes_metric);
    ASSERT_NE(nullptr, hit_count_metric);
    ASSERT_NE(nullptr, miss_count_metric);

    // Quota/used metrics should be gauge type
    ASSERT_EQ(MetricType::GAUGE, mem_quota_metric->type());
    ASSERT_EQ(MetricType::GAUGE, mem_used_metric->type());
    ASSERT_EQ(MetricType::GAUGE, disk_quota_metric->type());
    ASSERT_EQ(MetricType::GAUGE, disk_used_metric->type());
    ASSERT_EQ(MetricType::GAUGE, meta_used_metric->type());

    // Hit/miss byte counters should be counter type
    ASSERT_EQ(MetricType::COUNTER, hit_bytes_metric->type());
    ASSERT_EQ(MetricType::COUNTER, miss_bytes_metric->type());

    // Hit/miss count totals should be counter type with NOUNIT
    ASSERT_EQ(MetricType::COUNTER, hit_count_metric->type());
    ASSERT_EQ(MetricType::COUNTER, miss_count_metric->type());

    // All should have BYTES unit
    ASSERT_EQ(MetricUnit::BYTES, mem_quota_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, mem_used_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, disk_quota_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, disk_used_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, meta_used_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, hit_bytes_metric->unit());
    ASSERT_EQ(MetricUnit::BYTES, miss_bytes_metric->unit());
    ASSERT_EQ(MetricUnit::NOUNIT, hit_count_metric->unit());
    ASSERT_EQ(MetricUnit::NOUNIT, miss_count_metric->unit());
}

} // namespace starrocks
