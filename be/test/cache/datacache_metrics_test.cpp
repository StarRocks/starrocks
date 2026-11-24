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
#include "cache/disk_cache/starcache_engine.h"
#include "testutil/assert.h"
#endif

namespace starrocks {

<<<<<<< HEAD
=======
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
    Status update_lru_shard_refill(bool fallback_enable, uint32_t low_watermark_percent,
                                   uint32_t high_watermark_percent) override {
        return Status::OK();
    }
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

>>>>>>> 0f7b287 ([BugFix] Support datacache lru shard refill configs (#84193))
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
