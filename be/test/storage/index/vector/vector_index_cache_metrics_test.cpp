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

#include "storage/index/vector/vector_index_cache_metrics.h"

#include <gtest/gtest.h>

namespace starrocks {

namespace {

void assert_metric_registered(MetricRegistry* registry, const std::string& name) {
    ASSERT_NE(nullptr, registry->get_metric(name));
}

} // namespace

TEST(VectorIndexCacheMetricsTest, InstallRegistersMetrics) {
    MetricRegistry registry("test_registry");
    VectorIndexCacheMetrics metrics;

    ASSERT_TRUE(metrics.install(&registry));

    assert_metric_registered(&registry, "vector_index_cache_capacity");
    assert_metric_registered(&registry, "vector_index_cache_usage");
    assert_metric_registered(&registry, "vector_index_cache_usage_ratio");
    assert_metric_registered(&registry, "vector_index_cache_lookup_count");
    assert_metric_registered(&registry, "vector_index_cache_hit_count");
    assert_metric_registered(&registry, "vector_index_cache_hit_ratio");
    assert_metric_registered(&registry, "vector_index_cache_dynamic_lookup_count");
    assert_metric_registered(&registry, "vector_index_cache_dynamic_hit_count");
    assert_metric_registered(&registry, "vector_index_cache_dynamic_hit_ratio");
}

TEST(VectorIndexCacheMetricsTest, RefreshPublishesSnapshotAndDynamicDeltas) {
    MetricRegistry registry("test_registry");
    VectorIndexCacheMetrics metrics(&registry);

    metrics.update(/*capacity=*/100, /*usage=*/25, /*lookup_count=*/4, /*hit_count=*/3);
    registry.trigger_hook();

    EXPECT_EQ(100, metrics.vector_index_cache_capacity.value());
    EXPECT_EQ(25, metrics.vector_index_cache_usage.value());
    EXPECT_DOUBLE_EQ(0.25, metrics.vector_index_cache_usage_ratio.value());
    EXPECT_EQ(4, metrics.vector_index_cache_lookup_count.value());
    EXPECT_EQ(3, metrics.vector_index_cache_hit_count.value());
    EXPECT_DOUBLE_EQ(0.75, metrics.vector_index_cache_hit_ratio.value());
    EXPECT_EQ(4, metrics.vector_index_cache_dynamic_lookup_count.value());
    EXPECT_EQ(3, metrics.vector_index_cache_dynamic_hit_count.value());
    EXPECT_DOUBLE_EQ(0.75, metrics.vector_index_cache_dynamic_hit_ratio.value());

    metrics.update(/*capacity=*/200, /*usage=*/100, /*lookup_count=*/7, /*hit_count=*/5);
    registry.trigger_hook();

    EXPECT_EQ(200, metrics.vector_index_cache_capacity.value());
    EXPECT_EQ(100, metrics.vector_index_cache_usage.value());
    EXPECT_DOUBLE_EQ(0.5, metrics.vector_index_cache_usage_ratio.value());
    EXPECT_EQ(7, metrics.vector_index_cache_lookup_count.value());
    EXPECT_EQ(5, metrics.vector_index_cache_hit_count.value());
    EXPECT_DOUBLE_EQ(5.0 / 7.0, metrics.vector_index_cache_hit_ratio.value());
    EXPECT_EQ(3, metrics.vector_index_cache_dynamic_lookup_count.value());
    EXPECT_EQ(2, metrics.vector_index_cache_dynamic_hit_count.value());
    EXPECT_DOUBLE_EQ(2.0 / 3.0, metrics.vector_index_cache_dynamic_hit_ratio.value());
}

TEST(VectorIndexCacheMetricsTest, RefreshHandlesCounterResetWithoutUnsignedUnderflow) {
    MetricRegistry registry("test_registry");
    VectorIndexCacheMetrics metrics(&registry);

    metrics.update(/*capacity=*/100, /*usage=*/10, /*lookup_count=*/10, /*hit_count=*/8);
    registry.trigger_hook();

    metrics.update(/*capacity=*/100, /*usage=*/0, /*lookup_count=*/1, /*hit_count=*/1);
    registry.trigger_hook();

    EXPECT_EQ(1, metrics.vector_index_cache_lookup_count.value());
    EXPECT_EQ(1, metrics.vector_index_cache_hit_count.value());
    EXPECT_EQ(1, metrics.vector_index_cache_dynamic_lookup_count.value());
    EXPECT_EQ(1, metrics.vector_index_cache_dynamic_hit_count.value());
    EXPECT_DOUBLE_EQ(1.0, metrics.vector_index_cache_dynamic_hit_ratio.value());
}

} // namespace starrocks
