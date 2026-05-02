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

#include "common/metrics/thread_pool_metrics.h"

#include <gtest/gtest.h>

#include "base/metrics.h"
#include "common/thread/threadpool.h"

namespace starrocks {

TEST(ThreadPoolMetricGroupTest, RegisterAndUpdate) {
    MetricRegistry registry("test");
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("metric_test").set_min_threads(1).set_max_threads(2).set_max_queue_size(8).build(&pool)
                        .ok());

    ThreadPoolMetricGroup metrics;
    metrics.install(&registry, "metric_test", pool.get());
    registry.trigger_hook();

    auto* size = registry.get_metric("metric_test_threadpool_size");
    ASSERT_NE(nullptr, size);
    EXPECT_EQ("2", size->to_string());

    auto* queued = registry.get_metric("metric_test_queue_count");
    ASSERT_NE(nullptr, queued);
    EXPECT_EQ(MetricUnit::NOUNIT, queued->unit());

    auto* pending = registry.get_metric("metric_test_pending_time_ns_total");
    ASSERT_NE(nullptr, pending);
    EXPECT_EQ(MetricUnit::NANOSECONDS, pending->unit());
}

} // namespace starrocks
