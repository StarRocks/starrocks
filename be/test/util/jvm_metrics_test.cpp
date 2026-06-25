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

#include "util/jvm_metrics.h"

#include <gtest/gtest.h>

#include "testutil/assert.h"
#include "udf/java/java_udf.h"
#include "util/metrics.h"

namespace starrocks {

class JVMMetricsTest : public testing::Test {
public:
    JVMMetricsTest() = default;
    ~JVMMetricsTest() override = default;
};

TEST_F(JVMMetricsTest, normal) {
    MetricRegistry registry("test");
    JVMMetrics jvm_metrics;

    auto st = jvm_metrics.init();
    ASSERT_TRUE(st.ok());
    jvm_metrics.install(&registry);

    // Each JVM gauge is registered under a bare metric name plus a `type` label, so that the
    // Prometheus exposition emits one valid `# TYPE <name>` line per name (see issue #75159).
    auto type_label = [](const std::string& type) { return MetricLabels().add("type", type); };

    ASSERT_NE(nullptr, registry.get_metric("jvm_heap_size_bytes", type_label("used")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_heap_size_bytes", type_label("committed")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_heap_size_bytes", type_label("max")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_non_heap_size_bytes", type_label("used")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_non_heap_size_bytes", type_label("committed")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_young_size_bytes", type_label("used")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_young_size_bytes", type_label("committed")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_young_size_bytes", type_label("max")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_young_size_bytes", type_label("peak_used")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_young_size_bytes", type_label("peak_max")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_old_size_bytes", type_label("used")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_old_size_bytes", type_label("committed")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_old_size_bytes", type_label("max")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_old_size_bytes", type_label("peak_used")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_old_size_bytes", type_label("peak_max")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_survivor_size_bytes", type_label("used")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_survivor_size_bytes", type_label("committed")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_survivor_size_bytes", type_label("max")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_survivor_size_bytes", type_label("peak_used")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_survivor_size_bytes", type_label("peak_max")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_perm_size_bytes", type_label("used")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_perm_size_bytes", type_label("committed")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_perm_size_bytes", type_label("max")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_perm_size_bytes", type_label("peak_used")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_perm_size_bytes", type_label("peak_max")));

    // Regression guard for #75159: the label must NOT be baked into the metric name. If it were,
    // the `# TYPE` line would contain `{...}` and break the entire Prometheus scrape.
    ASSERT_EQ(nullptr, registry.get_metric("jvm_heap_size_bytes{type=\"used\"}"));
    ASSERT_EQ(nullptr, registry.get_metric("jvm_perm_size_bytes{type=\"peak_max\"}"));

    registry.trigger_hook();

    ASSERT_GE(jvm_metrics.jvm_heap_used_bytes.value(), 0);
    ASSERT_GE(jvm_metrics.jvm_heap_committed_bytes.value(), 0);
    ASSERT_GE(jvm_metrics.jvm_non_heap_used_bytes.value(), 0);
    ASSERT_GE(jvm_metrics.jvm_non_heap_committed_bytes.value(), 0);
}

} // namespace starrocks
