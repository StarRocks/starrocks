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

    ASSERT_NE(nullptr, registry.get_metric("jvm_heap_used_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_heap_committed_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_heap_max_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_nonheap_used_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_nonheap_committed_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_young_used_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_young_committed_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_young_max_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_young_peak_used_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_young_peak_max_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_old_used_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_old_committed_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_old_max_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_old_peak_used_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_old_peak_max_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_survivor_used_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_survivor_committed_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_survivor_max_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_survivor_peak_used_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_survivor_peak_max_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_perm_used_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_perm_committed_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_perm_max_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_perm_peak_used_bytes"));
    ASSERT_NE(nullptr, registry.get_metric("jvm_perm_peak_max_bytes"));

    registry.trigger_hook();

    ASSERT_GE(jvm_metrics.jvm_heap_used_bytes.value(), 0);
    ASSERT_GE(jvm_metrics.jvm_heap_committed_bytes.value(), 0);
    ASSERT_GE(jvm_metrics.jvm_nonheap_used_bytes.value(), 0);
    ASSERT_GE(jvm_metrics.jvm_nonheap_committed_bytes.value(), 0);
}

} // namespace starrocks
