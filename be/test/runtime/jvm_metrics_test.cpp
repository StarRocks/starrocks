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

#include "runtime/java/jvm_metrics.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>

#include "base/metrics.h"
#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "common/system/cpu_info.h"
#include "runtime/java/java_env.h"
#include "runtime/java/jni_env.h"
#include "runtime/runtime_env_test_util.h"

namespace starrocks {

class JVMMetricsTest : public testing::Test {
public:
    JVMMetricsTest() = default;
    ~JVMMetricsTest() override = default;

protected:
    void SetUp() override {
        if (std::getenv("JAVA_HOME") == nullptr) {
            GTEST_SKIP() << "JAVA_HOME is not set";
        }
        if (getJNIEnv() == nullptr) {
            GTEST_SKIP() << "JVM is unavailable";
        }
    }
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

    // A pool without a defined maximum reports -1 (e.g. G1 Eden/Survivor, Metaspace without
    // MaxMetaspaceSize); its max and peak_max must be hidden instead of exported as negative bytes.
    // Which pools have a maximum depends on the GC, so only check that no exported value is negative.
    struct PoolMaxGauges {
        const char* metric_name;
        IntGauge* max;
        IntGauge* peak_max;
    };
    for (const auto& pool : {PoolMaxGauges{"jvm_young_size_bytes", &jvm_metrics.jvm_young_max_bytes,
                                           &jvm_metrics.jvm_young_peak_max_bytes},
                             PoolMaxGauges{"jvm_old_size_bytes", &jvm_metrics.jvm_old_max_bytes,
                                           &jvm_metrics.jvm_old_peak_max_bytes},
                             PoolMaxGauges{"jvm_survivor_size_bytes", &jvm_metrics.jvm_survivor_max_bytes,
                                           &jvm_metrics.jvm_survivor_peak_max_bytes},
                             PoolMaxGauges{"jvm_perm_size_bytes", &jvm_metrics.jvm_perm_max_bytes,
                                           &jvm_metrics.jvm_perm_peak_max_bytes}}) {
        if (registry.get_metric(pool.metric_name, type_label("max")) != nullptr) {
            ASSERT_GE(pool.max->value(), 0) << pool.metric_name;
        }
        if (registry.get_metric(pool.metric_name, type_label("peak_max")) != nullptr) {
            ASSERT_GE(pool.peak_max->value(), 0) << pool.metric_name;
        }
    }
    // The heap always has a maximum, and hiding a pool's max must not touch its other values.
    ASSERT_NE(nullptr, registry.get_metric("jvm_heap_size_bytes", type_label("max")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_young_size_bytes", type_label("used")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_perm_size_bytes", type_label("committed")));
}

TEST_F(JVMMetricsTest, JavaEnvInstallsJvmMetrics) {
#ifdef __APPLE__
    GTEST_SKIP() << "JVM metrics initialization through JavaEnv is disabled on macOS";
#else
    CpuInfo::init();
    runtime_env_test::set_small_thread_pool_configs();

    MetricRegistry registry("java_env_test");
    JavaEnv java_env;
    DeferOp cleanup([&java_env]() {
        java_env.shutdown();
        java_env.destroy();
    });

    ASSERT_OK(java_env.init(&registry, true));

    auto type_label = [](const std::string& type) { return MetricLabels().add("type", type); };
    ASSERT_NE(nullptr, registry.get_metric("jvm_heap_size_bytes", type_label("used")));
    ASSERT_NE(nullptr, registry.get_metric("jvm_non_heap_size_bytes", type_label("used")));
    // The fixture already created the JVM, so the hook initializes the JNI side and collects.
    registry.trigger_hook();
    ASSERT_GT(JVMMetrics::instance()->jvm_heap_committed_bytes.value(), 0);
#endif
}

// Runs in a fresh process that has no JVM. Exits with 0 when every check passes; a failed check
// prints why and exits with its own non-zero code. std::_Exit skips static destructors, which
// would otherwise release Java global refs while the process is tearing down.
[[noreturn]] static void run_lazy_init_scenario() {
    auto fail = [](int code, const char* message) {
        fprintf(stderr, "%s\n", message);
        std::_Exit(code);
    };
    auto jvm_created = [&fail]() {
        auto created = JVMMetrics::is_jvm_created();
        if (!created.ok()) {
            fail(1, "JNI_GetCreatedJavaVMs failed");
        }
        return created.value();
    };

    if (jvm_created()) {
        fail(2, "a JVM exists before jvm metrics are installed");
    }

    MetricRegistry registry("lazy_init_test");
    JVMMetrics jvm_metrics;
    jvm_metrics.install(&registry);
    if (jvm_created()) {
        fail(3, "installing jvm metrics created a JVM");
    }

    registry.trigger_hook();
    if (jvm_created()) {
        fail(4, "the first jvm metrics hook created a JVM");
    }
    if (jvm_metrics.jvm_heap_committed_bytes.value() != 0) {
        fail(5, "jvm metrics were collected without a JVM");
    }

    if (getJNIEnv() == nullptr) {
        fail(6, "failed to create a JVM");
    }
    registry.trigger_hook();
    if (jvm_metrics.jvm_heap_committed_bytes.value() <= 0) {
        fail(7, "heap committed is not collected after the JVM is created");
    }
    if (jvm_metrics.jvm_non_heap_committed_bytes.value() <= 0) {
        fail(8, "non-heap committed is not collected after the JVM is created");
    }
    std::_Exit(0);
}

// No fixture: JVMMetricsTest::SetUp creates the JVM, and the child process runs the fixture too.
// The "threadsafe" style re-executes the test binary for the child, so the scenario starts in a
// process where no earlier test has created a JVM.
TEST(JVMMetricsLazyInitTest, CollectOnlyAfterJvmIsCreated) {
    if (std::getenv("JAVA_HOME") == nullptr) {
        GTEST_SKIP() << "JAVA_HOME is not set";
    }
    testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(run_lazy_init_scenario(), testing::ExitedWithCode(0), "");
}

} // namespace starrocks
