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

#include "runtime/process_memory_metrics.h"

#include <gtest/gtest.h>

#include <memory>

#include "base/utility/defer_op.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_env.h"

namespace starrocks {

namespace {

MetricRegistry* process_memory_metrics_registry_for_test() {
    static auto* registry = new MetricRegistry("process_memory_metrics_test");
    return registry;
}

} // namespace

TEST(ProcessMemoryMetricsTest, RuntimeEnvReturnsSingleton) {
    ASSERT_EQ(ProcessMemoryMetrics::instance(), RuntimeEnv::GetInstance()->process_memory_metrics());
}

TEST(ProcessMemoryMetricsTest, InstallRegistersProcessMemoryGauges) {
    auto* registry = process_memory_metrics_registry_for_test();
    auto* metrics = RuntimeEnv::GetInstance()->process_memory_metrics();

    metrics->install(registry);

    ASSERT_NE(nullptr, registry->get_metric("jemalloc_allocated_bytes"));
    ASSERT_NE(nullptr, registry->get_metric("process_mem_bytes"));
    ASSERT_NE(nullptr, registry->get_metric("query_mem_bytes"));
    ASSERT_NE(nullptr, registry->get_metric("datacache_mem_bytes"));
}

TEST(ProcessMemoryMetricsTest, UpdateMemoryMetricsReadsRuntimeEnvTrackers) {
    auto* env = RuntimeEnv::GetInstance();
    auto previous_process_mem_tracker = env->_process_mem_tracker;
    env->_process_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::PROCESS, -1, "process");
    DeferOp restore_process_mem_tracker([&] { env->_process_mem_tracker = previous_process_mem_tracker; });

    env->_process_mem_tracker->consume(123);

    auto* metrics = env->process_memory_metrics();
    metrics->process_mem_bytes.set_value(0);
    metrics->update_memory_metrics();

    ASSERT_EQ(123, metrics->process_mem_bytes.value());
}

} // namespace starrocks
