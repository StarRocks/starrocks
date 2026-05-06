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

#include "runtime/runtime_metrics.h"

#include <gtest/gtest.h>

#include <string>

namespace starrocks {

namespace {

void assert_metric_value(MetricRegistry* registry, const std::string& name, const std::string& value) {
    auto* metric = registry->get_metric(name);
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ(value, metric->to_string());
}

} // namespace

TEST(RuntimeMetricsTest, InstallRegistersFragmentAndLoadChannelMetrics) {
    MetricRegistry registry("test_registry");
    RuntimeMetrics metrics(&registry);
    metrics.install(&registry);

    metrics.fragment_requests_total.increment(3);
    assert_metric_value(&registry, "fragment_requests_total", "3");

    metrics.fragment_request_duration_us.increment(4);
    assert_metric_value(&registry, "fragment_request_duration_us", "4");

    metrics.load_channel_add_chunks_total.increment(5);
    assert_metric_value(&registry, "load_channel_add_chunks_total", "5");

    metrics.load_channel_add_chunks_eos_total.increment(6);
    assert_metric_value(&registry, "load_channel_add_chunks_eos_total", "6");

    metrics.load_channel_add_chunks_duration_us.increment(7);
    assert_metric_value(&registry, "load_channel_add_chunks_duration_us", "7");

    metrics.load_channel_add_chunks_wait_memtable_duration_us.increment(8);
    assert_metric_value(&registry, "load_channel_add_chunks_wait_memtable_duration_us", "8");

    metrics.load_channel_add_chunks_wait_writer_duration_us.increment(9);
    assert_metric_value(&registry, "load_channel_add_chunks_wait_writer_duration_us", "9");

    metrics.load_channel_add_chunks_wait_replica_duration_us.increment(10);
    assert_metric_value(&registry, "load_channel_add_chunks_wait_replica_duration_us", "10");
}

TEST(RuntimeMetricsTest, InstallRegistersProcessMetrics) {
    MetricRegistry registry("test_registry");
    RuntimeMetrics metrics(&registry);

    metrics.memory_pool_bytes_total.increment(11);
    assert_metric_value(&registry, "memory_pool_bytes_total", "11");

    metrics.process_thread_num.set_value(12);
    assert_metric_value(&registry, "process_thread_num", "12");

    metrics.process_fd_num_used.set_value(13);
    assert_metric_value(&registry, "process_fd_num_used", "13");

    metrics.process_fd_num_limit_soft.set_value(14);
    assert_metric_value(&registry, "process_fd_num_limit_soft", "14");

    metrics.process_fd_num_limit_hard.set_value(15);
    assert_metric_value(&registry, "process_fd_num_limit_hard", "15");

    metrics.max_disk_io_util_percent.set_value(16);
    assert_metric_value(&registry, "max_disk_io_util_percent", "16");

    metrics.max_network_send_bytes_rate.set_value(17);
    assert_metric_value(&registry, "max_network_send_bytes_rate", "17");

    metrics.max_network_receive_bytes_rate.set_value(18);
    assert_metric_value(&registry, "max_network_receive_bytes_rate", "18");

    metrics.exec_runtime_memory_size.increment(19);
    assert_metric_value(&registry, "exec_runtime_memory_size", "19");
}

} // namespace starrocks
