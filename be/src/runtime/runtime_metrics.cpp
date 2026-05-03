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

#include <utility>

#include "gutil/macros.h"

namespace starrocks {

RuntimeMetrics* RuntimeMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new RuntimeMetrics();
    return instance;
}

void RuntimeMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _registry = registry;

#define REGISTER_RUNTIME_METRIC(name) registry->register_metric(#name, &name)

    REGISTER_RUNTIME_METRIC(fragment_requests_total);
    REGISTER_RUNTIME_METRIC(fragment_request_duration_us);

    REGISTER_RUNTIME_METRIC(lake_txn_log_collect_legacy_total);
    REGISTER_RUNTIME_METRIC(lake_txn_log_collect_per_partition_total);
    REGISTER_RUNTIME_METRIC(lake_txn_log_collect_orphan_partition_total);

    REGISTER_RUNTIME_METRIC(load_channel_add_chunks_total);
    REGISTER_RUNTIME_METRIC(load_channel_add_chunks_eos_total);
    REGISTER_RUNTIME_METRIC(load_channel_add_chunks_duration_us);
    REGISTER_RUNTIME_METRIC(load_channel_add_chunks_wait_memtable_duration_us);
    REGISTER_RUNTIME_METRIC(load_channel_add_chunks_wait_writer_duration_us);
    REGISTER_RUNTIME_METRIC(load_channel_add_chunks_wait_replica_duration_us);

    REGISTER_RUNTIME_METRIC(memory_pool_bytes_total);
    REGISTER_RUNTIME_METRIC(process_thread_num);
    REGISTER_RUNTIME_METRIC(process_fd_num_used);
    REGISTER_RUNTIME_METRIC(process_fd_num_limit_soft);
    REGISTER_RUNTIME_METRIC(process_fd_num_limit_hard);

    REGISTER_RUNTIME_METRIC(max_disk_io_util_percent);
    REGISTER_RUNTIME_METRIC(max_network_send_bytes_rate);
    REGISTER_RUNTIME_METRIC(max_network_receive_bytes_rate);

    REGISTER_RUNTIME_METRIC(exec_runtime_memory_size);

    for (auto& pending : _pending_int_gauge_hooks) {
        _register_int_gauge_hook(pending.name, pending.metric, std::move(pending.value_fn));
    }
    _pending_int_gauge_hooks.clear();

#undef REGISTER_RUNTIME_METRIC
}

void RuntimeMetrics::register_runtime_filter_event_queue_len_hook(std::function<int64_t()> value_fn) {
    if (_registry == nullptr) {
        _pending_int_gauge_hooks.emplace_back(PendingIntGaugeHook{
                "runtime_filter_event_queue_len", &runtime_filter_event_queue_len, std::move(value_fn)});
        return;
    }
    _register_int_gauge_hook("runtime_filter_event_queue_len", &runtime_filter_event_queue_len, std::move(value_fn));
}

void RuntimeMetrics::_register_int_gauge_hook(const std::string& name, IntGauge* metric,
                                              std::function<int64_t()> value_fn) {
    DCHECK(_registry != nullptr);
    DCHECK(metric != nullptr);
    _registry->register_metric(name, metric);
    _registry->register_hook(name, [metric, value_fn = std::move(value_fn)] { metric->set_value(value_fn()); });
}

} // namespace starrocks
