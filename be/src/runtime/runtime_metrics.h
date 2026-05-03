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

#pragma once

#include <functional>
#include <string>
#include <vector>

#include "base/metrics.h"

namespace starrocks {

#define REGISTER_GAUGE_RUNTIME_METRIC(registry, name, func)                \
    (registry)->register_metric(#name, &RuntimeMetrics::instance()->name); \
    (registry)->register_hook(#name, [&]() { RuntimeMetrics::instance()->name.set_value(func()); });

#define REGISTER_THREAD_POOL_RUNTIME_METRICS(registry, name, threadpool)                        \
    do {                                                                                        \
        REGISTER_GAUGE_RUNTIME_METRIC(registry, name##_threadpool_size,                         \
                                      [this]() { return threadpool->max_threads(); })           \
        REGISTER_GAUGE_RUNTIME_METRIC(registry, name##_executed_tasks_total,                    \
                                      [this]() { return threadpool->total_executed_tasks(); })  \
        REGISTER_GAUGE_RUNTIME_METRIC(registry, name##_pending_time_ns_total,                   \
                                      [this]() { return threadpool->total_pending_time_ns(); }) \
        REGISTER_GAUGE_RUNTIME_METRIC(registry, name##_execute_time_ns_total,                   \
                                      [this]() { return threadpool->total_execute_time_ns(); }) \
        REGISTER_GAUGE_RUNTIME_METRIC(registry, name##_queue_count,                             \
                                      [this]() { return threadpool->num_queued_tasks(); })      \
        REGISTER_GAUGE_RUNTIME_METRIC(registry, name##_running_threads,                         \
                                      [this]() { return threadpool->num_threads(); })           \
        REGISTER_GAUGE_RUNTIME_METRIC(registry, name##_active_threads,                          \
                                      [this]() { return threadpool->active_threads(); })        \
    } while (false)

class RuntimeMetrics {
public:
    RuntimeMetrics() = default;
    explicit RuntimeMetrics(MetricRegistry* registry) { install(registry); }
    ~RuntimeMetrics() = default;

    static RuntimeMetrics* instance();

    void install(MetricRegistry* registry);
    void register_runtime_filter_event_queue_len_hook(std::function<int64_t()> value_fn);

    METRIC_DEFINE_INT_COUNTER(fragment_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(fragment_request_duration_us, MetricUnit::MICROSECONDS);

    METRIC_DEFINE_INT_COUNTER(lake_txn_log_collect_legacy_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(lake_txn_log_collect_per_partition_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(lake_txn_log_collect_orphan_partition_total, MetricUnit::NOUNIT);

    METRICS_DEFINE_THREAD_POOL(load_channel);
    METRIC_DEFINE_INT_COUNTER(load_channel_add_chunks_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(load_channel_add_chunks_eos_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(load_channel_add_chunks_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(load_channel_add_chunks_wait_memtable_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(load_channel_add_chunks_wait_writer_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(load_channel_add_chunks_wait_replica_duration_us, MetricUnit::MICROSECONDS);

    METRIC_DEFINE_INT_GAUGE(memory_pool_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(process_thread_num, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(process_fd_num_used, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(process_fd_num_limit_soft, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(process_fd_num_limit_hard, MetricUnit::NOUNIT);

    METRIC_DEFINE_INT_GAUGE(max_disk_io_util_percent, MetricUnit::PERCENT);
    METRIC_DEFINE_INT_GAUGE(max_network_send_bytes_rate, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(max_network_receive_bytes_rate, MetricUnit::BYTES);

    METRIC_DEFINE_UINT_GAUGE(broker_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(data_stream_receiver_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(fragment_endpoint_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(active_scan_context_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(plan_fragment_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(load_channel_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(result_buffer_block_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(result_block_queue_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(routine_load_task_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(small_file_cache_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(stream_load_pipe_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(brpc_endpoint_stub_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(tablet_writer_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_GAUGE(load_rpc_threadpool_size, MetricUnit::NOUNIT);

    METRICS_DEFINE_THREAD_POOL(merge_commit);
    METRICS_DEFINE_THREAD_POOL(put_aggregate_metadata);
    METRICS_DEFINE_THREAD_POOL(lake_metadata_fetch);
    METRICS_DEFINE_THREAD_POOL(lake_vi_build);
    METRICS_DEFINE_THREAD_POOL(cloud_native_pk_index_execution);
    METRICS_DEFINE_THREAD_POOL(cloud_native_pk_index_memtable_flush);
    METRICS_DEFINE_THREAD_POOL(lake_partial_update);
    METRICS_DEFINE_THREAD_POOL(automatic_partition);

    METRIC_DEFINE_INT_COUNTER(exec_runtime_memory_size, MetricUnit::BYTES);

    METRIC_DEFINE_INT_GAUGE(runtime_filter_event_queue_len, MetricUnit::NOUNIT);

private:
    struct PendingIntGaugeHook {
        std::string name;
        IntGauge* metric;
        std::function<int64_t()> value_fn;
    };

    void _register_int_gauge_hook(const std::string& name, IntGauge* metric, std::function<int64_t()> value_fn);

    MetricRegistry* _registry = nullptr;
    std::vector<PendingIntGaugeHook> _pending_int_gauge_hooks;
};

} // namespace starrocks
