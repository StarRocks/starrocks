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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/doris_metrics.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "util/metrics.h"
#include "util/system_metrics.h"

namespace starrocks {

class IntGaugeMetricsMap {
public:
    void set_metric(const std::string& key, int64_t val) {
        auto metric = metrics.find(key);
        if (metric != metrics.end()) {
            metric->second->set_value(val);
        }
    }

    IntGauge* add_metric(const std::string& key, const MetricUnit unit) {
        metrics.emplace(key, new IntGauge(unit));
        return metrics.find(key)->second.get();
    }

private:
    std::unordered_map<std::string, std::unique_ptr<IntGauge>> metrics;
};

#define REGISTER_GAUGE_STARROCKS_METRIC(name, func)                                                       \
    StarRocksMetrics::instance()->metrics()->register_metric(#name, &StarRocksMetrics::instance()->name); \
    StarRocksMetrics::instance()->metrics()->register_hook(                                               \
            #name, [&]() { StarRocksMetrics::instance()->name.set_value(func()); });

#define METRICS_DEFINE_THREAD_POOL(threadpool_name)                                             \
    METRIC_DEFINE_UINT_GAUGE(threadpool_name##_threadpool_size, MetricUnit::NOUNIT);            \
    METRIC_DEFINE_UINT_GAUGE(threadpool_name##_executed_tasks_total, MetricUnit::NOUNIT);       \
    METRIC_DEFINE_UINT_GAUGE(threadpool_name##_pending_time_ns_total, MetricUnit::NANOSECONDS); \
    METRIC_DEFINE_UINT_GAUGE(threadpool_name##_execute_time_ns_total, MetricUnit::NANOSECONDS); \
    METRIC_DEFINE_UINT_GAUGE(threadpool_name##_queue_count, MetricUnit::NOUNIT);                \
    METRIC_DEFINE_UINT_GAUGE(threadpool_name##_running_threads, MetricUnit::NOUNIT);            \
    METRIC_DEFINE_UINT_GAUGE(threadpool_name##_active_threads, MetricUnit::NOUNIT)

#define REGISTER_THREAD_POOL_METRICS(name, threadpool)                                                            \
    do {                                                                                                          \
        REGISTER_GAUGE_STARROCKS_METRIC(name##_threadpool_size, [this]() { return threadpool->max_threads(); })   \
        REGISTER_GAUGE_STARROCKS_METRIC(name##_executed_tasks_total,                                              \
                                        [this]() { return threadpool->total_executed_tasks(); })                  \
        REGISTER_GAUGE_STARROCKS_METRIC(name##_pending_time_ns_total,                                             \
                                        [this]() { return threadpool->total_pending_time_ns(); })                 \
        REGISTER_GAUGE_STARROCKS_METRIC(name##_execute_time_ns_total,                                             \
                                        [this]() { return threadpool->total_execute_time_ns(); })                 \
        REGISTER_GAUGE_STARROCKS_METRIC(name##_queue_count, [this]() { return threadpool->num_queued_tasks(); })  \
        REGISTER_GAUGE_STARROCKS_METRIC(name##_running_threads, [this]() { return threadpool->num_threads(); })   \
        REGISTER_GAUGE_STARROCKS_METRIC(name##_active_threads, [this]() { return threadpool->active_threads(); }) \
    } while (false)

class StarRocksMetrics {
public:
    // query execution
    METRIC_DEFINE_INT_GAUGE(pipe_prepare_pool_queue_len, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(pipe_scan_executor_queuing, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(pipe_driver_overloaded, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(pipe_driver_schedule_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(pipe_driver_execution_time, MetricUnit::NANOSECONDS);
    METRIC_DEFINE_INT_GAUGE(pipe_driver_queue_len, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(pipe_poller_block_queue_len, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(query_scan_bytes_per_second, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(query_scan_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(query_scan_rows, MetricUnit::ROWS);
    METRIC_DEFINE_INT_GAUGE(pipe_drivers, MetricUnit::NOUNIT);

    // counters
    METRIC_DEFINE_INT_COUNTER(fragment_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(fragment_request_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(http_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(http_request_send_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(push_requests_success_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(push_requests_fail_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(push_request_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(push_request_write_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(push_request_write_rows, MetricUnit::ROWS);
    METRIC_DEFINE_INT_COUNTER(create_tablet_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(create_tablet_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(drop_tablet_requests_total, MetricUnit::REQUESTS);

    METRIC_DEFINE_INT_COUNTER(report_all_tablets_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_all_tablets_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_tablet_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_tablet_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_disk_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_disk_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_task_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_task_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_workgroup_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_workgroup_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_resource_usage_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_resource_usage_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_datacache_metrics_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_datacache_metrics_requests_failed, MetricUnit::REQUESTS);

    METRIC_DEFINE_INT_COUNTER(schema_change_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(schema_change_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(create_rollup_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(create_rollup_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(storage_migrate_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(delete_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(delete_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(clone_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(clone_requests_failed, MetricUnit::REQUESTS);

    METRIC_DEFINE_INT_COUNTER(finish_task_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(finish_task_requests_failed, MetricUnit::REQUESTS);

    // Compaction Task Metric
    // compaction task num, including all finished tasks and failed tasks
    METRIC_DEFINE_INT_COUNTER(base_compaction_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(base_compaction_request_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(cumulative_compaction_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(cumulative_compaction_request_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(update_compaction_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(update_compaction_request_failed, MetricUnit::REQUESTS);

    // compaction task rate
    METRIC_DEFINE_INT_COUNTER(base_compaction_deltas_total, MetricUnit::ROWSETS);
    METRIC_DEFINE_INT_COUNTER(base_compaction_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(cumulative_compaction_deltas_total, MetricUnit::ROWSETS);
    METRIC_DEFINE_INT_COUNTER(cumulative_compaction_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(update_compaction_deltas_total, MetricUnit::ROWSETS);
    METRIC_DEFINE_INT_COUNTER(update_compaction_bytes_total, MetricUnit::BYTES);

    METRIC_DEFINE_INT_COUNTER(update_compaction_outputs_total, MetricUnit::ROWSETS);
    METRIC_DEFINE_INT_COUNTER(update_compaction_outputs_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(update_compaction_duration_us, MetricUnit::MICROSECONDS);

    METRIC_DEFINE_INT_COUNTER(publish_task_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(publish_task_failed_total, MetricUnit::REQUESTS);

    METRIC_DEFINE_INT_COUNTER(meta_write_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(meta_write_request_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(meta_read_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(meta_read_request_duration_us, MetricUnit::MICROSECONDS);

    // Counters for segment_v2
    // -----------------------
    // total number of segments read
    METRIC_DEFINE_INT_COUNTER(segment_read_total, MetricUnit::OPERATIONS);
    // total number of rows in queried segments (before index pruning)
    METRIC_DEFINE_INT_COUNTER(segment_row_total, MetricUnit::ROWS);
    // total number of rows selected by short key index
    METRIC_DEFINE_INT_COUNTER(segment_rows_by_short_key, MetricUnit::ROWS);
    // total number of rows selected by zone map index
    METRIC_DEFINE_INT_COUNTER(segment_rows_read_by_zone_map, MetricUnit::ROWS);

    METRIC_DEFINE_INT_COUNTER(txn_begin_request_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(txn_commit_request_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(txn_rollback_request_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(txn_exec_plan_total, MetricUnit::OPERATIONS);

    METRIC_DEFINE_INT_COUNTER(txn_persist_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(txn_persist_duration_us, MetricUnit::MICROSECONDS);

    METRIC_DEFINE_INT_COUNTER(stream_receive_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(stream_load_rows_total, MetricUnit::ROWS);
    METRIC_DEFINE_INT_COUNTER(load_rows_total, MetricUnit::ROWS);
    METRIC_DEFINE_INT_COUNTER(load_bytes_total, MetricUnit::BYTES);

    // Metrics for LoadChannel
    // The number that LoadChannel#add_chunks is accessed
    METRIC_DEFINE_INT_COUNTER(load_channel_add_chunks_total, MetricUnit::OPERATIONS);
    // Accumulated time that LoadChannel#add_chunks costs. The time can be divided into
    // waiting memtable, waiting async delta writer, waiting replicas, and others.
    METRIC_DEFINE_INT_COUNTER(load_channel_add_chunks_duration_us, MetricUnit::MICROSECONDS);
    // Accumulated time that waiting memtable costs in LoadChannel#add_chunks
    METRIC_DEFINE_INT_COUNTER(load_channel_add_chunks_wait_memtable_duration_us, MetricUnit::MICROSECONDS);
    // Accumulated time that waiting async delta writer costs in LoadChannel#add_chunks
    METRIC_DEFINE_INT_COUNTER(load_channel_add_chunks_wait_writer_duration_us, MetricUnit::MICROSECONDS);
    // Accumulated time that waiting replicas costs in LoadChannel#add_chunks
    METRIC_DEFINE_INT_COUNTER(load_channel_add_chunks_wait_replica_duration_us, MetricUnit::MICROSECONDS);

    // Metrics for async delta writer
    // The number of AsyncDeltaWriter::_execute is accessed. Each execution may run multiple tasks
    METRIC_DEFINE_INT_COUNTER(async_delta_writer_execute_total, MetricUnit::OPERATIONS);
    // The number of task that executed
    METRIC_DEFINE_INT_COUNTER(async_delta_writer_task_total, MetricUnit::OPERATIONS);
    // Accumulated time that task execution costs
    METRIC_DEFINE_INT_COUNTER(async_delta_writer_task_execute_duration_us, MetricUnit::MICROSECONDS);
    // Accumulated time that task pends in the queue
    METRIC_DEFINE_INT_COUNTER(async_delta_writer_task_pending_duration_us, MetricUnit::MICROSECONDS);

    // Metrics for metadata lru cache
    METRIC_DEFINE_INT_GAUGE(metadata_cache_bytes_total, MetricUnit::BYTES);

    // Metrics for delta writer
    // Accumulated time that delta writer waits for memtable flush. It's part of
    // async_delta_writer_task_execute_duration_us
    METRIC_DEFINE_INT_COUNTER(delta_writer_wait_flush_duration_us, MetricUnit::MICROSECONDS);
    // Accumulated time that delta writer waits for secondary replicas sync. It's part of
    // async_delta_writer_task_execute_duration_us
    METRIC_DEFINE_INT_COUNTER(delta_writer_wait_replica_duration_us, MetricUnit::MICROSECONDS);

    METRIC_DEFINE_INT_COUNTER(memtable_flush_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(memtable_finalize_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(memtable_flush_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(memtable_flush_io_time_us, MetricUnit::MICROSECONDS);
    // total memory size of memtables
    METRIC_DEFINE_INT_COUNTER(memtable_flush_memory_bytes_total, MetricUnit::BYTES);
    // total disk size of memtables which is smaller than memtable_flush_memory_bytes_total because of compression
    METRIC_DEFINE_INT_COUNTER(memtable_flush_disk_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(segment_flush_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(segment_flush_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(segment_flush_io_time_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(segment_flush_bytes_total, MetricUnit::BYTES);

    METRIC_DEFINE_INT_COUNTER(update_rowset_commit_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(update_rowset_commit_request_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(update_rowset_commit_apply_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(update_rowset_commit_apply_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_UINT_GAUGE(update_primary_index_num, MetricUnit::OPERATIONS);
    METRIC_DEFINE_UINT_GAUGE(update_primary_index_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_UINT_GAUGE(update_del_vector_num, MetricUnit::OPERATIONS);
    METRIC_DEFINE_UINT_GAUGE(update_del_vector_dels_num, MetricUnit::OPERATIONS);
    METRIC_DEFINE_UINT_GAUGE(update_del_vector_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_UINT_COUNTER(update_del_vector_deletes_total, MetricUnit::NOUNIT);
    METRIC_DEFINE_UINT_COUNTER(update_del_vector_deletes_new, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_COUNTER(column_partial_update_apply_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(column_partial_update_apply_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(delta_column_group_get_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(delta_column_group_get_hit_cache, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(delta_column_group_get_non_pk_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(delta_column_group_get_non_pk_hit_cache, MetricUnit::REQUESTS);

    // Gauges
    METRIC_DEFINE_INT_GAUGE(memory_pool_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(process_thread_num, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(process_fd_num_used, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(process_fd_num_limit_soft, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(process_fd_num_limit_hard, MetricUnit::NOUNIT);
    IntGaugeMetricsMap disks_total_capacity;
    IntGaugeMetricsMap disks_avail_capacity;
    IntGaugeMetricsMap disks_data_used_capacity;
    IntGaugeMetricsMap disks_state;

    // Compaction Task Metric
    // the max compaction score of all tablets.
    // Record base and cumulative scores separately, because
    // we need to get the larger of the two.
    METRIC_DEFINE_INT_GAUGE(tablet_cumulative_max_compaction_score, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(tablet_base_max_compaction_score, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(tablet_update_max_compaction_score, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(max_tablet_rowset_num, MetricUnit::NOUNIT);

    // compaction task num, including waiting tasks and running tasks
    METRIC_DEFINE_INT_GAUGE(wait_cumulative_compaction_task_num, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(wait_base_compaction_task_num, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(running_cumulative_compaction_task_num, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(running_base_compaction_task_num, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(running_update_compaction_task_num, MetricUnit::NOUNIT);

    // compaction task cost time
    METRIC_DEFINE_INT_GAUGE(cumulative_compaction_task_cost_time_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_GAUGE(base_compaction_task_cost_time_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_GAUGE(update_compaction_task_cost_time_ns, MetricUnit::NANOSECONDS);

    // compaction task rate
    METRIC_DEFINE_INT_GAUGE(base_compaction_task_byte_per_second, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(cumulative_compaction_task_byte_per_second, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(update_compaction_task_byte_per_second, MetricUnit::BYTES);

    // The following metrics will be calculated
    // by metric calculator
    METRIC_DEFINE_INT_GAUGE(push_request_write_bytes_per_second, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(max_disk_io_util_percent, MetricUnit::PERCENT);
    METRIC_DEFINE_INT_GAUGE(max_network_send_bytes_rate, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(max_network_receive_bytes_rate, MetricUnit::BYTES);

    // Metrics related with BlockManager
    METRIC_DEFINE_INT_COUNTER(readable_blocks_total, MetricUnit::BLOCKS);
    METRIC_DEFINE_INT_COUNTER(writable_blocks_total, MetricUnit::BLOCKS);
    METRIC_DEFINE_INT_COUNTER(blocks_created_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(blocks_deleted_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(bytes_read_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(bytes_written_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(disk_sync_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_GAUGE(blocks_open_reading, MetricUnit::BLOCKS);
    METRIC_DEFINE_INT_GAUGE(blocks_open_writing, MetricUnit::BLOCKS);

    // Size of some global containers
    METRIC_DEFINE_UINT_GAUGE(rowset_count_generated_and_in_use, MetricUnit::ROWSETS);
    METRIC_DEFINE_UINT_GAUGE(unused_rowsets_count, MetricUnit::ROWSETS);
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

    // thread pool metrics
    METRICS_DEFINE_THREAD_POOL(publish_version);
    METRICS_DEFINE_THREAD_POOL(async_delta_writer);
    METRICS_DEFINE_THREAD_POOL(memtable_flush);
    METRICS_DEFINE_THREAD_POOL(lake_memtable_flush);
    METRICS_DEFINE_THREAD_POOL(segment_replicate);
    METRICS_DEFINE_THREAD_POOL(segment_flush);
    METRICS_DEFINE_THREAD_POOL(update_apply);
    METRICS_DEFINE_THREAD_POOL(pk_index_compaction);

    METRIC_DEFINE_UINT_GAUGE(load_rpc_threadpool_size, MetricUnit::NOUNIT);

    // short circuit executor
    METRIC_DEFINE_INT_COUNTER(short_circuit_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(short_circuit_request_duration_us, MetricUnit::MICROSECONDS);

    static StarRocksMetrics* instance() {
        static StarRocksMetrics instance;
        return &instance;
    }

    // not thread-safe, call before calling metrics
    void initialize(const std::vector<std::string>& paths = std::vector<std::string>(),
                    bool init_system_metrics = false,
                    const std::set<std::string>& disk_devices = std::set<std::string>(),
                    const std::vector<std::string>& network_interfaces = std::vector<std::string>());

    MetricRegistry* metrics() { return &_metrics; }
    SystemMetrics* system_metrics() { return &_system_metrics; }

private:
    // Don't allow constrctor
    StarRocksMetrics();

    void _update();
    void _update_process_thread_num();
    void _update_process_fd_num();

private:
    static const std::string _s_registry_name;
    static const std::string _s_hook_name;

    MetricRegistry _metrics;
    SystemMetrics _system_metrics;
};

}; // namespace starrocks
