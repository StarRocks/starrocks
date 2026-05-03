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

#include <memory>
#include <string>
#include <unordered_map>

#include "base/metrics.h"

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

#define REGISTER_GAUGE_STARROCKS_METRIC(name, func)                                                            \
    GlobalMetricsRegistry::instance()->metrics()->register_metric(#name, &StarRocksMetrics::instance()->name); \
    GlobalMetricsRegistry::instance()->metrics()->register_hook(                                               \
            #name, [&]() { StarRocksMetrics::instance()->name.set_value(func()); });

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
    METRIC_DEFINE_INT_GAUGE(pipe_prepare_pool_queue_len, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(pipe_driver_overloaded, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(runtime_filter_event_queue_len, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(pipe_drivers, MetricUnit::NOUNIT);

    // counters
    METRIC_DEFINE_INT_COUNTER(http_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(http_request_send_bytes, MetricUnit::BYTES);
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
    METRIC_DEFINE_INT_COUNTER(clone_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(clone_requests_failed, MetricUnit::REQUESTS);

    METRIC_DEFINE_INT_COUNTER(finish_task_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(finish_task_requests_failed, MetricUnit::REQUESTS);

    // clone
    METRIC_DEFINE_INT_COUNTER(clone_task_inter_node_copy_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(clone_task_intra_node_copy_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(clone_task_inter_node_copy_duration_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_COUNTER(clone_task_intra_node_copy_duration_ms, MetricUnit::MILLISECONDS);

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

    // Shared-data combined_txn_log collection dispatch counters. Each eos on a
    // LakeTabletsChannel that enters the collection path bumps exactly one of
    // these. Useful for confirming which strategy is live in a given cluster
    // after an upgrade or Config flip.
    //
    // Legacy: FE didn't set `enable_per_partition_coordinator`, or any open on
    // the channel disagreed. Collection falls back to "sender_id == 0 collects
    // all logs".
    METRIC_DEFINE_INT_COUNTER(lake_txn_log_collect_legacy_total, MetricUnit::OPERATIONS);
    // Per-partition coordinator mode: collection runs through the elected
    // coordinator per partition (smallest sender_id among those that claimed
    // the partition via (incremental_)open).
    METRIC_DEFINE_INT_COUNTER(lake_txn_log_collect_per_partition_total, MetricUnit::OPERATIONS);
    // Diagnostic: number of txn logs produced for a partition that *no*
    // sender on this channel ever claimed via its (incremental_)open tablet
    // list. Such a log is dropped (no coordinator covers it), which would
    // silently re-introduce the publish-time loss this fix targets. Counted
    // only once per orphan log (by the minimum elected coordinator). A
    // healthy cluster must hold this at 0; any non-zero value points to a
    // missing open RPC or an open/data-arrival race and warrants
    // investigation. Each orphan is also logged at ERROR on the CN.
    METRIC_DEFINE_INT_COUNTER(lake_txn_log_collect_orphan_partition_total, MetricUnit::NOUNIT);

    // Metrics for metadata lru cache
    METRIC_DEFINE_INT_GAUGE(metadata_cache_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(segment_file_not_found_total, MetricUnit::OPERATIONS);

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
    METRIC_DEFINE_INT_COUNTER(primary_key_table_error_state_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(primary_key_wait_apply_done_duration_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_COUNTER(primary_key_wait_apply_done_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(pk_index_sst_read_error_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(pk_index_sst_write_error_total, MetricUnit::REQUESTS);

    // StarOS shared-data fallback metrics. Incremented when StarOSWorker issues
    // a g_starlet->get_shard_info() RPC to starmgr because the local cache did
    // not have the shard info (i.e. the FE did not push the shard to this BE
    // before a query referenced it). A high rate is a signal of FE-side
    // task/node mis-selection or shard push lag.
    METRIC_DEFINE_INT_COUNTER(staros_shard_info_fallback_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(staros_shard_info_fallback_failed_total, MetricUnit::REQUESTS);

    // Gauges
    IntGaugeMetricsMap disks_total_capacity;
    IntGaugeMetricsMap disks_avail_capacity;
    IntGaugeMetricsMap disks_data_used_capacity;
    IntGaugeMetricsMap disks_state;

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

    // thread pool metrics
    METRICS_DEFINE_THREAD_POOL(publish_version);
    METRICS_DEFINE_THREAD_POOL(async_delta_writer);
    METRICS_DEFINE_THREAD_POOL(load_spill_block_merge);
    METRICS_DEFINE_THREAD_POOL(memtable_flush);
    METRICS_DEFINE_THREAD_POOL(lake_memtable_flush);
    METRICS_DEFINE_THREAD_POOL(segment_replicate);
    METRICS_DEFINE_THREAD_POOL(segment_flush);
    METRICS_DEFINE_THREAD_POOL(update_apply);
    METRICS_DEFINE_THREAD_POOL(pk_index_compaction);
    METRICS_DEFINE_THREAD_POOL(compact_pool);
    METRICS_DEFINE_THREAD_POOL(pindex_load);
    METRICS_DEFINE_THREAD_POOL(cloud_native_pk_index_compact);
    METRICS_DEFINE_THREAD_POOL(exec_state_report);
    METRICS_DEFINE_THREAD_POOL(priority_exec_state_report);
    METRICS_DEFINE_THREAD_POOL(pip_prepare);
    METRICS_DEFINE_THREAD_POOL(tablet_internal_parallel_merge);

    // agent server thread pools
    METRICS_DEFINE_THREAD_POOL(drop);
    METRICS_DEFINE_THREAD_POOL(create_tablet);
    METRICS_DEFINE_THREAD_POOL(alter_tablet);
    METRICS_DEFINE_THREAD_POOL(clear_transaction);
    METRICS_DEFINE_THREAD_POOL(storage_medium_migrate);
    METRICS_DEFINE_THREAD_POOL(check_consistency);
    METRICS_DEFINE_THREAD_POOL(manual_compaction);
    METRICS_DEFINE_THREAD_POOL(compaction_control);
    METRICS_DEFINE_THREAD_POOL(update_schema);
    METRICS_DEFINE_THREAD_POOL(upload);
    METRICS_DEFINE_THREAD_POOL(download);
    METRICS_DEFINE_THREAD_POOL(make_snapshot);
    METRICS_DEFINE_THREAD_POOL(release_snapshot);
    METRICS_DEFINE_THREAD_POOL(move_dir);
    METRICS_DEFINE_THREAD_POOL(update_tablet_meta_info);
    METRICS_DEFINE_THREAD_POOL(drop_auto_increment_map_dir);
    METRICS_DEFINE_THREAD_POOL(clone);
    METRICS_DEFINE_THREAD_POOL(remote_snapshot);
    METRICS_DEFINE_THREAD_POOL(replicate_snapshot);

    // short circuit executor
    METRIC_DEFINE_INT_COUNTER(short_circuit_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(short_circuit_request_duration_us, MetricUnit::MICROSECONDS);

    static StarRocksMetrics* instance() {
        static StarRocksMetrics instance;
        return &instance;
    }

private:
    StarRocksMetrics();
};

}; // namespace starrocks
