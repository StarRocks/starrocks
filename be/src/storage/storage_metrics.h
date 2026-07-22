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
#include "common/metrics/thread_pool_metric_group.h"

namespace starrocks {

class ThreadPool;

class StorageMetrics {
public:
    StorageMetrics() = default;
    explicit StorageMetrics(MetricRegistry* registry) { install(registry); }
    ~StorageMetrics() = default;

    static StorageMetrics* instance();

    void install(MetricRegistry* registry);
    void register_thread_pool_metrics(const std::string& name, ThreadPoolMetricGroup* metric_group,
                                      ThreadPool* threadpool);
    void register_metadata_cache_bytes_total_hook(std::function<int64_t()> value_fn);
    void register_unused_rowsets_count_hook(std::function<uint64_t()> value_fn);
    void register_rowset_count_generated_and_in_use_hook(std::function<uint64_t()> value_fn);

    METRIC_DEFINE_INT_COUNTER(push_requests_success_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(push_requests_fail_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(push_request_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(push_request_write_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(push_request_write_rows, MetricUnit::ROWS);
    METRIC_DEFINE_INT_GAUGE(push_request_write_bytes_per_second, MetricUnit::BYTES);

    METRIC_DEFINE_INT_COUNTER(meta_write_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(meta_write_request_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(meta_read_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(meta_read_request_duration_us, MetricUnit::MICROSECONDS);

    METRIC_DEFINE_INT_COUNTER(segment_read_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(segment_row_total, MetricUnit::ROWS);
    METRIC_DEFINE_INT_COUNTER(segment_rows_by_short_key, MetricUnit::ROWS);
    METRIC_DEFINE_INT_COUNTER(segment_rows_read_by_zone_map, MetricUnit::ROWS);

    METRIC_DEFINE_INT_COUNTER(txn_persist_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(txn_persist_duration_us, MetricUnit::MICROSECONDS);

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
    // SDCG (sparse delta column group) observability: lightweight overlay-chain merge + conflict handling.
    METRIC_DEFINE_INT_COUNTER(sdcg_overlay_merge_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(sdcg_overlay_merge_layers_folded_total, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_COUNTER(sdcg_compaction_conflict_replayable_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(sdcg_compaction_conflict_discard_total, MetricUnit::OPERATIONS);
    // Outcome of conflict replay (distinct from *_replayable_total, which counts classified-replayable
    // conflicts INCLUDING those discarded because replay is disabled / preconditions unmet). *_executed_total
    // counts conflicts whose racing overlays were actually re-applied onto the kept compaction output;
    // *_overlay_bytes_total is the space the discard path would have orphaned (the space-reclaim proof).
    METRIC_DEFINE_INT_COUNTER(sdcg_compaction_conflict_replay_executed_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(sdcg_compaction_conflict_replay_winner_rows_total, MetricUnit::ROWS);
    METRIC_DEFINE_INT_COUNTER(sdcg_compaction_conflict_replay_skipped_rows_total, MetricUnit::ROWS);
    METRIC_DEFINE_INT_COUNTER(sdcg_compaction_conflict_replay_overlay_bytes_total, MetricUnit::BYTES);
    // partial_update_mode=auto: which write mode the adaptive selector actually resolved to, per
    // (load for row, or per-segment for the column formats). Lets prod observe the auto decision mix
    // without VLOG. row = level-1 masked full-row rewrite; dense/sparse = level-2 column format;
    // packed/masked = flexible (heterogeneous) column formats.
    METRIC_DEFINE_INT_COUNTER(sdcg_write_mode_row_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(sdcg_write_mode_dense_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(sdcg_write_mode_sparse_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(sdcg_write_mode_packed_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(sdcg_write_mode_masked_dense_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(primary_key_wait_apply_done_duration_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_COUNTER(primary_key_wait_apply_done_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(pk_index_sst_read_error_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(pk_index_sst_write_error_total, MetricUnit::REQUESTS);

    METRIC_DEFINE_INT_COUNTER(readable_blocks_total, MetricUnit::BLOCKS);
    METRIC_DEFINE_INT_COUNTER(writable_blocks_total, MetricUnit::BLOCKS);
    METRIC_DEFINE_INT_COUNTER(blocks_created_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(blocks_deleted_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(bytes_read_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(bytes_written_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(disk_sync_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_GAUGE(blocks_open_reading, MetricUnit::BLOCKS);
    METRIC_DEFINE_INT_GAUGE(blocks_open_writing, MetricUnit::BLOCKS);

    METRIC_DEFINE_UINT_GAUGE(rowset_count_generated_and_in_use, MetricUnit::ROWSETS);
    METRIC_DEFINE_UINT_GAUGE(unused_rowsets_count, MetricUnit::ROWSETS);

    METRIC_DEFINE_INT_COUNTER(storage_migrate_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(create_tablet_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(create_tablet_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(drop_tablet_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_all_tablets_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(report_tablet_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(create_rollup_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(create_rollup_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(delete_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(delete_requests_failed, MetricUnit::REQUESTS);
    // Lake-only ADD/DROP INDEX fast path (Index Delta Group). Total counts the
    // entries into do_process_add_index_only / do_process_drop_index_only.
    // lake_idg_files_written_total counts every .idx file successfully written
    // by AddIndexSchemaChange::build_idg_for_segment (one per base segment).
    METRIC_DEFINE_INT_COUNTER(lake_add_index_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(lake_add_index_requests_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(lake_drop_index_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(lake_idg_files_written_total, MetricUnit::OPERATIONS);

    METRIC_DEFINE_INT_COUNTER(base_compaction_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(base_compaction_request_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(cumulative_compaction_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(cumulative_compaction_request_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(update_compaction_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(update_compaction_request_failed, MetricUnit::REQUESTS);

    METRIC_DEFINE_INT_COUNTER(base_compaction_deltas_total, MetricUnit::ROWSETS);
    METRIC_DEFINE_INT_COUNTER(base_compaction_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(cumulative_compaction_deltas_total, MetricUnit::ROWSETS);
    METRIC_DEFINE_INT_COUNTER(cumulative_compaction_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(update_compaction_deltas_total, MetricUnit::ROWSETS);
    METRIC_DEFINE_INT_COUNTER(update_compaction_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(update_compaction_outputs_total, MetricUnit::ROWSETS);
    METRIC_DEFINE_INT_COUNTER(update_compaction_outputs_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(update_compaction_duration_us, MetricUnit::MICROSECONDS);

    METRIC_DEFINE_INT_COUNTER(async_delta_writer_execute_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(async_delta_writer_task_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(async_delta_writer_task_execute_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(async_delta_writer_task_pending_duration_us, MetricUnit::MICROSECONDS);

    METRIC_DEFINE_INT_COUNTER(delta_writer_commit_task_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(delta_writer_wait_flush_task_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(delta_writer_wait_flush_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(delta_writer_wait_replica_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(delta_writer_txn_commit_duration_us, MetricUnit::MICROSECONDS);

    METRIC_DEFINE_INT_COUNTER(memtable_flush_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(memtable_finalize_task_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(memtable_finalize_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(memtable_flush_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(memtable_flush_io_time_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(memtable_flush_memory_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(memtable_flush_disk_bytes_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(segment_flush_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(segment_flush_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(segment_flush_io_time_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(segment_flush_bytes_total, MetricUnit::BYTES);

    METRIC_DEFINE_INT_COUNTER(update_rowset_commit_request_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(update_rowset_commit_request_failed, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(update_rowset_commit_apply_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(update_rowset_commit_apply_duration_us, MetricUnit::MICROSECONDS);

    METRIC_DEFINE_INT_GAUGE(tablet_cumulative_max_compaction_score, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(tablet_base_max_compaction_score, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(tablet_update_max_compaction_score, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(max_tablet_rowset_num, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(wait_cumulative_compaction_task_num, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(wait_base_compaction_task_num, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(running_cumulative_compaction_task_num, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(running_base_compaction_task_num, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(running_update_compaction_task_num, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(cumulative_compaction_task_cost_time_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_GAUGE(base_compaction_task_cost_time_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_GAUGE(update_compaction_task_cost_time_ns, MetricUnit::NANOSECONDS);
    METRIC_DEFINE_INT_GAUGE(base_compaction_task_byte_per_second, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(cumulative_compaction_task_byte_per_second, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(update_compaction_task_byte_per_second, MetricUnit::BYTES);

    METRICS_DEFINE_THREAD_POOL(async_delta_writer);
    METRICS_DEFINE_THREAD_POOL(memtable_flush);
    METRICS_DEFINE_THREAD_POOL(lake_memtable_flush);
    METRICS_DEFINE_THREAD_POOL(storage_cleanup);
    METRICS_DEFINE_THREAD_POOL(lake_schema_change);
    METRICS_DEFINE_THREAD_POOL(segment_replicate);
    METRICS_DEFINE_THREAD_POOL(segment_flush);
    METRICS_DEFINE_THREAD_POOL(update_apply);
    METRICS_DEFINE_THREAD_POOL(pk_index_compaction);
    METRICS_DEFINE_THREAD_POOL(compact_pool);
    METRICS_DEFINE_THREAD_POOL(pindex_load);
    METRICS_DEFINE_THREAD_POOL(cloud_native_pk_index_compact);

private:
    struct PendingThreadPoolMetrics {
        std::string name;
        ThreadPoolMetricGroup* metric_group;
        ThreadPool* threadpool;
    };

    struct PendingIntGaugeHook {
        std::string name;
        IntGauge* metric;
        std::function<int64_t()> value_fn;
    };

    struct PendingUIntGaugeHook {
        std::string name;
        UIntGauge* metric;
        std::function<uint64_t()> value_fn;
    };

    void _register_thread_pool_metrics(const std::string& name, ThreadPoolMetricGroup* metric_group,
                                       ThreadPool* threadpool);
    void _register_int_gauge_hook(const std::string& name, IntGauge* metric, std::function<int64_t()> value_fn);
    void _register_uint_gauge_hook(const std::string& name, UIntGauge* metric, std::function<uint64_t()> value_fn);

    MetricRegistry* _registry = nullptr;
    std::vector<PendingThreadPoolMetrics> _pending_thread_pool_metrics;
    std::vector<PendingIntGaugeHook> _pending_int_gauge_hooks;
    std::vector<PendingUIntGaugeHook> _pending_uint_gauge_hooks;
};

} // namespace starrocks

#define REGISTER_STORAGE_THREAD_POOL_METRICS(storage_metrics, metric_name, threadpool)                      \
    do {                                                                                                    \
        auto* metric_owner = (storage_metrics);                                                             \
        metric_owner->register_thread_pool_metrics(#metric_name, &metric_owner->metric_name, (threadpool)); \
    } while (false)
