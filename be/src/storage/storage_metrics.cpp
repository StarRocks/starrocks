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

#include "storage/storage_metrics.h"

#include "common/thread/threadpool.h"
#include "gutil/macros.h"

namespace starrocks {

namespace {

void register_thread_pool_metric_group(MetricRegistry* registry, const std::string& name, ThreadPool* threadpool,
                                       UIntGauge* threadpool_size, UIntGauge* executed_tasks_total,
                                       UIntGauge* pending_time_ns_total, UIntGauge* execute_time_ns_total,
                                       UIntGauge* queue_count, UIntGauge* running_threads, UIntGauge* active_threads) {
    DCHECK(registry != nullptr);
    DCHECK(threadpool != nullptr);

    const auto threadpool_size_name = name + "_threadpool_size";
    registry->register_metric(threadpool_size_name, threadpool_size);
    registry->register_hook(threadpool_size_name,
                            [threadpool_size, threadpool] { threadpool_size->set_value(threadpool->max_threads()); });

    const auto executed_tasks_total_name = name + "_executed_tasks_total";
    registry->register_metric(executed_tasks_total_name, executed_tasks_total);
    registry->register_hook(executed_tasks_total_name, [executed_tasks_total, threadpool] {
        executed_tasks_total->set_value(threadpool->total_executed_tasks());
    });

    const auto pending_time_ns_total_name = name + "_pending_time_ns_total";
    registry->register_metric(pending_time_ns_total_name, pending_time_ns_total);
    registry->register_hook(pending_time_ns_total_name, [pending_time_ns_total, threadpool] {
        pending_time_ns_total->set_value(threadpool->total_pending_time_ns());
    });

    const auto execute_time_ns_total_name = name + "_execute_time_ns_total";
    registry->register_metric(execute_time_ns_total_name, execute_time_ns_total);
    registry->register_hook(execute_time_ns_total_name, [execute_time_ns_total, threadpool] {
        execute_time_ns_total->set_value(threadpool->total_execute_time_ns());
    });

    const auto queue_count_name = name + "_queue_count";
    registry->register_metric(queue_count_name, queue_count);
    registry->register_hook(queue_count_name,
                            [queue_count, threadpool] { queue_count->set_value(threadpool->num_queued_tasks()); });

    const auto running_threads_name = name + "_running_threads";
    registry->register_metric(running_threads_name, running_threads);
    registry->register_hook(running_threads_name,
                            [running_threads, threadpool] { running_threads->set_value(threadpool->num_threads()); });

    const auto active_threads_name = name + "_active_threads";
    registry->register_metric(active_threads_name, active_threads);
    registry->register_hook(active_threads_name,
                            [active_threads, threadpool] { active_threads->set_value(threadpool->active_threads()); });
}

} // namespace

StorageMetrics* StorageMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new StorageMetrics();
    return instance;
}

void StorageMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _registry = registry;

#define REGISTER_STORAGE_METRIC(name) registry->register_metric(#name, &name)
#define REGISTER_ENGINE_REQUEST_METRIC(type, status, metric)                                                     \
    registry->register_metric("engine_requests_total", MetricLabels().add("type", #type).add("status", #status), \
                              &metric)

    registry->register_metric("push_requests_total", MetricLabels().add("status", "SUCCESS"),
                              &push_requests_success_total);
    registry->register_metric("push_requests_total", MetricLabels().add("status", "FAIL"), &push_requests_fail_total);
    REGISTER_STORAGE_METRIC(push_request_duration_us);
    REGISTER_STORAGE_METRIC(push_request_write_bytes);
    REGISTER_STORAGE_METRIC(push_request_write_rows);

    registry->register_metric("meta_request_total", MetricLabels().add("type", "write"), &meta_write_request_total);
    registry->register_metric("meta_request_total", MetricLabels().add("type", "read"), &meta_read_request_total);
    registry->register_metric("meta_request_duration", MetricLabels().add("type", "write"),
                              &meta_write_request_duration_us);
    registry->register_metric("meta_request_duration", MetricLabels().add("type", "read"),
                              &meta_read_request_duration_us);

    registry->register_metric("segment_read", MetricLabels().add("type", "segment_total_read_times"),
                              &segment_read_total);
    registry->register_metric("segment_read", MetricLabels().add("type", "segment_total_row_num"), &segment_row_total);
    registry->register_metric("segment_read", MetricLabels().add("type", "segment_rows_by_short_key"),
                              &segment_rows_by_short_key);
    registry->register_metric("segment_read", MetricLabels().add("type", "segment_rows_read_by_zone_map"),
                              &segment_rows_read_by_zone_map);

    REGISTER_STORAGE_METRIC(txn_persist_total);
    REGISTER_STORAGE_METRIC(txn_persist_duration_us);
    REGISTER_STORAGE_METRIC(segment_file_not_found_total);

    REGISTER_STORAGE_METRIC(update_primary_index_num);
    REGISTER_STORAGE_METRIC(update_primary_index_bytes_total);
    REGISTER_STORAGE_METRIC(update_del_vector_num);
    REGISTER_STORAGE_METRIC(update_del_vector_dels_num);
    REGISTER_STORAGE_METRIC(update_del_vector_bytes_total);
    REGISTER_STORAGE_METRIC(update_del_vector_deletes_total);
    REGISTER_STORAGE_METRIC(update_del_vector_deletes_new);
    REGISTER_STORAGE_METRIC(column_partial_update_apply_total);
    REGISTER_STORAGE_METRIC(column_partial_update_apply_duration_us);
    REGISTER_STORAGE_METRIC(delta_column_group_get_total);
    REGISTER_STORAGE_METRIC(delta_column_group_get_hit_cache);
    REGISTER_STORAGE_METRIC(delta_column_group_get_non_pk_total);
    REGISTER_STORAGE_METRIC(delta_column_group_get_non_pk_hit_cache);
    REGISTER_STORAGE_METRIC(primary_key_table_error_state_total);
    REGISTER_STORAGE_METRIC(primary_key_wait_apply_done_duration_ms);
    REGISTER_STORAGE_METRIC(primary_key_wait_apply_done_total);
    REGISTER_STORAGE_METRIC(pk_index_sst_read_error_total);
    REGISTER_STORAGE_METRIC(pk_index_sst_write_error_total);

    REGISTER_STORAGE_METRIC(readable_blocks_total);
    REGISTER_STORAGE_METRIC(writable_blocks_total);
    REGISTER_STORAGE_METRIC(blocks_created_total);
    REGISTER_STORAGE_METRIC(blocks_deleted_total);
    REGISTER_STORAGE_METRIC(bytes_read_total);
    REGISTER_STORAGE_METRIC(bytes_written_total);
    REGISTER_STORAGE_METRIC(disk_sync_total);
    REGISTER_STORAGE_METRIC(blocks_open_reading);
    REGISTER_STORAGE_METRIC(blocks_open_writing);

    REGISTER_ENGINE_REQUEST_METRIC(storage_migrate, total, storage_migrate_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(delete, total, delete_requests_total);
    REGISTER_ENGINE_REQUEST_METRIC(delete, failed, delete_requests_failed);
    REGISTER_ENGINE_REQUEST_METRIC(base_compaction, total, base_compaction_request_total);
    REGISTER_ENGINE_REQUEST_METRIC(base_compaction, failed, base_compaction_request_failed);
    REGISTER_ENGINE_REQUEST_METRIC(cumulative_compaction, total, cumulative_compaction_request_total);
    REGISTER_ENGINE_REQUEST_METRIC(cumulative_compaction, failed, cumulative_compaction_request_failed);
    REGISTER_ENGINE_REQUEST_METRIC(update_compaction, total, update_compaction_request_total);
    REGISTER_ENGINE_REQUEST_METRIC(update_compaction, failed, update_compaction_request_failed);

    registry->register_metric("compaction_deltas_total", MetricLabels().add("type", "base"),
                              &base_compaction_deltas_total);
    registry->register_metric("compaction_deltas_total", MetricLabels().add("type", "cumulative"),
                              &cumulative_compaction_deltas_total);
    registry->register_metric("compaction_bytes_total", MetricLabels().add("type", "base"),
                              &base_compaction_bytes_total);
    registry->register_metric("compaction_bytes_total", MetricLabels().add("type", "cumulative"),
                              &cumulative_compaction_bytes_total);
    registry->register_metric("compaction_deltas_total", MetricLabels().add("type", "update"),
                              &update_compaction_deltas_total);
    registry->register_metric("compaction_bytes_total", MetricLabels().add("type", "update"),
                              &update_compaction_bytes_total);
    registry->register_metric("update_compaction_outputs_total", MetricLabels().add("type", "update"),
                              &update_compaction_outputs_total);
    registry->register_metric("update_compaction_outputs_bytes_total", MetricLabels().add("type", "update"),
                              &update_compaction_outputs_bytes_total);
    registry->register_metric("update_compaction_duration_us", MetricLabels().add("type", "update"),
                              &update_compaction_duration_us);

    REGISTER_STORAGE_METRIC(async_delta_writer_execute_total);
    REGISTER_STORAGE_METRIC(async_delta_writer_task_total);
    REGISTER_STORAGE_METRIC(async_delta_writer_task_execute_duration_us);
    REGISTER_STORAGE_METRIC(async_delta_writer_task_pending_duration_us);

    REGISTER_STORAGE_METRIC(load_spill_local_blocks_read_total);
    REGISTER_STORAGE_METRIC(load_spill_local_blocks_write_total);
    REGISTER_STORAGE_METRIC(load_spill_remote_blocks_read_total);
    REGISTER_STORAGE_METRIC(load_spill_remote_blocks_write_total);
    REGISTER_STORAGE_METRIC(load_spill_local_bytes_read_total);
    REGISTER_STORAGE_METRIC(load_spill_local_bytes_write_total);
    REGISTER_STORAGE_METRIC(load_spill_remote_bytes_read_total);
    REGISTER_STORAGE_METRIC(load_spill_remote_bytes_write_total);

    REGISTER_STORAGE_METRIC(delta_writer_commit_task_total);
    REGISTER_STORAGE_METRIC(delta_writer_wait_flush_task_total);
    REGISTER_STORAGE_METRIC(delta_writer_wait_flush_duration_us);
    REGISTER_STORAGE_METRIC(delta_writer_pk_preload_duration_us);
    REGISTER_STORAGE_METRIC(delta_writer_wait_replica_duration_us);
    REGISTER_STORAGE_METRIC(delta_writer_txn_commit_duration_us);

    REGISTER_STORAGE_METRIC(memtable_flush_total);
    REGISTER_STORAGE_METRIC(memtable_finalize_task_total);
    REGISTER_STORAGE_METRIC(memtable_finalize_duration_us);
    REGISTER_STORAGE_METRIC(memtable_flush_duration_us);
    REGISTER_STORAGE_METRIC(memtable_flush_io_time_us);
    REGISTER_STORAGE_METRIC(memtable_flush_memory_bytes_total);
    REGISTER_STORAGE_METRIC(memtable_flush_disk_bytes_total);
    REGISTER_STORAGE_METRIC(segment_flush_total);
    REGISTER_STORAGE_METRIC(segment_flush_duration_us);
    REGISTER_STORAGE_METRIC(segment_flush_io_time_us);
    REGISTER_STORAGE_METRIC(segment_flush_bytes_total);

    REGISTER_STORAGE_METRIC(update_rowset_commit_request_total);
    REGISTER_STORAGE_METRIC(update_rowset_commit_request_failed);
    REGISTER_STORAGE_METRIC(update_rowset_commit_apply_total);
    REGISTER_STORAGE_METRIC(update_rowset_commit_apply_duration_us);

    REGISTER_STORAGE_METRIC(tablet_cumulative_max_compaction_score);
    REGISTER_STORAGE_METRIC(tablet_base_max_compaction_score);
    REGISTER_STORAGE_METRIC(tablet_update_max_compaction_score);
    REGISTER_STORAGE_METRIC(max_tablet_rowset_num);
    REGISTER_STORAGE_METRIC(wait_cumulative_compaction_task_num);
    REGISTER_STORAGE_METRIC(wait_base_compaction_task_num);
    REGISTER_STORAGE_METRIC(running_cumulative_compaction_task_num);
    REGISTER_STORAGE_METRIC(running_base_compaction_task_num);
    REGISTER_STORAGE_METRIC(running_update_compaction_task_num);
    REGISTER_STORAGE_METRIC(cumulative_compaction_task_cost_time_ms);
    REGISTER_STORAGE_METRIC(base_compaction_task_cost_time_ms);
    REGISTER_STORAGE_METRIC(update_compaction_task_cost_time_ns);
    REGISTER_STORAGE_METRIC(base_compaction_task_byte_per_second);
    REGISTER_STORAGE_METRIC(cumulative_compaction_task_byte_per_second);
    REGISTER_STORAGE_METRIC(update_compaction_task_byte_per_second);
    REGISTER_STORAGE_METRIC(push_request_write_bytes_per_second);

    for (const auto& pending : _pending_thread_pool_metrics) {
        _register_thread_pool_metrics(pending.name, pending.threadpool);
    }
    _pending_thread_pool_metrics.clear();

    for (auto& pending : _pending_int_gauge_hooks) {
        _register_int_gauge_hook(pending.name, pending.metric, std::move(pending.value_fn));
    }
    _pending_int_gauge_hooks.clear();

    for (auto& pending : _pending_uint_gauge_hooks) {
        _register_uint_gauge_hook(pending.name, pending.metric, std::move(pending.value_fn));
    }
    _pending_uint_gauge_hooks.clear();

#undef REGISTER_ENGINE_REQUEST_METRIC
#undef REGISTER_STORAGE_METRIC
}

void StorageMetrics::register_thread_pool_metrics(const std::string& name, ThreadPool* threadpool) {
    DCHECK(threadpool != nullptr);
    if (_registry == nullptr) {
        _pending_thread_pool_metrics.emplace_back(PendingThreadPoolMetrics{name, threadpool});
        return;
    }
    _register_thread_pool_metrics(name, threadpool);
}

void StorageMetrics::register_metadata_cache_bytes_total_hook(std::function<int64_t()> value_fn) {
    if (_registry == nullptr) {
        _pending_int_gauge_hooks.emplace_back(
                PendingIntGaugeHook{"metadata_cache_bytes_total", &metadata_cache_bytes_total, std::move(value_fn)});
        return;
    }
    _register_int_gauge_hook("metadata_cache_bytes_total", &metadata_cache_bytes_total, std::move(value_fn));
}

void StorageMetrics::register_unused_rowsets_count_hook(std::function<uint64_t()> value_fn) {
    if (_registry == nullptr) {
        _pending_uint_gauge_hooks.emplace_back(
                PendingUIntGaugeHook{"unused_rowsets_count", &unused_rowsets_count, std::move(value_fn)});
        return;
    }
    _register_uint_gauge_hook("unused_rowsets_count", &unused_rowsets_count, std::move(value_fn));
}

void StorageMetrics::register_rowset_count_generated_and_in_use_hook(std::function<uint64_t()> value_fn) {
    if (_registry == nullptr) {
        _pending_uint_gauge_hooks.emplace_back(PendingUIntGaugeHook{
                "rowset_count_generated_and_in_use", &rowset_count_generated_and_in_use, std::move(value_fn)});
        return;
    }
    _register_uint_gauge_hook("rowset_count_generated_and_in_use", &rowset_count_generated_and_in_use,
                              std::move(value_fn));
}

void StorageMetrics::_register_thread_pool_metrics(const std::string& name, ThreadPool* threadpool) {
    DCHECK(_registry != nullptr);
    DCHECK(threadpool != nullptr);

#define REGISTER_STORAGE_THREAD_POOL_METRICS(threadpool_name)                                                       \
    if (name == #threadpool_name) {                                                                                 \
        register_thread_pool_metric_group(_registry, name, threadpool, &threadpool_name##_threadpool_size,          \
                                          &threadpool_name##_executed_tasks_total,                                  \
                                          &threadpool_name##_pending_time_ns_total,                                 \
                                          &threadpool_name##_execute_time_ns_total, &threadpool_name##_queue_count, \
                                          &threadpool_name##_running_threads, &threadpool_name##_active_threads);   \
        return;                                                                                                     \
    }

    REGISTER_STORAGE_THREAD_POOL_METRICS(async_delta_writer);
    REGISTER_STORAGE_THREAD_POOL_METRICS(load_spill_block_merge);
    REGISTER_STORAGE_THREAD_POOL_METRICS(memtable_flush);
    REGISTER_STORAGE_THREAD_POOL_METRICS(lake_memtable_flush);
    REGISTER_STORAGE_THREAD_POOL_METRICS(segment_replicate);
    REGISTER_STORAGE_THREAD_POOL_METRICS(segment_flush);
    REGISTER_STORAGE_THREAD_POOL_METRICS(update_apply);
    REGISTER_STORAGE_THREAD_POOL_METRICS(pk_index_compaction);
    REGISTER_STORAGE_THREAD_POOL_METRICS(compact_pool);
    REGISTER_STORAGE_THREAD_POOL_METRICS(pindex_load);
    REGISTER_STORAGE_THREAD_POOL_METRICS(cloud_native_pk_index_compact);
    REGISTER_STORAGE_THREAD_POOL_METRICS(tablet_internal_parallel_merge);

#undef REGISTER_STORAGE_THREAD_POOL_METRICS

    DCHECK(false) << "unknown storage thread pool metric group: " << name;
}

void StorageMetrics::_register_int_gauge_hook(const std::string& name, IntGauge* metric,
                                              std::function<int64_t()> value_fn) {
    DCHECK(_registry != nullptr);
    DCHECK(metric != nullptr);
    _registry->register_metric(name, metric);
    _registry->register_hook(name, [metric, value_fn = std::move(value_fn)] { metric->set_value(value_fn()); });
}

void StorageMetrics::_register_uint_gauge_hook(const std::string& name, UIntGauge* metric,
                                               std::function<uint64_t()> value_fn) {
    DCHECK(_registry != nullptr);
    DCHECK(metric != nullptr);
    _registry->register_metric(name, metric);
    _registry->register_hook(name, [metric, value_fn = std::move(value_fn)] { metric->set_value(value_fn()); });
}

} // namespace starrocks
