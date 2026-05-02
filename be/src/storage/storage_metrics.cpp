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

#include "gutil/macros.h"

namespace starrocks {

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

#undef REGISTER_ENGINE_REQUEST_METRIC
#undef REGISTER_STORAGE_METRIC
}

} // namespace starrocks
