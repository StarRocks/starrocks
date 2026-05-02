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

#include "base/metrics.h"

namespace starrocks {

class StorageMetrics {
public:
    StorageMetrics() = default;
    explicit StorageMetrics(MetricRegistry* registry) { install(registry); }
    ~StorageMetrics() = default;

    static StorageMetrics* instance();

    void install(MetricRegistry* registry);

    METRIC_DEFINE_INT_COUNTER(push_requests_success_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(push_requests_fail_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(push_request_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(push_request_write_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(push_request_write_rows, MetricUnit::ROWS);
    METRIC_DEFINE_INT_GAUGE(push_request_write_bytes_per_second, MetricUnit::BYTES);

    METRIC_DEFINE_INT_COUNTER(storage_migrate_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(delete_requests_total, MetricUnit::REQUESTS);
    METRIC_DEFINE_INT_COUNTER(delete_requests_failed, MetricUnit::REQUESTS);

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

    METRIC_DEFINE_INT_COUNTER(load_spill_local_blocks_read_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(load_spill_local_blocks_write_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(load_spill_remote_blocks_read_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(load_spill_remote_blocks_write_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(load_spill_local_bytes_read_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(load_spill_local_bytes_write_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(load_spill_remote_bytes_read_total, MetricUnit::BYTES);
    METRIC_DEFINE_INT_COUNTER(load_spill_remote_bytes_write_total, MetricUnit::BYTES);

    METRIC_DEFINE_INT_COUNTER(delta_writer_commit_task_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(delta_writer_wait_flush_task_total, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_COUNTER(delta_writer_wait_flush_duration_us, MetricUnit::MICROSECONDS);
    METRIC_DEFINE_INT_COUNTER(delta_writer_pk_preload_duration_us, MetricUnit::MICROSECONDS);
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

private:
    MetricRegistry* _registry = nullptr;
};

} // namespace starrocks
