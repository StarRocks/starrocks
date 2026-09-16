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

#include <butil/containers/linked_list.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

#include "common/status.h"
#include "gen_cpp/lake_service.pb.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage_primitive/olap_tuple.h"

namespace starrocks {
struct OlapReaderStatistics;
struct OlapWriterStatistics;
} // namespace starrocks

namespace starrocks::lake {

class CompactionTaskCallback;
class Progress {
public:
    int value() const { return _value.load(std::memory_order_acquire); }

    void update(int value) { _value.store(value, std::memory_order_release); }

private:
    std::atomic<int> _value{0};
};

struct CompactionTaskStats {
    // High-level wall-clock phases. These fields are mutually exclusive, except
    // for the explicitly documented nested reader and vertical-group metrics
    // below, so task_total_ns can be reconciled with task_accounted_ns and
    // task_unaccounted_ns in the serialized profile.
    std::string compaction_type = "unknown";
    int64_t task_attempt_count = 0;
    int64_t queue_wait_ns = 0;
    int64_t task_prepare_ns = 0;
    int64_t task_execute_ns = 0;
    int64_t task_total_ns = 0;
    int64_t input_prepare_ns = 0;
    int64_t reader_prepare_ns = 0;
    int64_t reader_open_ns = 0;
    int64_t reader_get_next_ns = 0;
    int64_t reader_close_ns = 0;
    int64_t chunk_transform_ns = 0;
    int64_t writer_create_ns = 0;
    int64_t writer_open_ns = 0;
    int64_t writer_write_ns = 0;
    int64_t writer_flush_ns = 0;
    int64_t writer_finish_ns = 0;
    int64_t writer_close_ns = 0;
    int64_t mask_io_ns = 0;
    int64_t txn_log_build_ns = 0;
    int64_t txn_log_write_ns = 0;
    int64_t preload_compaction_state_ns = 0;
    int64_t tablet_write_log_ns = 0;

    // Nested reader breakdown. These counters explain work inside reader open
    // and get_next and must not be added to the high-level wall-clock phases.
    int64_t create_segment_iter_ns = 0;
    int64_t decompress_ns = 0;
    int64_t block_load_ns = 0;
    int64_t block_fetch_ns = 0;
    int64_t block_seek_ns = 0;
    int64_t block_seek_count = 0;
    int64_t decode_dict_ns = 0;
    int64_t get_rowsets_ns = 0;
    int64_t get_delvec_ns = 0;
    int64_t get_delta_column_group_ns = 0;
    int64_t del_filter_ns = 0;
    int64_t blocks_load = 0;
    int64_t raw_rows_read = 0;
    int64_t compressed_bytes_read = 0;
    int64_t uncompressed_bytes_read = 0;

    int64_t io_ns_read_remote = 0;
    int64_t io_ns_read_local_disk = 0;
    int64_t io_bytes_read_remote = 0;
    int64_t io_bytes_read_local_disk = 0;
    int64_t segment_init_ns = 0;
    int64_t column_iterator_init_ns = 0;
    int64_t io_count_local_disk = 0;
    int64_t io_count_remote = 0;
    int64_t in_queue_time_sec = 0;
    int64_t input_rowset_count = 0;
    int64_t input_row_count = 0;
    int64_t output_row_count = 0;
    int64_t read_chunk_count = 0;
    int64_t write_chunk_count = 0;
    int64_t column_group_count = 0;
    // Nested wall-clock totals for vertical compaction groups. They overlap
    // with the high-level reader/writer phases and are not part of accounting.
    int64_t vertical_key_group_ns = 0;
    int64_t vertical_value_group_ns = 0;
    int64_t read_segment_count = 0;
    int64_t write_segment_count = 0;
    int64_t write_segment_bytes = 0;
    int64_t io_ns_write_remote = 0;
    int64_t pk_sst_merge_ns = 0;
    int64_t input_file_size = 0;

    void collect(const OlapReaderStatistics& reader_stats);
    void collect(const OlapWriterStatistics& writer_stats);
    CompactionTaskStats operator+(const CompactionTaskStats& that) const;
    CompactionTaskStats operator-(const CompactionTaskStats& that) const;
    int64_t task_accounted_ns() const;
    int64_t task_unaccounted_ns() const;
    bool is_slow(int64_t slow_log_ms) const {
        static constexpr int64_t kNanosPerMillisecond = 1'000'000;
        return task_total_ns / kNanosPerMillisecond >= slow_log_ms;
    }
    std::string to_json_stats(bool profile_final = true) const;

    // Same JSON layout as to_json_stats(), with parallel-subtask metadata fields
    // (subtask_id, input_rowsets, is_parallel_subtask) appended. Used for the
    // PROFILE column of be_cloud_native_compactions so that both the
    // CompactionTaskStats counters and per-subtask metadata are visible. Actual
    // read volume is already reported via read_local_mb / read_remote_mb in the
    // stats fields, so the planned input_bytes is intentionally omitted.
    std::string to_json_stats_with_subtask_metadata(int32_t subtask_id, size_t input_rowsets,
                                                    bool profile_final = true) const;
};

// Context of a single tablet compaction task.
struct CompactionTaskContext : public butil::LinkNode<CompactionTaskContext> {
    // Constructor for normal compaction
    explicit CompactionTaskContext(int64_t txn_id_, int64_t tablet_id_, int64_t version_, bool force_base_compaction_,
                                   bool skip_write_txnlog_, std::shared_ptr<CompactionTaskCallback> cb_,
                                   int64_t table_id_ = 0, int64_t partition_id_ = 0, bool is_unshare_ = false)
            : txn_id(txn_id_),
              tablet_id(tablet_id_),
              version(version_),
              force_base_compaction(force_base_compaction_),
              skip_write_txnlog(skip_write_txnlog_),
              is_unshare(is_unshare_),
              callback(std::move(cb_)),
              table_id(table_id_),
              partition_id(partition_id_) {}

    // Factory method for parallel compaction subtasks (with subtask_id)
    static std::unique_ptr<CompactionTaskContext> create_for_subtask(int64_t txn_id_, int64_t tablet_id_,
                                                                     int64_t version_, bool force_base_compaction_,
                                                                     bool skip_write_txnlog_,
                                                                     std::shared_ptr<CompactionTaskCallback> cb_,
                                                                     int32_t subtask_id_, bool is_unshare_ = false) {
        auto ctx = std::make_unique<CompactionTaskContext>(txn_id_, tablet_id_, version_, force_base_compaction_,
                                                           skip_write_txnlog_, std::move(cb_), 0, 0, is_unshare_);
        ctx->subtask_id = subtask_id_;
        return ctx;
    }

#ifndef NDEBUG
    ~CompactionTaskContext() {
        CHECK(next() == this && previous() == this) << "Must remove CompactionTaskContext from list before destructor";
    }
#endif

    const int64_t txn_id;
    const int64_t tablet_id;
    const int64_t version;
    const bool force_base_compaction;
    const bool skip_write_txnlog;
    const bool is_unshare;
    std::atomic<int64_t> start_time{0};
    std::atomic<int64_t> finish_time{0};
    // Monotonic timestamps for adding the elapsed part of the current attempt
    // to a live profile without mutating the finalized cumulative counters.
    std::atomic<int64_t> task_attempt_start_ns{0};
    std::atomic<int64_t> task_execute_start_ns{0};
    std::atomic<bool> skipped{false};
    std::atomic<int> runs{0};
    Status status;
    Progress progress;
    int64_t enqueue_time_sec{0}; // time point when put into queue
    int64_t enqueue_time_ns{0};  // monotonic time point for precise queue wait accounting
    std::shared_ptr<CompactionTaskCallback> callback;
    std::unique_ptr<CompactionTaskStats> stats = std::make_unique<CompactionTaskStats>();
    std::shared_ptr<TxnLogPB> txn_log;
    int64_t table_id;
    int64_t partition_id;
    int32_t subtask_id = -1; // -1 means not a parallel compaction subtask
    // Number of subtasks in this compaction (1 for normal compaction, >1 for parallel compaction)
    int32_t subtask_count = 1;
    // Snapshot of the parallel-subtask input footprint, copied from SubtaskInfo at execution
    // start so that the PROFILE column of be_cloud_native_compactions can keep reporting it
    // after the subtask leaves running_subtasks.
    int64_t subtask_input_rowsets = 0;
    // Flag to indicate this is a merged context from parallel compaction.
    // When true, cleanup_tablet should be called in remove_states after RPC response is sent.
    bool is_parallel_merged = false;

    // Range split: sort key range for this subtask.
    // When set, the compaction task only reads/writes data within this range.
    bool has_range_split = false;
    std::vector<OlapTuple> range_start_key;
    std::vector<OlapTuple> range_end_key;
    bool range_lower_inclusive = true;
    bool range_upper_inclusive = false;
    // Explicit bound presence flags. When has_lower_bound is false, the lower bound
    // is unbounded (scan from the beginning). When has_upper_bound is false, the upper
    // bound is unbounded (scan to the end). The corresponding OlapTuple remains empty
    // so both sides can still be passed to TabletReader as a paired range.
    bool has_lower_bound = false;
    bool has_upper_bound = false;
    bool is_first_range = false;
    bool is_last_range = false;

    void reset_attempt_stats();
    void publish_stats_snapshot();
    CompactionTaskStats stats_snapshot(bool include_live_timers) const;

private:
    // Running task queries read this published copy instead of racing with the
    // worker's in-place updates to stats. The worker refreshes it only at safe
    // phase boundaries, while the atomic timestamps keep live timers moving.
    mutable std::mutex _stats_snapshot_mutex;
    CompactionTaskStats _published_stats;
    int64_t _published_task_attempt_start_ns = 0;
    int64_t _published_task_execute_start_ns = 0;
};

} // namespace starrocks::lake
