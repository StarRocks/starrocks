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

#include <atomic>
#include <functional>
#include <ostream>

#include "column/column_access_path.h"
#include "common/status.h"
#include "compaction_task_context.h"
#include "fs/fs.h"
#include "runtime/mem_tracker.h"
#include "storage/lake/versioned_tablet.h"

namespace starrocks {
class TxnLogPB;
class TxnLogPB_OpCompaction;
} // namespace starrocks

namespace starrocks::lake {

class Rowset;
class TabletWriter;

class CompactionTask {
public:
    // Holding the input set is worth at most this factor of read-chunk shrink; past it the task
    // stops holding. See chunk_size_with_held_segments().
    static constexpr int32_t kMaxHeldChunkShrink = 8;

    // CancelFunc is a function that used to tell the compaction task whether the task
    // should be cancelled.
    using CancelFunc = std::function<Status()>;

    explicit CompactionTask(VersionedTablet tablet, std::vector<std::shared_ptr<Rowset>> input_rowsets,
                            CompactionTaskContext* context, std::shared_ptr<const TabletSchema> tablet_schema);
    virtual ~CompactionTask() = default;

    virtual Status execute(CancelFunc cancel_func, ThreadPool* flush_pool = nullptr) = 0;

    Status execute_index_major_compaction(TxnLogPB* txn_log);

    inline static const CancelFunc kNoCancelFn = []() { return Status::OK(); };
    inline static const CancelFunc kCancelledFn = []() { return Status::Aborted(""); };

    Status fill_compaction_segment_info(TxnLogPB_OpCompaction* op_compaction, TabletWriter* writer);

    bool should_enable_pk_index_eager_build(int64_t input_bytes);

    // SST stats result from compute_sst_stats
    struct SstStats {
        int32_t input_files = 0;
        int64_t input_bytes = 0;
        int32_t output_files = 0;
        int64_t output_bytes = 0;
    };

    // Compute SST stats from writer's sst list and txn_log's compaction operation.
    // This is a static method for testability.
    static SstStats compute_sst_stats(const std::vector<FileInfo>& writer_ssts, const TxnLogPB* txn_log);

    // Collect SST stats from eager build (writer) and major compaction (txn_log),
    // to be used when recording tablet write log.
    void collect_sst_stats(const TabletWriter* writer, const TxnLogPB* txn_log);

    int32_t sst_input_files() const { return _sst_input_files; }
    int64_t sst_input_bytes() const { return _sst_input_bytes; }
    int32_t sst_output_files() const { return _sst_output_files; }
    int64_t sst_output_bytes() const { return _sst_output_bytes; }

protected:
    // Read chunk size for this phase, deciding at the same time whether the held input set may stay.
    // The held set is resident for the whole task, so it comes out of the same per-worker budget the
    // read buffers are sized from -- but only up to a point: past it, holding starves the chunk
    // sizing (get_read_chunk_size divides what is left by the per-row footprint, so an exhausted
    // budget yields a one-row chunk and the task crawls) while pinning memory the metadata-cache LRU
    // cannot reclaim. When holding would shrink the chunk that far, this releases the held sets and
    // clears `_hold_input_segments`, so the shared metadata cache carries cross-pass reuse again --
    // the behaviour from before hold_segments existed -- and returns the un-held chunk size.
    int32_t chunk_size_with_held_segments(int64_t held_segments_bytes, int64_t total_num_rows,
                                          int64_t total_mem_footprint, size_t source_num);

    int64_t _txn_id;
    VersionedTablet _tablet;
    std::vector<std::shared_ptr<Rowset>> _input_rowsets;
    // Snapshot of config::lake_compaction_hold_input_segments, taken once at the top of execute().
    // The config is mutable, and every phase of the task must agree on it: a flip mid-task would
    // leave the remaining phases with neither the held segments nor a filled metadata cache.
    // chunk_size_with_held_segments() may clear it -- see there.
    bool _hold_input_segments = false;
    std::unique_ptr<MemTracker> _mem_tracker = nullptr;
    CompactionTaskContext* _context;
    std::shared_ptr<const TabletSchema> _tablet_schema;
    // for flat json used
    std::vector<std::unique_ptr<ColumnAccessPath>> _column_access_paths;

    // PK index SST stats collected from eager build and major compaction
    int32_t _sst_input_files = 0;
    int64_t _sst_input_bytes = 0;
    int32_t _sst_output_files = 0;
    int64_t _sst_output_bytes = 0;
};

} // namespace starrocks::lake
