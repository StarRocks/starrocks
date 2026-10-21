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
    // Floor for the per-stream read buffer chosen by stream_buffer_size(). Below this a remote read
    // spends more on request overhead than it saves in memory.
    static constexpr int64_t kMinStreamBufferSize = 64 * 1024;

    // Read buffer for each of the `num_streams` (input source, column) streams one read pass keeps
    // open at once. Every stream gets its own buffer, so with the configured
    // lake_compaction_stream_buffer_size_bytes a backlog of hundreds of input segments alone costs
    // gigabytes -- far past compaction_memory_limit_per_worker, which only sizes the read chunk. The
    // buffers are shrunk to share half of that budget, but never below kMinStreamBufferSize and never
    // above the configured size. A non-positive budget or configured size is returned unchanged.
    static int64_t stream_buffer_size(int64_t num_streams);

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

protected:
    // stream_buffer_size() for a pass over `num_streams` streams; also counts in the task stats the
    // passes that had to shrink below the configured size.
    int64_t stream_buffer_size_for_pass(int64_t num_streams);

    // Memory left for the read chunk once a pass's read buffers (`stream_buffer_bytes`, i.e. streams *
    // stream buffer size) are taken off compaction_memory_limit_per_worker. The buffers are allocated
    // whatever the chunk size is, so they come off the budget first; stream_buffer_size() keeps them
    // within half of it unless its floor kicks in, and even then the chunk keeps a quarter, so a huge
    // fan-in degrades to small chunks rather than one-row reads. A non-positive limit is returned unchanged.
    static int64_t chunk_mem_limit(int64_t stream_buffer_bytes);

    int64_t _txn_id;
    VersionedTablet _tablet;
    std::vector<std::shared_ptr<Rowset>> _input_rowsets;
    // Read buffer for every stream of the current read pass, chosen by stream_buffer_size_for_pass()
    // when the pass sizes its chunk, so the chunk sizing and the reader it feeds agree.
    int64_t _stream_buffer_size = 0;
    std::unique_ptr<MemTracker> _mem_tracker = nullptr;
    CompactionTaskContext* _context;
    std::shared_ptr<const TabletSchema> _tablet_schema;
    // for flat json used
    std::vector<std::unique_ptr<ColumnAccessPath>> _column_access_paths;
};

} // namespace starrocks::lake
