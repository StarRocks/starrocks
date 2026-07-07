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

#include "common/runtime_profile.h"
#include "compute_env/load_spill/load_chunk_spiller.h"
#include "runtime/mem_tracker.h"
#include "storage/lake/load_spill_pipeline_merge_context.h"
#include "storage/memtable_sink.h"

namespace starrocks {

class RuntimeState;
class LoadSpillBlockManager;
class ThreadPoolToken;
namespace lake {

class TabletWriter;

// Sink for writing memtable data with spilling support
// When spilling is enabled, chunks are written to temporary blocks first, then merged to segments.
// The slot_idx parameter is used to track the original flush order for correct merging.
class SpillMemTableSink : public MemTableSink {
public:
    SpillMemTableSink(LoadSpillBlockManager* block_manager, TabletWriter* writer, RuntimeProfile* profile);
    ~SpillMemTableSink() override;

    // Spill chunk to temporary storage or write directly if eos and no prior spills
    // @param slot_idx: slot index for tracking flush order in parallel flush mode
    Status flush_chunk(const Chunk& chunk, starrocks::SegmentPB* segment = nullptr, bool eos = false,
                       int64_t* flush_data_size = nullptr, int64_t slot_idx = -1) override;

    // Spill chunk with deletes to temporary storage
    // @param slot_idx: slot index for tracking flush order in parallel flush mode
    Status flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes,
                                    starrocks::SegmentPB* segment = nullptr, bool eos = false,
                                    int64_t* flush_data_size = nullptr, int64_t slot_idx = -1) override;

    // The spill sink keeps the __op column so the merge resolves upsert/delete order by slot.
    // Keep the __op column (drive the op-aware merge) only when the in-transaction upsert/delete order
    // feature is enabled. When it is off (the default), return false so the memtable takes the legacy
    // _split_upserts_deletes path (deletes split into a del file, applied after all segments) -- keeping
    // the spill path's behavior, del-file layout, and delete-with-merge-condition rejection identical to
    // before this feature. Gated by config::lake_enable_pk_preserve_txn_delete_order.
    bool keep_op_column() const override;

    // Spill a chunk that still carries the trailing __op column (see keep_op_column()).
    Status flush_chunk_with_op(const Chunk& chunk_with_op, starrocks::SegmentPB* segment = nullptr, bool eos = false,
                               int64_t* flush_data_size = nullptr, int64_t slot_idx = -1) override;

    Status merge_blocks_to_segments();

    spill::Spiller* get_spiller() { return _load_chunk_spiller->spiller().get(); }

    int64_t txn_id() override;
    int64_t tablet_id() override;

private:
    TabletWriter* _writer;

    // Memory tracker for merge operations, parent is compaction tracker.
    // RATIONALE: Merge phase uses separate memory budget from normal load operations
    // to prevent OOM. Parent is compaction tracker since merge is similar to compaction.
    std::unique_ptr<MemTracker> _merge_mem_tracker = nullptr;

    // Coordinates parallel merge tasks and consolidates their results.
    // Collects tasks from both eager merge (in flush_chunk) and final merge phases,
    // ensuring all task results are properly merged into the tablet writer.
    std::unique_ptr<LoadSpillPipelineMergeContext> _pipeline_merge_context = nullptr;

    // Manages spilling chunks to disk and provides merge capabilities
    std::unique_ptr<LoadChunkSpiller> _load_chunk_spiller = nullptr;
};

} // namespace lake
} // namespace starrocks
