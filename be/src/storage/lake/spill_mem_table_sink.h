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

#include "storage/load_chunk_spiller.h"
#include "storage/memtable_sink.h"
#include "util/runtime_profile.h"

namespace starrocks {

class RuntimeState;
class LoadSpillBlockManager;
class ThreadPoolToken;
class LoadSpillPipelineMergeContext;

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
