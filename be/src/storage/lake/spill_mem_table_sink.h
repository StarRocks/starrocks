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

namespace lake {

class TabletWriter;

class TabletInternalParallelMergeTask : public Runnable {
public:
    TabletInternalParallelMergeTask(TabletWriter* writer, ChunkIterator* block_iterator, MemTracker* merge_mem_tracker,
                                    Schema* schema, int32_t task_index)
            : _writer(writer),
              _block_iterator(block_iterator),
              _merge_mem_tracker(merge_mem_tracker),
              _schema(schema),
              _task_index(task_index) {}
    ~TabletInternalParallelMergeTask() override = default;

    void run() override;

    void update_status(const Status& st);

    const Status& status() const { return _status; }

private:
    TabletWriter* _writer = nullptr;
    ChunkIterator* _block_iterator = nullptr;
    MemTracker* _merge_mem_tracker = nullptr;
    Schema* _schema = nullptr;
    int32_t _task_index = 0;
    Status _status;
};

class SpillMemTableSink : public MemTableSink {
public:
    SpillMemTableSink(LoadSpillBlockManager* block_manager, TabletWriter* writer, RuntimeProfile* profile);
    ~SpillMemTableSink() override = default;

    Status flush_chunk(const Chunk& chunk, starrocks::SegmentPB* segment = nullptr, bool eos = false,
                       int64_t* flush_data_size = nullptr) override;

    Status flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes,
                                    starrocks::SegmentPB* segment = nullptr, bool eos = false,
                                    int64_t* flush_data_size = nullptr) override;

    Status merge_blocks_to_segments();

    spill::Spiller* get_spiller() { return _load_chunk_spiller->spiller().get(); }

    int64_t txn_id() override;
    int64_t tablet_id() override;

private:
    TabletWriter* _writer;
    // used for spill merge, parent trakcer is compaction tracker
    std::unique_ptr<MemTracker> _merge_mem_tracker = nullptr;
    std::unique_ptr<LoadChunkSpiller> _load_chunk_spiller = nullptr;
};

} // namespace lake
} // namespace starrocks
