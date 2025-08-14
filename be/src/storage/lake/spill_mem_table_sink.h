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
