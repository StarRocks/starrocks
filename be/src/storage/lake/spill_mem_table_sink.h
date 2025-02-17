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

#include "exec/spill/block_manager.h"
#include "exec/spill/data_stream.h"
#include "exec/spill/spiller_factory.h"
#include "storage/memtable_sink.h"
#include "util/runtime_profile.h"

namespace starrocks {

class RuntimeState;

namespace lake {

class LoadSpillBlockManager;
class TabletWriter;

class LoadSpillOutputDataStream : public spill::SpillOutputDataStream {
public:
    LoadSpillOutputDataStream(LoadSpillBlockManager* block_manager) : _block_manager(block_manager) {}

    Status append(RuntimeState* state, const std::vector<Slice>& data, size_t total_write_size,
                  size_t write_num_rows) override;

    Status flush() override;

    bool is_remote() const override;

    int64_t append_bytes() const { return _append_bytes; }

private:
    Status _preallocate(size_t block_size);

    // Freeze current block and append it to block container
    Status _freeze_current_block();

private:
    LoadSpillBlockManager* _block_manager = nullptr;
    spill::BlockPtr _block;
    int64_t _append_bytes = 0;
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

    spill::Spiller* get_spiller() { return _spiller.get(); }

private:
    Status _prepare(const ChunkPtr& chunk_ptr);
    Status _do_spill(const Chunk& chunk, const spill::SpillOutputDataStreamPtr& output);

private:
    LoadSpillBlockManager* _block_manager = nullptr;
    TabletWriter* _writer;
    // destroy spiller before runtime_state
    std::shared_ptr<RuntimeState> _runtime_state;
    RuntimeProfile* _profile = nullptr;
    spill::SpillerFactoryPtr _spiller_factory;
    std::shared_ptr<spill::Spiller> _spiller;
    SchemaPtr _schema;
    // used for spill merge, parent trakcer is compaction tracker
    std::unique_ptr<MemTracker> _merge_mem_tracker = nullptr;
};

} // namespace lake
} // namespace starrocks