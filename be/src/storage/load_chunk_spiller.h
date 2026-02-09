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
#include "util/runtime_profile.h"

namespace starrocks {

class RuntimeState;
class LoadSpillBlockManager;
class ChunkIterator;
class LoadChunkSpiller;
class LoadSpillPipelineMergeTask;
class LoadSpillPipelineMergeContext;

using ChunkIteratorPtr = std::shared_ptr<ChunkIterator>;
using LoadSpillPipelineMergeTaskPtr = std::unique_ptr<LoadSpillPipelineMergeTask>;

namespace lake {
class TabletWriter;
class TabletInternalParallelMergeTask;
} // namespace lake

namespace spill {
class BlockGroup;
} // namespace spill

// Output stream for spilling data to disk blocks
// Each stream writes to a specific block group, which is tagged with a slot_idx
// to maintain ordering information for parallel flush scenarios.
class LoadSpillOutputDataStream : public spill::SpillOutputDataStream {
public:
    // @param block_group: the target block group to write spilled data into,
    //                     which carries the slot_idx for ordering
    LoadSpillOutputDataStream(LoadSpillBlockManager* block_manager, spill::BlockGroup* block_group)
            : _block_manager(block_manager), _block_group(block_group) {}

    Status append(RuntimeState* state, const std::vector<Slice>& data, size_t total_write_size,
                  size_t write_num_rows) override;

    Status flush() override;

    bool is_remote() const override;

    int64_t append_bytes() const { return _append_bytes; }

private:
    Status _preallocate(size_t block_size);

    // Freeze current block and append it to block container
    Status _freeze_current_block();

    // Switch to remote block when local disk is full
    Status _switch_to_remote_block(size_t block_size);

private:
    LoadSpillBlockManager* _block_manager = nullptr;
    // Target block group for this output stream, tagged with slot_idx for ordering
    spill::BlockGroup* _block_group = nullptr;
    spill::BlockPtr _block;
    int64_t _append_bytes = 0;
};

struct SpillBlockInputTasks {
    std::vector<ChunkIteratorPtr> iterators;
    size_t total_blocks = 0;
    size_t total_block_bytes = 0;
    size_t group_count = 0;
};

class LoadChunkSpiller {
public:
    friend class LoadSpillPipelineMergeIterator;
    LoadChunkSpiller(LoadSpillBlockManager* block_manager, RuntimeProfile* profile,
                     LoadSpillPipelineMergeContext* pipeline_merge_context = nullptr);
    ~LoadChunkSpiller() = default;

    StatusOr<size_t> spill(const Chunk& chunk, int64_t slot_idx = -1);

    // `target_size` controls the maximum amount of data merged per operation,
    // while `memory_usage_per_merge` controls the peak memory usage of each merge.
    Status merge_write(size_t target_size, size_t memory_usage_per_merge, bool do_sort, bool do_agg,
                       std::function<Status(Chunk*)> write_func, std::function<Status()> flush_func);

    StatusOr<SpillBlockInputTasks> generate_spill_block_input_tasks(size_t target_size, size_t memory_usage_per_merge,
                                                                    bool do_sort, bool do_agg);

    StatusOr<LoadSpillPipelineMergeTaskPtr> generate_pipeline_merge_task(size_t target_size,
                                                                         size_t memory_usage_per_merge, bool do_sort,
                                                                         bool do_agg, bool final_round);

    bool empty();

    std::shared_ptr<spill::Spiller> spiller() { return _spiller; }

    SchemaPtr schema() { return _schema; }

    size_t total_bytes() const;

    RuntimeProfile* profile() const { return _profile; }

private:
    Status _prepare(const ChunkPtr& chunk_ptr);

    Status _do_spill(const Chunk& chunk, const spill::SpillOutputDataStreamPtr& output);

private:
    LoadSpillBlockManager* _block_manager = nullptr;
    RuntimeProfile* _profile = nullptr;
    // destroy spiller before runtime_state
    std::shared_ptr<RuntimeState> _runtime_state;
    // pipeline merge context for managing merge tasks
    LoadSpillPipelineMergeContext* _pipeline_merge_context = nullptr;
    // used when input profile is nullptr
    std::unique_ptr<RuntimeProfile> _dummy_profile;
    spill::SpillerFactoryPtr _spiller_factory;
    std::shared_ptr<spill::Spiller> _spiller;
    SchemaPtr _schema;
};

} // namespace starrocks
