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
#include <mutex>

#include "exec/spill/block_manager.h"
#include "exec/spill/data_stream.h"
#include "exec/spill/spiller_factory.h"
#include "util/runtime_profile.h"

namespace starrocks {

class RuntimeState;
class LoadSpillBlockManager;

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

    // Switch to remote block when local disk is full
    Status _switch_to_remote_block(size_t block_size);

private:
    LoadSpillBlockManager* _block_manager = nullptr;
    spill::BlockPtr _block;
    int64_t _append_bytes = 0;
};

class LoadChunkSpiller {
public:
    LoadChunkSpiller(LoadSpillBlockManager* block_manager, RuntimeProfile* profile);
    ~LoadChunkSpiller() = default;

    StatusOr<size_t> spill(const Chunk& chunk);

    Status merge_write(size_t target_size, bool do_sort, bool do_agg, std::function<Status(Chunk*)> write_func,
                       std::function<Status()> flush_func);

    bool empty();

    std::shared_ptr<spill::Spiller> spiller() { return _spiller; }

    SchemaPtr schema() { return _schema; }

    RuntimeProfile* profile() const { return _profile; }

private:
    Status _prepare(const ChunkPtr& chunk_ptr);

    Status _do_spill(const Chunk& chunk, const spill::SpillOutputDataStreamPtr& output);

private:
    LoadSpillBlockManager* _block_manager = nullptr;
    // destroy spiller before runtime_state
    std::shared_ptr<RuntimeState> _runtime_state;
    // Load spilling uses a dummy RuntimeState without QueryContext.
    std::atomic_int64_t _total_spill_bytes = 0;
    // used when input profile is nullptr
    std::unique_ptr<RuntimeProfile> _dummy_profile;
    RuntimeProfile* _profile = nullptr;
    spill::SpillerFactoryPtr _spiller_factory;
    std::shared_ptr<spill::Spiller> _spiller;
    // Drives the one-time initialization of `_spiller` (and its serde) in _prepare().
    // The load spill path is entered concurrently by multiple memtable-flush threads that
    // share the same LoadChunkSpiller, so std::call_once serializes the first spill: the
    // winning thread runs the init body while the others block until it finishes and then
    // observe the fully-constructed `_spiller`. Readiness is no longer inferred from
    // `_spiller != nullptr`, which used to become visible before the serde's encode context
    // was created and let a racing thread crash in ColumnarSerde::serialize().
    std::once_flag _prepare_once;
    // Result of the one-time initialization, published by the call_once body and read by
    // every caller. A failed init is cached here (call_once will not re-run on a normal
    // return), so all threads see the same error instead of using a half-built spiller.
    Status _prepare_status;
    SchemaPtr _schema;
    // used for spill merge, parent trakcer is compaction tracker
    std::unique_ptr<MemTracker> _merge_mem_tracker = nullptr;
};

} // namespace starrocks
