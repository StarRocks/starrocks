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
#include <memory>
#include <mutex>

#include "common/status.h"
#include "exec/spill/block_manager.h"
#include "exec/spill/data_stream.h"
#include "exec/spill/spiller_factory.h"
#include "storage/chunk_iterator.h"
#include "storage/memtable_sink.h"
#include "util/runtime_profile.h"

namespace starrocks {

class RuntimeState;

namespace lake {

class LoadSpillBlockManager;
class TabletWriter;

class SpillMergeCancellation {
public:
    void cancel(const Status& status);

    bool is_cancelled() const { return _cancelled.load(std::memory_order_acquire); }

    Status status() const;

private:
    std::atomic<bool> _cancelled{false};
    mutable std::mutex _mutex;
    Status _status;
};

struct SpillBlockInputTasks {
    std::vector<ChunkIteratorPtr> iterators;
    size_t total_blocks = 0;
    size_t total_block_bytes = 0;
    size_t group_count = 0;
};

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

class SpillMemTableSink : public MemTableSink {
public:
    SpillMemTableSink(LoadSpillBlockManager* block_manager, TabletWriter* writer, RuntimeProfile* profile,
                      std::shared_ptr<SpillMergeCancellation> cancellation = nullptr);
    ~SpillMemTableSink() override = default;

    Status flush_chunk(const Chunk& chunk, starrocks::SegmentPB* segment = nullptr, bool eos = false,
                       int64_t* flush_data_size = nullptr) override;

    Status flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes,
                                    starrocks::SegmentPB* segment = nullptr, bool eos = false,
                                    int64_t* flush_data_size = nullptr) override;

    Status merge_blocks_to_segments();

    void cancel(const Status& status) { _cancellation->cancel(status); }

    // Parallel merge spill blocks to segments when config enable_load_spill_parallel_merge is true
    Status merge_blocks_to_segments_parallel(bool do_agg);
    Status merge_blocks_to_segments_serial(bool do_agg);

    spill::Spiller* get_spiller() { return _spiller.get(); }

    RuntimeProfile* profile() const { return _profile; }

private:
    StatusOr<SpillBlockInputTasks> generate_spill_block_input_tasks(size_t target_size, size_t memory_usage_per_merge,
                                                                    bool do_sort, bool do_agg);
    Status _prepare(const ChunkPtr& chunk_ptr);
    Status _do_spill(const Chunk& chunk, const spill::SpillOutputDataStreamPtr& output);

private:
    LoadSpillBlockManager* _block_manager = nullptr;
    TabletWriter* _writer;
    // destroy spiller before runtime_state
    std::shared_ptr<RuntimeState> _runtime_state;
    // used when input profile is nullptr
    std::unique_ptr<RuntimeProfile> _dummy_profile;
    RuntimeProfile* _profile = nullptr;
    spill::SpillerFactoryPtr _spiller_factory;
    std::shared_ptr<spill::Spiller> _spiller;
    SchemaPtr _schema;
    // used for spill merge, parent trakcer is compaction tracker
    std::unique_ptr<MemTracker> _merge_mem_tracker = nullptr;
    std::shared_ptr<SpillMergeCancellation> _cancellation;
};

} // namespace lake
} // namespace starrocks
