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

#include <functional>
#include <memory>
#include <utility>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "exec/spill/block_manager.h"
#include "exec/workgroup//scan_task_queue.h"
#include "exprs/expr_context.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks::spill {
using FlushCallBack = std::function<Status(const ChunkPtr&)>;
class SpillInputStream;
class Spiller;
class MemoryBlock;
//  This component is the intermediate buffer for our spill data, which may be ordered or unordered,
// depending on the requirements of the upper layer

// usage:
//
// auto mem_table = create();
// while (!mem_table->is_full()) {
//     mem_table->append(next_chunk());
// }
// mem_table->finalize();
// auto serialized_data = mem_table->get_serialized_data();
// ...

class SpillableMemTable {
public:
    SpillableMemTable(RuntimeState* state, size_t max_buffer_size, MemTracker* parent, Spiller* spiller)
            : _runtime_state(state), _max_buffer_size(max_buffer_size), _spiller(spiller) {
        _tracker = std::make_unique<MemTracker>(-1, "spill-mem-table", parent);
    }
    virtual ~SpillableMemTable() {
        if (auto parent = _tracker->parent()) {
            parent->release(_tracker->consumption());
        }
    }
    bool is_full() const { return _tracker->consumption() >= _max_buffer_size; };
    virtual bool is_empty() = 0;
    size_t mem_usage() { return _tracker->consumption(); }
    // append data to mem table
    [[nodiscard]] virtual Status append(ChunkPtr chunk) = 0;
    [[nodiscard]] virtual Status append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from,
                                                  uint32_t size) = 0;

    // @TODO(silverbullet233)
    // Theoretically, the implementation of done and finalize interfaces only have calculation logic, and there is no need to do them separately.
    // However, there are some limitations at this stage:
    // 1. The pipeline scheduling mechanism does not support reentrancy. It is difficult to implement the yield mechanism in the computing thread. we can only rely on the yield mechanism of the scan task.
    // 2. Some long-time operations do not support yield (such as sorting). If they are placed in the io thread, they may have a greater impact on other tasks.
    // In order to make a compromise, we can only adopt this approach now, and we can consider unifying it after our yield mechanism is perfected.

    // call `done` to do something after all data has been added
    // this function is called in the pipeline execution thread
    virtual Status done() {
        _is_done = true;
        return Status::OK();
    }
    // call `finalize` to serialize data after `done` is called
    // after `finalize` is successfully called, we can get serialized data by `get_next_serialized_data`.
    // NOTE: The implementation of `finalize` needs to ensure reentrancy in order to implement io task yield mechanism
    virtual Status finalize(workgroup::YieldContext& yield_ctx) = 0;

    virtual StatusOr<std::shared_ptr<SpillInputStream>> as_input_stream(bool shared) {
        return Status::NotSupported("unsupport to call as_input_stream");
    }

    StatusOr<Slice> get_next_serialized_data();
    size_t get_serialized_data_size() const;
    virtual void reset() = 0;

    size_t num_rows() const { return _num_rows; }

protected:
    using MemoryBlockPtr = std::shared_ptr<MemoryBlock>;
    RuntimeState* _runtime_state;
    const size_t _max_buffer_size;
    std::unique_ptr<MemTracker> _tracker;
    Spiller* _spiller = nullptr;
    size_t _num_rows = 0;
    MemoryBlockPtr _block;
    bool _is_done = false;
};

using MemTablePtr = std::shared_ptr<SpillableMemTable>;

class UnorderedMemTable final : public SpillableMemTable {
public:
    template <class... Args>
    UnorderedMemTable(Args&&... args) : SpillableMemTable(std::forward<Args>(args)...) {}
    ~UnorderedMemTable() override = default;

    bool is_empty() override;
    [[nodiscard]] Status append(ChunkPtr chunk) override;
    [[nodiscard]] Status append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from,
                                          uint32_t size) override;

    Status finalize(workgroup::YieldContext& yield_ctx) override;
    void reset() override;

    StatusOr<std::shared_ptr<SpillInputStream>> as_input_stream(bool shared) override;

private:
    size_t _processed_index = 0;
    std::vector<ChunkPtr> _chunks;
};

class OrderedMemTable final : public SpillableMemTable {
public:
    template <class... Args>
    OrderedMemTable(const std::vector<ExprContext*>* sort_exprs, const SortDescs* sort_desc, Args&&... args)
            : SpillableMemTable(std::forward<Args>(args)...), _sort_exprs(sort_exprs), _sort_desc(*sort_desc) {}
    ~OrderedMemTable() override = default;

    bool is_empty() override;
    [[nodiscard]] Status append(ChunkPtr chunk) override;
    [[nodiscard]] Status append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from,
                                          uint32_t size) override;

    Status done() override;
    Status finalize(workgroup::YieldContext& yield_ctx) override;
    void reset() override;

private:
    StatusOr<ChunkPtr> _do_sort(const ChunkPtr& chunk);

    const std::vector<ExprContext*>* _sort_exprs;
    const SortDescs _sort_desc;
    Permutation _permutation;
    ChunkPtr _chunk;
    ChunkSharedSlice _chunk_slice;
};
} // namespace starrocks::spill