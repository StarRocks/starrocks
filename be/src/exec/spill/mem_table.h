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
#include "exprs/expr_context.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks::spill {
using FlushCallBack = std::function<Status(const ChunkPtr&)>;
class SpillInputStream;
class Spiller;

//  This component is the intermediate buffer for our spill data, which may be ordered or unordered,
// depending on the requirements of the upper layer

// usage:
//
// auto mem_table = create();
// while (!mem_table->is_full()) {
//     mem_table->append(next_chunk());
// }
// mem_table->done();
// mem_table->flush();

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
    // all of data has been added
    // done will be called in pipeline executor threads
    virtual Status done() = 0;
    // flush all data to callback, then release the memory in memory table
    // flush will be called in IO threads
    // flush needs to be designed to be reentrant. Because callbacks can return Status::Yield.
    virtual Status flush(FlushCallBack callback) = 0;

    virtual StatusOr<std::shared_ptr<SpillInputStream>> as_input_stream(bool shared) {
        return Status::NotSupported("unsupport to call as_input_stream");
    }

protected:
    RuntimeState* _runtime_state;
    const size_t _max_buffer_size;
    std::unique_ptr<MemTracker> _tracker;
    Spiller* _spiller = nullptr;
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
    Status done() override { return Status::OK(); };
    Status flush(FlushCallBack callback) override;

    StatusOr<std::shared_ptr<SpillInputStream>> as_input_stream(bool shared) override;

private:
    size_t _processed_index{};
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
    Status flush(FlushCallBack callback) override;

private:
    StatusOr<ChunkPtr> _do_sort(const ChunkPtr& chunk);

    const std::vector<ExprContext*>* _sort_exprs;
    const SortDescs _sort_desc;
    Permutation _permutation;
    ChunkPtr _chunk;
    ChunkSharedSlice _chunk_slice;
};
} // namespace starrocks::spill