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

#include "exec/spill/mem_table.h"

#include <glog/logging.h>

#include <memory>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/chunks_sorter.h"
#include "exec/spill/input_stream.h"
#include "runtime/current_thread.h"

namespace starrocks::spill {
class RawChunkInputStream final : public SpillInputStream {
public:
    RawChunkInputStream(std::vector<ChunkPtr> chunks) : _chunks(std::move(chunks)) {}
    StatusOr<ChunkUniquePtr> get_next(SerdeContext& ctx) override;

    bool is_ready() override { return true; };
    void close() override{};

    bool enable_prefetch() const override { return true; }

    Status prefetch(SerdeContext& ctx) override {
        mark_is_eof();
        return Status::EndOfFile("eos");
    }

private:
    size_t read_idx{};
    std::vector<ChunkPtr> _chunks;
};

StatusOr<ChunkUniquePtr> RawChunkInputStream::get_next(SerdeContext& context) {
    if (read_idx >= _chunks.size()) {
        return Status::EndOfFile("eos");
    }
    // TODO: make ChunkPtr could convert to ChunkUniquePtr to avoid unused memory copy
    auto res = std::move(_chunks[read_idx++])->clone_unique();
    _chunks[read_idx - 1].reset();

    return res;
}

bool UnorderedMemTable::is_empty() {
    return _chunks.empty();
}

Status UnorderedMemTable::append(ChunkPtr chunk) {
    _tracker->consume(chunk->memory_usage());
    _chunks.emplace_back(std::move(chunk));
    return Status::OK();
}

Status UnorderedMemTable::append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    if (_chunks.empty() || _chunks.back()->num_rows() + size > _runtime_state->chunk_size()) {
        _chunks.emplace_back(src.clone_empty());
        _tracker->consume(_chunks.back()->memory_usage());
    }

    Chunk* current = _chunks.back().get();
    size_t mem_usage = current->memory_usage();
    current->append_selective(src, indexes, from, size);
    mem_usage = current->memory_usage() - mem_usage;

    _tracker->consume(mem_usage);

    return Status::OK();
}

Status UnorderedMemTable::flush(FlushCallBack callback) {
    for (const auto& chunk : _chunks) {
        RETURN_IF_ERROR(callback(chunk));
    }
    _tracker->release(_tracker->consumption());
    _chunks.clear();
    return Status::OK();
}

StatusOr<std::shared_ptr<SpillInputStream>> UnorderedMemTable::as_input_stream(bool shared) {
    if (shared) {
        return std::make_shared<RawChunkInputStream>(_chunks);
    } else {
        return std::make_shared<RawChunkInputStream>(std::move(_chunks));
    }
}

bool OrderedMemTable::is_empty() {
    return _chunk == nullptr || _chunk->is_empty();
}

Status OrderedMemTable::append(ChunkPtr chunk) {
    if (_chunk == nullptr) {
        _chunk = chunk->clone_empty();
    }
    _chunk->append(*chunk);
    _tracker->set(_chunk->memory_usage());
    return Status::OK();
}

Status OrderedMemTable::append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    if (_chunk == nullptr) {
        _chunk = src.clone_empty();
    }

    Chunk* current = _chunk.get();
    size_t mem_usage = current->memory_usage();
    _chunk->append_selective(src, indexes, from, size);
    mem_usage = current->memory_usage() - mem_usage;

    _tracker->consume(mem_usage);

    return Status::OK();
}

Status OrderedMemTable::flush(FlushCallBack callback) {
    while (!_chunk_slice.empty()) {
        auto chunk = _chunk_slice.cutoff(_runtime_state->chunk_size());
        RETURN_IF_ERROR(callback(chunk));
    }
    _chunk_slice.reset(nullptr);
    _tracker->release(_tracker->consumption());
    _chunk.reset();
    return Status::OK();
}

Status OrderedMemTable::done() {
    // do sort
    ASSIGN_OR_RETURN(_chunk, _do_sort(_chunk));
    _chunk_slice.reset(_chunk);
    return Status::OK();
}

StatusOr<ChunkPtr> OrderedMemTable::_do_sort(const ChunkPtr& chunk) {
    RETURN_IF_ERROR(chunk->upgrade_if_overflow());
    DataSegment segment(_sort_exprs, chunk);
    _permutation.resize(0);

    auto& order_bys = segment.order_by_columns;
    RETURN_IF_ERROR(sort_and_tie_columns(_runtime_state->cancelled_ref(), order_bys, _sort_desc, &_permutation));

    ChunkPtr sorted_chunk = _chunk->clone_empty_with_slot(_chunk->num_rows());
    materialize_by_permutation(sorted_chunk.get(), {_chunk}, _permutation);

    return sorted_chunk;
}
} // namespace starrocks::spill