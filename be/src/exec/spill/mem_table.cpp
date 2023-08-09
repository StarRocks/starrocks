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

bool UnorderedMemTable::is_empty() {
    return _chunks.empty();
}

Status UnorderedMemTable::append(ChunkPtr chunk) {
    _tracker->consume(chunk->memory_usage());
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, chunk->memory_usage());
    _chunks.emplace_back(std::move(chunk));
    return Status::OK();
}

Status UnorderedMemTable::append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    if (_chunks.empty() || _chunks.back()->num_rows() + size > _runtime_state->chunk_size()) {
        _chunks.emplace_back(src.clone_empty());
        _tracker->consume(_chunks.back()->memory_usage());
        COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, _chunks.back()->memory_usage());
    }

    Chunk* current = _chunks.back().get();
    size_t mem_usage = current->memory_usage();
    current->append_selective(src, indexes, from, size);
    mem_usage = current->memory_usage() - mem_usage;

    _tracker->consume(mem_usage);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, mem_usage);

    return Status::OK();
}

Status UnorderedMemTable::flush(FlushCallBack callback) {
    for (const auto& chunk : _chunks) {
        RETURN_IF_ERROR(callback(chunk));
    }
    int64_t consumption = _tracker->consumption();
    _tracker->release(consumption);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, -consumption);
    _chunks.clear();
    return Status::OK();
}

StatusOr<std::shared_ptr<SpillInputStream>> UnorderedMemTable::as_input_stream(bool shared) {
    if (shared) {
        return SpillInputStream::as_stream(_chunks, _spiller);
    } else {
        return SpillInputStream::as_stream(std::move(_chunks), _spiller);
    }
}

bool OrderedMemTable::is_empty() {
    return _chunk == nullptr || _chunk->is_empty();
}

Status OrderedMemTable::append(ChunkPtr chunk) {
    if (_chunk == nullptr) {
        _chunk = chunk->clone_empty();
    }
    int64_t old_mem_usage = _chunk->memory_usage();
    _chunk->append(*chunk);
    int64_t new_mem_usage = _chunk->memory_usage();
    _tracker->set(_chunk->memory_usage());
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, new_mem_usage - old_mem_usage);
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
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, mem_usage);

    return Status::OK();
}

Status OrderedMemTable::flush(FlushCallBack callback) {
    while (!_chunk_slice.empty()) {
        auto chunk = _chunk_slice.cutoff(_runtime_state->chunk_size());
        RETURN_IF_ERROR(callback(chunk));
    }
    _chunk_slice.reset(nullptr);
    int64_t consumption = _tracker->consumption();
    _tracker->release(consumption);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, -consumption);
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