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
#include "exec/spill/executor.h"
#include "exec/spill/input_stream.h"
#include "exec/spill/serde.h"
#include "exec/workgroup/scan_task_queue.h"
#include "runtime/current_thread.h"
#include "util/runtime_profile.h"

namespace starrocks::spill {

void SpillableMemTable::reset() {
    _is_done = false;
    _num_rows = 0;
    int64_t consumption = _tracker->consumption();
    _tracker->release(consumption);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, -consumption);
}

bool UnorderedMemTable::is_empty() {
    return _chunks.empty();
}

Status UnorderedMemTable::append(ChunkPtr chunk) {
    DCHECK(!_is_done);
    DCHECK(chunk != nullptr);
    _tracker->consume(chunk->memory_usage());
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, chunk->memory_usage());
    _num_rows += chunk->num_rows();
    _chunks.emplace_back(std::move(chunk));
    return Status::OK();
}

Status UnorderedMemTable::append_selective(const Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    DCHECK(!_is_done);
    if (_chunks.empty() || _chunks.back()->num_rows() + size > _runtime_state->chunk_size()) {
        _chunks.emplace_back(src.clone_empty());
        _tracker->consume(_chunks.back()->memory_usage());
        COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, _chunks.back()->memory_usage());
    }

    Chunk* current = _chunks.back().get();
    size_t mem_usage = current->memory_usage();
    current->append_selective(src, indexes, from, size);
    _num_rows += size;
    mem_usage = current->memory_usage() - mem_usage;

    _tracker->consume(mem_usage);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, mem_usage);
    return Status::OK();
}

Status UnorderedMemTable::finalize(workgroup::YieldContext& yield_ctx, const SpillOutputDataStreamPtr& output) {
    DCHECK(_is_done) << "done must invoke before finalize";
    SCOPED_TIMER(_spiller->metrics().mem_table_finalize_timer);
    auto& serde = _spiller->serde();
    {
        SerdeContext serde_ctx;
        auto io_ctx = std::any_cast<SpillIOTaskContextPtr>(yield_ctx.task_context_data);
        bool need_aligned = _runtime_state->spill_enable_direct_io();
        while (_processed_index < _chunks.size()) {
            if (!(output->is_remote() ^ io_ctx->use_local_io_executor)) {
                TRACE_SPILL_LOG << "yield before serialize";
                yield_ctx.need_yield = true;
                io_ctx->use_local_io_executor = !output->is_remote();
                return Status::OK();
            }
            SCOPED_RAW_TIMER(&yield_ctx.time_spent_ns);
            auto chunk = _chunks[_processed_index++];
            RETURN_IF_ERROR(serde->serialize(_runtime_state, serde_ctx, chunk, output, need_aligned));
            RETURN_OK_IF_NEED_YIELD(yield_ctx.wg, &yield_ctx.need_yield, yield_ctx.time_spent_ns);
        }
    }
    TRACE_SPILL_LOG << fmt::format("finalize spillable unordered memtable done, rows[{}] {}", num_rows(), (void*)this);
    return Status::OK();
}

void UnorderedMemTable::reset() {
    DCHECK(_processed_index >= _chunks.size());
    SpillableMemTable::reset();
    _chunks.clear();
    _processed_index = 0;
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
    DCHECK(chunk != nullptr);
    if (_chunk == nullptr) {
        _chunk = chunk->clone_empty();
    }
    int64_t old_mem_usage = _chunk->memory_usage();
    _chunk->append(*chunk);
    _num_rows += chunk->num_rows();
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
    _num_rows += size;
    mem_usage = current->memory_usage() - mem_usage;

    _tracker->consume(mem_usage);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, mem_usage);
    return Status::OK();
}

Status OrderedMemTable::done() {
    ASSIGN_OR_RETURN(_chunk, _do_sort(_chunk));
    _chunk_slice.reset(_chunk);
    return SpillableMemTable::done();
}

Status OrderedMemTable::finalize(workgroup::YieldContext& yield_ctx, const SpillOutputDataStreamPtr& output) {
    DCHECK(_is_done) << "done must invoke before finalize";
    SCOPED_TIMER(_spiller->metrics().mem_table_finalize_timer);
    // seriealize data, store result into _block
    auto& serde = _spiller->serde();

    SerdeContext serde_ctx;
    auto io_ctx = std::any_cast<SpillIOTaskContextPtr>(yield_ctx.task_context_data);
    while (!_chunk_slice.empty()) {
        if (!(output->is_remote() ^ io_ctx->use_local_io_executor)) {
            TRACE_SPILL_LOG << "yield before serialize";
            yield_ctx.need_yield = true;
            io_ctx->use_local_io_executor = !output->is_remote();
            return Status::OK();
        }
        SCOPED_RAW_TIMER(&yield_ctx.time_spent_ns);
        ChunkPtr chunk = _chunk_slice.cutoff(_runtime_state->chunk_size());
        bool need_aligned = _runtime_state->spill_enable_direct_io();

        RETURN_IF_ERROR(serde->serialize(_runtime_state, serde_ctx, chunk, output, need_aligned));
        RETURN_OK_IF_NEED_YIELD(yield_ctx.wg, &yield_ctx.need_yield, yield_ctx.time_spent_ns);
    }
    TRACE_SPILL_LOG << fmt::format("finalize spillable ordered memtable done, rows[{}]", num_rows());
    // clear all data
    _chunk_slice.reset(nullptr);
    int64_t old_consumption = _tracker->consumption();
    _tracker->release(old_consumption);
    COUNTER_ADD(_spiller->metrics().mem_table_peak_memory_usage, -old_consumption);
    _chunk.reset();
    return Status::OK();
}

void OrderedMemTable::reset() {
    SpillableMemTable::reset();
    _chunk_slice.reset(nullptr);
    _chunk.reset();
    _permutation.clear();
    _permutation.shrink_to_fit();
}

StatusOr<ChunkPtr> OrderedMemTable::_do_sort(const ChunkPtr& chunk) {
    RETURN_IF_ERROR(chunk->upgrade_if_overflow());
    DataSegment segment(_sort_exprs, chunk);
    _permutation.resize(0);

    auto& order_bys = segment.order_by_columns;
    {
        SCOPED_TIMER(_spiller->metrics().sort_chunk_timer);
        RETURN_IF_ERROR(sort_and_tie_columns(_runtime_state->cancelled_ref(), order_bys, _sort_desc, &_permutation));
    }

    ChunkPtr sorted_chunk = _chunk->clone_empty_with_slot(_chunk->num_rows());
    {
        SCOPED_TIMER(_spiller->metrics().materialize_chunk_timer);
        materialize_by_permutation(sorted_chunk.get(), {_chunk}, _permutation);
    }
    return sorted_chunk;
}
} // namespace starrocks::spill