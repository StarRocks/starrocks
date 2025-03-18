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

#include "split_local_exchange.h"

#include <memory>

#include "exec/exec_node.h"

namespace starrocks::pipeline {

Status SplitLocalExchanger::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::prepare(_split_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_split_expr_ctxs, state));
    return Status::OK();
}

void SplitLocalExchanger::close(RuntimeState* state) {
    Expr::close(_split_expr_ctxs, state);
}

Status SplitLocalExchanger::init_metrics(RuntimeProfile* profile, bool is_first_sink_driver) {
    if (is_first_sink_driver) {
        _peak_memory_usage_counter = profile->AddHighWaterMarkCounter(
                "ExchangerPeakMemoryUsage", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
        _peak_buffer_row_size_counter = profile->AddHighWaterMarkCounter(
                "ExchangerPeakBufferRowSize", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));
    }
    return Status::OK();
}

Status SplitLocalExchanger::push_chunk(const ChunkPtr& chunk, int32_t sink_driver_sequence) {
    // std::unique_lock l(_mutex);
    // currently only In and not-In
    DCHECK(_split_expr_ctxs.size() == 2);
    // split chunk into multiple chunks using split_expr_ctxs
    // cur_chunk is the chunk that is being split and has no overlap with the previous chunks
    auto cur_chunk = chunk;
    if (cur_chunk->is_empty()) return Status::OK();
    size_t original_chunk_size = cur_chunk->num_rows();
    size_t cur_chunk_size = original_chunk_size;
    for (size_t i = _split_expr_ctxs.size() - 1; i > 0; i--) {
        Filter chunk_filter(cur_chunk->num_rows(), 1);
        auto& expr_ctx = _split_expr_ctxs[i];
        ASSIGN_OR_RETURN(size_t new_chunk_size,
                         ExecNode::eval_conjuncts_into_filter({expr_ctx}, cur_chunk.get(), &chunk_filter));
        // all false for expr_ctx
        if (new_chunk_size == 0) {
            continue;
        }

        if (new_chunk_size == cur_chunk_size) {
            // no row is filtered, just push the cur_chunk
            std::unique_lock l(_mutex);
            DCHECK(cur_chunk->num_rows());
            _current_accumulated_row_size += cur_chunk_size;
            _current_memory_usage += cur_chunk->memory_usage();
            _buffer[i].emplace(std::move(cur_chunk));
            return Status::OK();
        }
        // split current chunk into two chunks by expr[i]
        std::vector<uint32_t> new_chunk_row_indexes;
        new_chunk_row_indexes.reserve(new_chunk_size);
        std::vector<uint32_t> cur_chunk_row_indexes;
        cur_chunk_row_indexes.reserve(cur_chunk_size - new_chunk_size);

        for (size_t idx = 0; idx < cur_chunk_size; idx++) {
            if (chunk_filter[idx]) {
                new_chunk_row_indexes.push_back(idx);
            } else {
                cur_chunk_row_indexes.push_back(idx);
            }
        }
        auto newChunk = cur_chunk->clone_empty_with_slot(new_chunk_size);
        newChunk->append_selective(*cur_chunk, new_chunk_row_indexes.data(), 0, new_chunk_size);

        {
            std::unique_lock l(_mutex);
            DCHECK(newChunk->num_rows());
            _current_accumulated_row_size += newChunk->num_rows();
            _current_memory_usage += newChunk->memory_usage();
            _buffer[i].emplace(std::move(newChunk));
        }

        // cur_chunk =  cur_chunk - newChunk
        auto temp_chunk = cur_chunk->clone_empty_with_slot(cur_chunk_size - new_chunk_size);
        temp_chunk->append_selective(*cur_chunk, cur_chunk_row_indexes.data(), 0, cur_chunk_size - new_chunk_size);
        cur_chunk = std::move(temp_chunk);

        cur_chunk_size = cur_chunk->num_rows();
        DCHECK_EQ(cur_chunk_size + new_chunk_size, original_chunk_size);
    }

    {
        std::unique_lock l(_mutex);
        DCHECK(cur_chunk->num_rows());
        _current_accumulated_row_size += cur_chunk_size;
        _current_memory_usage += cur_chunk->memory_usage();
        _buffer[0].emplace(std::move(cur_chunk));
    }
    return Status::OK();
}

StatusOr<ChunkPtr> SplitLocalExchanger::pull_chunk(RuntimeState* state, int32_t consuemr_index) {
    std::unique_lock l(_mutex);
    if (_buffer[consuemr_index].empty()) {
        if (_opened_sink_number == 0) return Status::EndOfFile("split_local_exchanger eof");
        // because other driver with same consuemr_index also can consume this queue
        return nullptr;
    }
    ChunkPtr chunk = _buffer[consuemr_index].front();
    _buffer[consuemr_index].pop();
    if (chunk->is_empty()) return chunk;
    DCHECK_GE(_current_accumulated_row_size, chunk->num_rows());
    _current_accumulated_row_size -= chunk->num_rows();
    _current_memory_usage -= chunk->memory_usage();
    return chunk;
}

bool SplitLocalExchanger::can_pull_chunk(int32_t consumer_index) const {
    std::unique_lock l(_mutex);
    if (_opened_sink_number == 0) return true;
    return !(_buffer[consumer_index].empty());
}

bool SplitLocalExchanger::can_push_chunk() const {
    std::unique_lock l(_mutex);
    if (_current_accumulated_row_size > _chunk_size * kBufferedRowSizeScaleFactor) {
        return false;
    }
    return true;
}

void SplitLocalExchanger::open_source_operator(int32_t consumer_index) {
    std::unique_lock l(_mutex);
    if (_opened_source_opcount[consumer_index] == 0) {
        _opened_source_number += 1;
    }
    _opened_source_opcount[consumer_index] += 1;
}
void SplitLocalExchanger::close_source_operator(int32_t consumer_index) {
    std::unique_lock l(_mutex);
    _opened_source_opcount[consumer_index] -= 1;
    if (_opened_source_opcount[consumer_index] == 0) {
        _opened_source_number--;
    }
}

void SplitLocalExchanger::open_sink_operator() {
    std::unique_lock l(_mutex);
    _opened_sink_number++;
}

void SplitLocalExchanger::close_sink_operator() {
    std::unique_lock l(_mutex);
    _opened_sink_number--;
}

} // namespace starrocks::pipeline