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
namespace starrocks::pipeline {

Status SplitLocalExchanger::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::prepare(_split_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_split_expr_ctxs, state));
    return Status::OK();
}

void SplitLocalExchanger::close(RuntimeState* state) {
    Expr::close(_split_expr_ctxs, state);
}

Status SplitLocalExchanger::push_chunk(const ChunkPtr& chunk, SplitLocalExchangeSinkOperator* sink_op) {
    // std::unique_lock l(_mutex);
    // currently only In and not-In
    DCHECK(_split_expr_ctxs.size() == 2);
    // split chunk into multiple chunks using split_expr_ctxs
    // cur_chunk is the chunk that is being split and has no overlap with the previous chunks
    auto& cur_chunk = chunk;
    if (cur_chunk->is_empty()) return Status::OK();
    size_t original_chunk_size = cur_chunk->num_rows();
    size_t cur_chunk_size = original_chunk_size;
    size_t new_chunk_size;
    for (size_t i = _split_expr_ctxs.size() - 1; i > 0; i--) {
        Filter chunk_filter(cur_chunk->num_rows(), 1);
        auto& expr_ctx = _split_expr_ctxs[i];
        ASSIGN_OR_RETURN(new_chunk_size,
                         ExecNode::eval_conjuncts_into_filter({expr_ctx}, cur_chunk.get(), &chunk_filter));
        // all false for expr_ctx
        if (new_chunk_size == 0) {
            continue;
        }

        if (new_chunk_size == cur_chunk_size) {
            // no row is filtered, just push the cur_chunk
            std::unique_lock l(_mutex);
            DCHECK(cur_chunk->num_rows());
            _buffer[i].emplace(std::move(cur_chunk));
            _current_accumulated_row_size += cur_chunk_size;
            _current_memory_usage += cur_chunk->memory_usage();
            sink_op->update_counter(_current_memory_usage, _current_accumulated_row_size);
            return Status::OK();
        }

        auto newChunk = cur_chunk->clone_unique();
        newChunk->filter(chunk_filter);
        std::unique_lock l(_mutex);
        DCHECK(newChunk->num_rows());
        _current_accumulated_row_size += newChunk->num_rows();
        _current_memory_usage += newChunk->memory_usage();
        _buffer[i].emplace(std::move(newChunk));
        l.unlock();
        std::transform(chunk_filter.begin(), chunk_filter.end(), chunk_filter.begin(),
                       [](uint8_t val) { return val ^ 1; });
        // cur_chunk =  cur_chunk - newChunk
        cur_chunk->filter(chunk_filter);
        cur_chunk_size = cur_chunk->num_rows();
        DCHECK_EQ(cur_chunk_size + new_chunk_size, original_chunk_size);
    }

    std::unique_lock l(_mutex);
    DCHECK(cur_chunk->num_rows());
    _buffer[0].emplace(std::move(cur_chunk));
    _current_accumulated_row_size += cur_chunk_size;
    _current_memory_usage += cur_chunk->memory_usage();
    sink_op->update_counter(_current_memory_usage, _current_accumulated_row_size);
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
    if (chunk->is_empty()) return chunk;
    _buffer[consuemr_index].pop();
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

// source op
Status SplitLocalExchangeSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    _exchanger->open_source_operator(_consumer_index);

    return Status::OK();
}

Status SplitLocalExchangeSourceOperator::set_finishing(RuntimeState* state) {
    if (!_is_finished) {
        _is_finished = true;
        _exchanger->close_source_operator(_consumer_index);
    }
    return Status::OK();
}

StatusOr<ChunkPtr> SplitLocalExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    auto ret = _exchanger->pull_chunk(state, _consumer_index);
    if (ret.status().is_end_of_file()) {
        (void)set_finishing(state);
    }
    return ret;
}

bool SplitLocalExchangeSourceOperator::has_output() const {
    return _exchanger->can_pull_chunk(_consumer_index);
}
// sink op
Status SplitLocalExchangeSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _exchanger->open_sink_operator();
    _peak_memory_usage_counter = _unique_metrics->AddHighWaterMarkCounter(
            "ExchangerPeakMemoryUsage", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    _peak_buffer_row_size_counter = _unique_metrics->AddHighWaterMarkCounter(
            "ExchangerPeakBufferRowSize", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));
    return Status::OK();
}

bool SplitLocalExchangeSinkOperator::need_input() const {
    return _exchanger->can_push_chunk();
}

Status SplitLocalExchangeSinkOperator::set_finishing(RuntimeState* state) {
    if (!_is_finished) {
        _is_finished = true;
        _exchanger->close_sink_operator();
    }
    return Status::OK();
}

StatusOr<ChunkPtr> SplitLocalExchangeSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Should not pull_chunk in SplitLocalExchangeSinkOperator");
}

Status SplitLocalExchangeSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _exchanger->push_chunk(chunk, this);
}

void SplitLocalExchangeSinkOperator::update_counter(size_t memory_usage, size_t buffer_row_size) {
    _peak_memory_usage_counter->set(memory_usage);
    _peak_buffer_row_size_counter->set(buffer_row_size);
}

/// LocalExchangeSinkOperatorFactory.
Status SplitLocalExchangeSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(_exchanger->prepare(state));
    return Status::OK();
}
void SplitLocalExchangeSinkOperatorFactory::close(RuntimeState* state) {
    _exchanger->close(state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline