// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/exchange/local_exchange_source_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status LocalExchangeSourceOperator::add_chunk(vectorized::ChunkPtr chunk) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_full_chunk == nullptr) {
        _full_chunk = std::move(chunk);
    } else {
        vectorized::Columns& dest_columns = _full_chunk->columns();
        vectorized::Columns& src_columns = chunk->columns();
        size_t num_rows = chunk->num_rows();
        for (size_t i = 0; i < dest_columns.size(); i++) {
            dest_columns[i]->append(*src_columns[i], 0, num_rows);
        }
    }
    return Status::OK();
}

Status LocalExchangeSourceOperator::add_chunk(vectorized::Chunk* chunk, const uint32_t* indexes, uint32_t from,
                                              uint32_t size) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_partial_chunk == nullptr) {
        _partial_chunk = chunk->clone_empty_with_slot();
    }

    if (_partial_chunk->num_rows() + size > config::vector_chunk_size) {
        _full_chunk = std::move(_partial_chunk);
    }

    _partial_chunk->append_selective(*chunk, indexes, from, size);
    return Status::OK();
}

bool LocalExchangeSourceOperator::is_finished() const {
    std::lock_guard<std::mutex> l(_chunk_lock);
    return _is_finished && _full_chunk == nullptr;
}

bool LocalExchangeSourceOperator::has_output() {
    std::lock_guard<std::mutex> l(_chunk_lock);
    return _full_chunk != nullptr;
}

StatusOr<vectorized::ChunkPtr> LocalExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    _memory_manager->update_row_count(-_full_chunk->num_rows());
    return std::move(_full_chunk);
}

} // namespace starrocks::pipeline
