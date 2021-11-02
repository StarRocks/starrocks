// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/exchange/local_exchange_source_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status LocalExchangeSourceOperator::add_chunk(vectorized::ChunkPtr chunk) {
    std::lock_guard<std::mutex> l(_chunk_lock);

    _full_chunks.emplace(std::move(chunk));

    return Status::OK();
}

Status LocalExchangeSourceOperator::add_chunk(vectorized::Chunk* chunk, const uint32_t* indexes, uint32_t from,
                                              uint32_t size) {
    std::lock_guard<std::mutex> l(_chunk_lock);

    if (_partial_chunk == nullptr) {
        _partial_chunk = chunk->clone_empty_with_slot();
    }

    if (_partial_chunk->num_rows() + size > config::vector_chunk_size) {
        _full_chunks.emplace(std::move(_partial_chunk));
        _partial_chunk = chunk->clone_empty_with_slot();
    }

    _partial_chunk->append_selective(*chunk, indexes, from, size);
    if (_partial_chunk->num_rows() >= config::vector_chunk_size) {
        _full_chunks.emplace(std::move(_partial_chunk));
    }

    return Status::OK();
}

bool LocalExchangeSourceOperator::is_finished() const {
    std::lock_guard<std::mutex> l(_chunk_lock);

    return _is_finished && _full_chunks.empty() && _partial_chunk == nullptr;
}

bool LocalExchangeSourceOperator::has_output() const {
    std::lock_guard<std::mutex> l(_chunk_lock);

    return (!_full_chunks.empty()) || (_is_finished && _partial_chunk != nullptr);
}

StatusOr<vectorized::ChunkPtr> LocalExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    std::lock_guard<std::mutex> l(_chunk_lock);

    vectorized::ChunkPtr dst_chunk;
    if (!_full_chunks.empty()) {
        dst_chunk = std::move(_full_chunks.front());
        _full_chunks.pop();
    } else {
        DCHECK(_is_finished && _partial_chunk != nullptr);
        dst_chunk = std::move(_partial_chunk);
    }

    _memory_manager->update_row_count(-(static_cast<int32_t>(dst_chunk->num_rows())));
    return std::move(dst_chunk);
}

} // namespace starrocks::pipeline
