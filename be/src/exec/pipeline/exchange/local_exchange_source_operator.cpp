// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/exchange/local_exchange_source_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

// Used for PassthroughExchanger.
// The input chunk is most likely full, so we don't merge it to avoid copying chunk data.
Status LocalExchangeSourceOperator::add_chunk(vectorized::ChunkPtr chunk) {
    std::lock_guard<std::mutex> l(_chunk_lock);

    _full_chunk_queue.emplace(std::move(chunk));

    return Status::OK();
}

// Used for PartitionExchanger.
// Only enqueue the partition chunk information here, and merge chunk in pull_chunk().
Status LocalExchangeSourceOperator::add_chunk(vectorized::ChunkPtr chunk,
                                              std::shared_ptr<std::vector<uint32_t>> indexes, uint32_t from,
                                              uint32_t size) {
    std::lock_guard<std::mutex> l(_chunk_lock);

    _partition_chunk_queue.emplace(std::move(chunk), std::move(indexes), from, size);
    _partition_rows_num += size;

    return Status::OK();
}

bool LocalExchangeSourceOperator::is_finished() const {
    std::lock_guard<std::mutex> l(_chunk_lock);

    return _is_finished && _full_chunk_queue.empty() && !_partition_rows_num;
}

bool LocalExchangeSourceOperator::has_output() const {
    std::lock_guard<std::mutex> l(_chunk_lock);

    return !_full_chunk_queue.empty() || _partition_rows_num >= config::vector_chunk_size ||
           (_is_finished && _partition_rows_num > 0);
}

StatusOr<vectorized::ChunkPtr> LocalExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    vectorized::ChunkPtr chunk = _pull_passthrough_chunk(state);
    if (chunk == nullptr) {
        chunk = _pull_shuffle_chunk(state);
    }
    _memory_manager->update_row_count(-(static_cast<int32_t>(chunk->num_rows())));
    return std::move(chunk);
}

vectorized::ChunkPtr LocalExchangeSourceOperator::_pull_passthrough_chunk(RuntimeState* state) {
    std::lock_guard<std::mutex> l(_chunk_lock);

    if (!_full_chunk_queue.empty()) {
        vectorized::ChunkPtr chunk = std::move(_full_chunk_queue.front());
        _full_chunk_queue.pop();
        return chunk;
    }

    return nullptr;
}

vectorized::ChunkPtr LocalExchangeSourceOperator::_pull_shuffle_chunk(RuntimeState* state) {
    std::vector<PartitionChunk> selected_partition_chunks;
    size_t rows_num = 0;
    // Lock during pop partition chunks from queue.
    {
        std::lock_guard<std::mutex> l(_chunk_lock);

        DCHECK(!_partition_chunk_queue.empty());

        while (!_partition_chunk_queue.empty() &&
               rows_num + _partition_chunk_queue.front().size <= config::vector_chunk_size) {
            rows_num += _partition_chunk_queue.front().size;
            selected_partition_chunks.emplace_back(std::move(_partition_chunk_queue.front()));
            _partition_chunk_queue.pop();
        }
        _partition_rows_num -= rows_num;
    }

    // Unlock during merging partition chunks into a full chunk.
    vectorized::ChunkPtr chunk = selected_partition_chunks[0].chunk->clone_empty_with_slot();
    chunk->reserve(rows_num);
    for (const auto& partition_chunk : selected_partition_chunks) {
        chunk->append_selective(*partition_chunk.chunk, partition_chunk.indexes->data(), partition_chunk.from,
                                partition_chunk.size);
    }

    return chunk;
}

} // namespace starrocks::pipeline
