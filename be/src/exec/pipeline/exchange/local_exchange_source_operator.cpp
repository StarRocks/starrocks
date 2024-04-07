// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/exchange/local_exchange_source_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

// Used for PassthroughExchanger.
// The input chunk is most likely full, so we don't merge it to avoid copying chunk data.
Status LocalExchangeSourceOperator::add_chunk(vectorized::ChunkPtr chunk) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return Status::OK();
    }
<<<<<<< HEAD
    _memory_manager->update_row_count(chunk->num_rows());
    _full_chunk_queue.emplace(std::move(chunk));
=======

    // unpack chunk's const column, since Chunk#append_selective cannot be const column
    chunk->unpack_and_duplicate_const_columns();

    _partition_chunk_queue.emplace(std::move(chunk), std::move(indexes), from, size, memory_usage);
    _partition_rows_num += size;
    _local_memory_usage += memory_usage;
    _memory_manager->update_memory_usage(memory_usage, size);
>>>>>>> 608e00f4bd ([BugFix] Unpack const column when in local exchange source operator (#43403))

    return Status::OK();
}

// Used for PartitionExchanger.
// Only enqueue the partition chunk information here, and merge chunk in pull_chunk().
Status LocalExchangeSourceOperator::add_chunk(vectorized::ChunkPtr chunk,
                                              std::shared_ptr<std::vector<uint32_t>> indexes, uint32_t from,
                                              uint32_t size) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return Status::OK();
    }
<<<<<<< HEAD
    _memory_manager->update_row_count(size);
    _partition_chunk_queue.emplace(std::move(chunk), std::move(indexes), from, size);
    _partition_rows_num += size;
=======

    // unpack chunk's const column, since Chunk#append_selective cannot be const column
    chunk->unpack_and_duplicate_const_columns();

    auto partition_key = std::make_shared<PartitionKey>(std::make_shared<Columns>(partition_columns), (*indexes)[0]);
    auto partition_entry = _partitions.find(partition_key);
    if (partition_entry == _partitions.end()) {
        ChunkPtr one_row_chunk = chunk->clone_empty_with_slot();
        one_row_chunk->append_selective(*chunk, indexes.get()->data(), 0, 1);

        Columns one_row_partitions_columns(partition_expr_ctxs.size());
        for (size_t i = 0; i < partition_expr_ctxs.size(); ++i) {
            ASSIGN_OR_RETURN(one_row_partitions_columns[i], partition_expr_ctxs[i]->evaluate(one_row_chunk.get()));
            DCHECK(one_row_partitions_columns[i] != nullptr);
        }

        auto copied_partition_key =
                std::make_shared<PartitionKey>(std::make_shared<Columns>(one_row_partitions_columns), 0);

        auto queue = std::queue<PartitionChunk>();
        queue.emplace(std::move(chunk), std::move(indexes), from, size, memory_usage);
        PendingPartitionChunks pendingPartitionChunks(std::move(queue), size, memory_usage);
        _partitions.emplace(std::move(copied_partition_key), std::move(pendingPartitionChunks));
    } else {
        PendingPartitionChunks& chunks = partition_entry->second;
        chunks.partition_chunk_queue.emplace(std::move(chunk), std::move(indexes), from, size, memory_usage);
        chunks.partition_row_nums += size;
        chunks.memory_usage += memory_usage;
    }

    _local_memory_usage += memory_usage;
    _memory_manager->update_memory_usage(memory_usage, size);
>>>>>>> 608e00f4bd ([BugFix] Unpack const column when in local exchange source operator (#43403))

    return Status::OK();
}

bool LocalExchangeSourceOperator::is_finished() const {
    std::lock_guard<std::mutex> l(_chunk_lock);

    return _is_finished && _full_chunk_queue.empty() && !_partition_rows_num;
}

bool LocalExchangeSourceOperator::has_output() const {
    std::lock_guard<std::mutex> l(_chunk_lock);

    return !_full_chunk_queue.empty() || _partition_rows_num >= _factory->runtime_state()->chunk_size() ||
           (_is_finished && _partition_rows_num > 0);
}

Status LocalExchangeSourceOperator::set_finished(RuntimeState* state) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    _is_finished = true;
    // Compute out the number of rows of the _full_chunk_queue.
    size_t full_rows_num = 0;
    while (!_full_chunk_queue.empty()) {
        auto chunk = std::move(_full_chunk_queue.front());
        _full_chunk_queue.pop();
        full_rows_num += chunk->num_rows();
    }
    // clear _full_chunk_queue
    { [[maybe_unused]] typeof(_full_chunk_queue) tmp = std::move(_full_chunk_queue); }
    // clear _partition_chunk_queue
    { [[maybe_unused]] typeof(_partition_chunk_queue) tmp = std::move(_partition_chunk_queue); }
    // Subtract the number of rows of buffered chunks from row_count of _memory_manager and make it unblocked.
    _memory_manager->update_row_count(-(full_rows_num + _partition_rows_num));
    _partition_rows_num = 0;
    return Status::OK();
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
               rows_num + _partition_chunk_queue.front().size <= state->chunk_size()) {
            rows_num += _partition_chunk_queue.front().size;
            selected_partition_chunks.emplace_back(std::move(_partition_chunk_queue.front()));
            _partition_chunk_queue.pop();
        }
<<<<<<< HEAD
        _partition_rows_num -= rows_num;
=======
        _partition_rows_num -= num_rows;
        _local_memory_usage -= memory_usage;
        _memory_manager->update_memory_usage(-memory_usage, -num_rows);
    }
    if (selected_partition_chunks.empty()) {
        throw std::runtime_error("local exchange gets empty shuffled chunk.");
    }
    // Unlock during merging partition chunks into a full chunk.
    ChunkPtr chunk = selected_partition_chunks[0].chunk->clone_empty_with_slot();
    chunk->reserve(num_rows);
    for (const auto& partition_chunk : selected_partition_chunks) {
        // NOTE: unpack column if `partition_chunk.chunk` constains const column
        chunk->append_selective(*partition_chunk.chunk, partition_chunk.indexes->data(), partition_chunk.from,
                                partition_chunk.size);
    }
    return chunk;
}

ChunkPtr LocalExchangeSourceOperator::_pull_key_partition_chunk(RuntimeState* state) {
    std::vector<PartitionChunk> selected_partition_chunks;
    size_t num_rows = 0;
    size_t memory_usage = 0;

    {
        std::lock_guard<std::mutex> l(_chunk_lock);

        PendingPartitionChunks& pendingPartitionChunks = _max_row_partition_chunks();

        DCHECK(!pendingPartitionChunks.partition_chunk_queue.empty());

        while (!pendingPartitionChunks.partition_chunk_queue.empty() &&
               num_rows + pendingPartitionChunks.partition_chunk_queue.front().size <= state->chunk_size()) {
            num_rows += pendingPartitionChunks.partition_chunk_queue.front().size;
            memory_usage += pendingPartitionChunks.partition_chunk_queue.front().memory_usage;

            selected_partition_chunks.emplace_back(std::move(pendingPartitionChunks.partition_chunk_queue.front()));
            pendingPartitionChunks.partition_chunk_queue.pop();
        }

        pendingPartitionChunks.partition_row_nums -= num_rows;
        pendingPartitionChunks.memory_usage -= memory_usage;
        _local_memory_usage -= memory_usage;
        _memory_manager->update_memory_usage(-memory_usage, -num_rows);
>>>>>>> 608e00f4bd ([BugFix] Unpack const column when in local exchange source operator (#43403))
    }

    // Unlock during merging partition chunks into a full chunk.
    vectorized::ChunkPtr chunk = selected_partition_chunks[0].chunk->clone_empty_with_slot();
    chunk->reserve(rows_num);
    for (const auto& partition_chunk : selected_partition_chunks) {
        // NOTE: unpack column if `partition_chunk.chunk` constains const column
        chunk->append_selective(*partition_chunk.chunk, partition_chunk.indexes->data(), partition_chunk.from,
                                partition_chunk.size);
    }

    return chunk;
}

} // namespace starrocks::pipeline
