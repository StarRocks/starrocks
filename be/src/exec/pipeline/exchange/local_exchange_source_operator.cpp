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

#include "exec/pipeline/exchange/local_exchange_source_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

// Used for PassthroughExchanger.
// The input chunk is most likely full, so we don't merge it to avoid copying chunk data.
Status LocalExchangeSourceOperator::add_chunk(ChunkPtr chunk) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return Status::OK();
    }
    size_t memory_usage = chunk->memory_usage();
    _memory_manager->update_memory_usage(memory_usage);
    _local_memory_usage += memory_usage;
    _full_chunk_queue.emplace(std::move(chunk));

    return Status::OK();
}

// Used for PartitionExchanger.
// Only enqueue the partition chunk information here, and merge chunk in pull_chunk().
Status LocalExchangeSourceOperator::add_chunk(ChunkPtr chunk, std::shared_ptr<std::vector<uint32_t>> indexes,
                                              uint32_t from, uint32_t size, size_t memory_usage) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return Status::OK();
    }
    _memory_manager->update_memory_usage(memory_usage);
    _partition_chunk_queue.emplace(std::move(chunk), std::move(indexes), from, size, memory_usage);
    _partition_rows_num += size;
    _local_memory_usage += memory_usage;

    return Status::OK();
}

Status LocalExchangeSourceOperator::add_chunk(ChunkPtr chunk, std::shared_ptr<std::vector<uint32_t>> indexes,
                                              uint32_t from, uint32_t size, Columns& partition_columns,
                                              const std::vector<ExprContext*>& partition_expr_ctxs,
                                              size_t memory_usage) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return Status::OK();
    }

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
    _memory_manager->update_memory_usage(memory_usage);

    return Status::OK();
}

bool LocalExchangeSourceOperator::is_finished() const {
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_full_chunk_queue.empty() && !_partition_rows_num && _key_partition_pending_chunk_empty()) {
        if (UNLIKELY(_local_memory_usage != 0)) {
            throw std::runtime_error("_local_memory_usage should be 0 as there is no rows left.");
        }
    }

    return _is_finished && _full_chunk_queue.empty() && !_partition_rows_num && _key_partition_pending_chunk_empty();
}

bool LocalExchangeSourceOperator::has_output() const {
    std::lock_guard<std::mutex> l(_chunk_lock);

    return !_full_chunk_queue.empty() || _partition_rows_num >= _factory->runtime_state()->chunk_size() ||
           _key_partition_max_rows() >= _factory->runtime_state()->chunk_size() ||
           (_is_finished && (_partition_rows_num > 0 || _key_partition_max_rows() > 0)) || _local_buffer_almost_full();
}

Status LocalExchangeSourceOperator::set_finished(RuntimeState* state) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    _is_finished = true;
    // clear _full_chunk_queue
    { [[maybe_unused]] typeof(_full_chunk_queue) tmp = std::move(_full_chunk_queue); }
    // clear _partition_chunk_queue
    { [[maybe_unused]] typeof(_partition_chunk_queue) tmp = std::move(_partition_chunk_queue); }
    // clear _key_partition_pending_chunks
    { [[maybe_unused]] typeof(_partitions) tmp = std::move(_partitions); }
    // Subtract the number of rows of buffered chunks from row_count of _memory_manager and make it unblocked.
    _memory_manager->update_memory_usage(-_local_memory_usage);
    _partition_rows_num = 0;
    _local_memory_usage = 0;
    return Status::OK();
}

StatusOr<ChunkPtr> LocalExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    ChunkPtr chunk = _pull_passthrough_chunk(state);
    if (chunk == nullptr && _key_partition_pending_chunk_empty()) {
        chunk = _pull_shuffle_chunk(state);
    } else if (chunk == nullptr && !_key_partition_pending_chunk_empty()) {
        chunk = _pull_key_partition_chunk(state);
    }
    return std::move(chunk);
}

const size_t min_local_memory_limit = 1LL * 1024 * 1024;

void LocalExchangeSourceOperator::enter_release_memory_mode() {
    _local_memory_limit = min_local_memory_limit;
    _memory_manager->update_max_memory_usage(min_local_memory_limit);
}

bool LocalExchangeSourceOperator::release_memory_mode_done() const {
    return _local_memory_usage <= min_local_memory_limit;
}

void LocalExchangeSourceOperator::set_execute_mode(int performance_level) {
    enter_release_memory_mode();
}

ChunkPtr LocalExchangeSourceOperator::_pull_passthrough_chunk(RuntimeState* state) {
    std::lock_guard<std::mutex> l(_chunk_lock);

    if (!_full_chunk_queue.empty()) {
        ChunkPtr chunk = std::move(_full_chunk_queue.front());
        _full_chunk_queue.pop();
        size_t memory_usage = chunk->memory_usage();
        _memory_manager->update_memory_usage(-memory_usage);
        _local_memory_usage -= memory_usage;
        return chunk;
    }

    return nullptr;
}

ChunkPtr LocalExchangeSourceOperator::_pull_shuffle_chunk(RuntimeState* state) {
    std::vector<PartitionChunk> selected_partition_chunks;
    size_t rows_num = 0;
    size_t memory_usage = 0;
    // Lock during pop partition chunks from queue.
    {
        std::lock_guard<std::mutex> l(_chunk_lock);

        DCHECK(!_partition_chunk_queue.empty());

        while (!_partition_chunk_queue.empty() &&
               rows_num + _partition_chunk_queue.front().size <= state->chunk_size()) {
            rows_num += _partition_chunk_queue.front().size;
            memory_usage += _partition_chunk_queue.front().memory_usage;
            selected_partition_chunks.emplace_back(std::move(_partition_chunk_queue.front()));
            _partition_chunk_queue.pop();
        }
        _partition_rows_num -= rows_num;
        _local_memory_usage -= memory_usage;
        _memory_manager->update_memory_usage(-memory_usage);
    }
    if (selected_partition_chunks.empty()) {
        throw std::runtime_error("local exchange gets empty shuffled chunk.");
    }
    // Unlock during merging partition chunks into a full chunk.
    ChunkPtr chunk = selected_partition_chunks[0].chunk->clone_empty_with_slot();
    chunk->reserve(rows_num);
    for (const auto& partition_chunk : selected_partition_chunks) {
        chunk->append_selective(*partition_chunk.chunk, partition_chunk.indexes->data(), partition_chunk.from,
                                partition_chunk.size);
    }
    return chunk;
}

ChunkPtr LocalExchangeSourceOperator::_pull_key_partition_chunk(RuntimeState* state) {
    std::vector<PartitionChunk> selected_partition_chunks;
    size_t rows_num = 0;
    size_t memory_usage = 0;

    {
        std::lock_guard<std::mutex> l(_chunk_lock);

        PendingPartitionChunks& pendingPartitionChunks = _max_row_partition_chunks();

        DCHECK(!pendingPartitionChunks.partition_chunk_queue.empty());

        while (!pendingPartitionChunks.partition_chunk_queue.empty() &&
               rows_num + pendingPartitionChunks.partition_chunk_queue.front().size <= state->chunk_size()) {
            rows_num += pendingPartitionChunks.partition_chunk_queue.front().size;
            memory_usage += pendingPartitionChunks.partition_chunk_queue.front().memory_usage;

            selected_partition_chunks.emplace_back(std::move(pendingPartitionChunks.partition_chunk_queue.front()));
            pendingPartitionChunks.partition_chunk_queue.pop();
        }

        pendingPartitionChunks.partition_row_nums -= rows_num;
        pendingPartitionChunks.memory_usage -= memory_usage;
        _local_memory_usage -= memory_usage;
        _memory_manager->update_memory_usage(-memory_usage);
    }

    // Unlock during merging partition chunks into a full chunk.
    ChunkPtr chunk = selected_partition_chunks[0].chunk->clone_empty_with_slot();
    chunk->reserve(rows_num);
    for (const auto& partition_chunk : selected_partition_chunks) {
        chunk->append_selective(*partition_chunk.chunk, partition_chunk.indexes->data(), partition_chunk.from,
                                partition_chunk.size);
    }

    return chunk;
}

int64_t LocalExchangeSourceOperator::_key_partition_max_rows() const {
    int64_t max_rows = 0;
    for (const auto& partition : _partitions) {
        max_rows = std::max(partition.second.partition_row_nums, max_rows);
    }

    return max_rows;
}

LocalExchangeSourceOperator::PendingPartitionChunks& LocalExchangeSourceOperator::_max_row_partition_chunks() {
    using it_type = decltype(_partitions)::value_type;
    auto max_it = std::max_element(_partitions.begin(), _partitions.end(), [](const it_type& lhs, const it_type& rhs) {
        return lhs.second.partition_row_nums < rhs.second.partition_row_nums;
    });

    return max_it->second;
}

} // namespace starrocks::pipeline
