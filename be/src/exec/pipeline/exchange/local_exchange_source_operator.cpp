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
Status LocalExchangeSourceOperator::add_chunk(ChunkPtr chunk, const std::shared_ptr<std::vector<uint32_t>>& indexes,
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

bool LocalExchangeSourceOperator::is_finished() const {
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_full_chunk_queue.empty() && !_partition_rows_num) {
        if (UNLIKELY(_local_memory_usage != 0)) {
            throw std::runtime_error("_local_memory_usage should be 0 as there is no rows left.");
        }
    }

    return _is_finished && _full_chunk_queue.empty() && !_partition_rows_num;
}

bool LocalExchangeSourceOperator::has_output() const {
    std::lock_guard<std::mutex> l(_chunk_lock);

    return !_full_chunk_queue.empty() || _partition_rows_num >= _factory->runtime_state()->chunk_size() ||
           (_is_finished && _partition_rows_num > 0) || _local_buffer_almost_full();
}

Status LocalExchangeSourceOperator::set_finished(RuntimeState* state) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    _is_finished = true;
    // clear _full_chunk_queue
    { [[maybe_unused]] typeof(_full_chunk_queue) tmp = std::move(_full_chunk_queue); }
    // clear _partition_chunk_queue
    { [[maybe_unused]] typeof(_partition_chunk_queue) tmp = std::move(_partition_chunk_queue); }
    // Subtract the number of rows of buffered chunks from row_count of _memory_manager and make it unblocked.
    _memory_manager->update_memory_usage(-_local_memory_usage);
    _partition_rows_num = 0;
    _local_memory_usage = 0;
    return Status::OK();
}

StatusOr<ChunkPtr> LocalExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    ChunkPtr chunk = _pull_passthrough_chunk(state);
    if (chunk == nullptr) {
        chunk = _pull_shuffle_chunk(state);
    }
    return std::move(chunk);
}

const size_t min_local_memory_limit = 1LL * 1024 * 1024;

void LocalExchangeSourceOperator::enter_release_memory_mode() {
    // limit the memory limit to a very small value so that the upstream will not write new data until all the data has been pushed to the downstream operators
    _local_memory_limit = min_local_memory_limit;
    size_t max_memory_usage = min_local_memory_limit * _memory_manager->get_max_input_dop();
    _memory_manager->update_max_memory_usage(max_memory_usage);
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

} // namespace starrocks::pipeline
