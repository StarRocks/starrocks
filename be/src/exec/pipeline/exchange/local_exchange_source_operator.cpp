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
    _memory_manager->update_row_count(chunk->num_rows());
    _full_chunk_queue.emplace(std::move(chunk));

    return Status::OK();
}

// Used for PartitionExchanger.
// Only enqueue the partition chunk information here, and merge chunk in pull_chunk().
Status LocalExchangeSourceOperator::add_chunk(ChunkPtr chunk, std::shared_ptr<std::vector<uint32_t>> indexes,
                                              uint32_t from, uint32_t size) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return Status::OK();
    }
    _memory_manager->update_row_count(size);
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

    return !_full_chunk_queue.empty() || _partition_rows_num >= _factory->runtime_state()->chunk_size() ||
           (_is_finished && _partition_rows_num > 0);
}

Status LocalExchangeSourceOperator::set_finished(RuntimeState* state) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    _is_finished = true;
    // Compute out the number of rows of the _full_chunk_queue.
    int64_t full_rows_num = 0;
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

StatusOr<ChunkPtr> LocalExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    ChunkPtr chunk = _pull_passthrough_chunk(state);
    if (chunk == nullptr) {
        chunk = _pull_shuffle_chunk(state);
    }
    _memory_manager->update_row_count(-(static_cast<int32_t>(chunk->num_rows())));
    return std::move(chunk);
}

ChunkPtr LocalExchangeSourceOperator::_pull_passthrough_chunk(RuntimeState* state) {
    std::lock_guard<std::mutex> l(_chunk_lock);

    if (!_full_chunk_queue.empty()) {
        ChunkPtr chunk = std::move(_full_chunk_queue.front());
        _full_chunk_queue.pop();
        return chunk;
    }

    return nullptr;
}

ChunkPtr LocalExchangeSourceOperator::_pull_shuffle_chunk(RuntimeState* state) {
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
        _partition_rows_num -= rows_num;
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
