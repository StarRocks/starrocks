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
#include "exec/pipeline/exchange/local_exchange.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

// Used for PassthroughExchanger.
// The input chunk is most likely full, so we don't merge it to avoid copying chunk data.
void LocalExchangeSourceOperator::add_chunk(ChunkPtr chunk) {
    auto notify = defer_notify();
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return;
    }
    size_t memory_usage = chunk->memory_usage();
    size_t num_rows = chunk->num_rows();
    _local_memory_usage += memory_usage;
    _full_chunk_queue.emplace(std::move(chunk));
    _memory_manager->update_memory_usage(memory_usage, num_rows);
}

// Used for PartitionExchanger.
// Only enqueue the partition chunk information here, and merge chunk in pull_chunk().
Status LocalExchangeSourceOperator::add_chunk(ChunkPtr chunk, const std::shared_ptr<std::vector<uint32_t>>& indexes,
                                              uint32_t from, uint32_t size, size_t memory_usage) {
    auto notify = defer_notify();
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return Status::OK();
    }

    // unpack chunk's const column, since Chunk#append_selective cannot be const column
    chunk->unpack_and_duplicate_const_columns();

    _partition_chunk_queue.emplace(std::move(chunk), std::move(indexes), from, size, memory_usage);
    _partition_rows_num += size;
    _local_memory_usage += memory_usage;
    _memory_manager->update_memory_usage(memory_usage, size);

    return Status::OK();
}

Status LocalExchangeSourceOperator::add_chunk(const std::vector<std::string>& partition_key,
                                              std::unique_ptr<Chunk> chunk) {
    auto notify = defer_notify();
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return Status::OK();
    }

    // unpack chunk's const column, since Chunk#append_selective cannot be const column
    chunk->unpack_and_duplicate_const_columns();
    auto memory_usage = chunk->memory_usage();
    auto num_rows = chunk->num_rows();

    _partition_key2partial_chunks[partition_key].queue.push(std::move(chunk));
    _partition_key2partial_chunks[partition_key].num_rows += num_rows;
    _partition_key2partial_chunks[partition_key].memory_usage += memory_usage;

    _local_memory_usage += memory_usage;
    _memory_manager->update_memory_usage(memory_usage, num_rows);
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
           _key_partition_max_rows() > 0 || ((_is_finished || _local_buffer_almost_full()) && _partition_rows_num > 0);
}

Status LocalExchangeSourceOperator::set_finished(RuntimeState* state) {
    auto* exchanger = down_cast<LocalExchangeSourceOperatorFactory*>(_factory)->exchanger();
    exchanger->finish_source();
    // notify local-exchange sink
    // notify-condition 1. mem-buffer full 2. all finished
    auto notify = exchanger->defer_notify_sink();
    std::lock_guard<std::mutex> l(_chunk_lock);
    _is_finished = true;
    {
        // clear _full_chunk_queue
        _full_chunk_queue = {};
        // clear _partition_chunk_queue
        _partition_chunk_queue = {};
        // clear _key_partition_pending_chunks
        _partition_key2partial_chunks = std::unordered_map<std::vector<std::string>, PartialChunks>{};
    }
    // Subtract the number of rows of buffered chunks from row_count of _memory_manager and make it unblocked.
    _memory_manager->update_memory_usage(-_local_memory_usage, -_partition_rows_num);
    _partition_rows_num = 0;
    _local_memory_usage = 0;
    return Status::OK();
}

StatusOr<ChunkPtr> LocalExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    // notify sink
    auto* exchanger = down_cast<LocalExchangeSourceOperatorFactory*>(_factory)->exchanger();
    auto notify = exchanger->defer_notify_sink();
    ChunkPtr chunk = _pull_passthrough_chunk(state);
    if (chunk == nullptr && _key_partition_pending_chunk_empty()) {
        chunk = _pull_shuffle_chunk(state);
    } else if (chunk == nullptr && !_key_partition_pending_chunk_empty()) {
        chunk = _pull_key_partition_chunk(state);
    }
    return std::move(chunk);
}

std::string LocalExchangeSourceOperator::get_name() const {
    std::string finished = is_finished() ? "X" : "O";
    return fmt::format("{}_{}_{}({}) {{ has_output:{}}}", _name, _plan_node_id, (void*)this, finished, has_output());
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
        size_t num_rows = chunk->num_rows();
        _memory_manager->update_memory_usage(-memory_usage, -num_rows);
        _local_memory_usage -= memory_usage;
        return chunk;
    }

    return nullptr;
}

ChunkPtr LocalExchangeSourceOperator::_pull_shuffle_chunk(RuntimeState* state) {
    std::vector<PartitionChunk> selected_partition_chunks;
    size_t num_rows = 0;
    size_t memory_usage = 0;
    // Lock during pop partition chunks from queue.
    {
        std::lock_guard<std::mutex> l(_chunk_lock);

        DCHECK(!_partition_chunk_queue.empty());

        while (!_partition_chunk_queue.empty() &&
               num_rows + _partition_chunk_queue.front().size <= state->chunk_size()) {
            num_rows += _partition_chunk_queue.front().size;
            memory_usage += _partition_chunk_queue.front().memory_usage;
            selected_partition_chunks.emplace_back(std::move(_partition_chunk_queue.front()));
            _partition_chunk_queue.pop();
        }
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
    std::vector<ChunkPtr> selected_partition_chunks;
    size_t num_rows = 0;
    size_t memory_usage = 0;

    {
        std::lock_guard<std::mutex> l(_chunk_lock);

        PartialChunks& partial_chunks = _max_row_partition_chunks();
        DCHECK(!partial_chunks.queue.empty());

        while (!partial_chunks.queue.empty() &&
               num_rows + partial_chunks.queue.front()->num_rows() <= state->chunk_size()) {
            num_rows += partial_chunks.queue.front()->num_rows();
            memory_usage += partial_chunks.queue.front()->memory_usage();

            selected_partition_chunks.push_back(std::move(partial_chunks.queue.front()));
            partial_chunks.queue.pop();
        }

        partial_chunks.num_rows -= num_rows;
        partial_chunks.memory_usage -= memory_usage;
        _local_memory_usage -= memory_usage;
        _memory_manager->update_memory_usage(-memory_usage, -num_rows);
    }

    // Unlock during merging partition chunks into a full chunk.
    ChunkPtr chunk = selected_partition_chunks[0]->clone_empty_with_slot();
    chunk->reserve(num_rows);
    for (const auto& partition_chunk : selected_partition_chunks) {
        // NOTE: unpack column if `partition_chunk.chunk` constains const column
        chunk->append(*partition_chunk);
    }

    return chunk;
}

int64_t LocalExchangeSourceOperator::_key_partition_max_rows() const {
    int64_t max_rows = 0;
    for (const auto& partition : _partition_key2partial_chunks) {
        max_rows = std::max(partition.second.num_rows, max_rows);
    }

    return max_rows;
}

LocalExchangeSourceOperator::PartialChunks& LocalExchangeSourceOperator::_max_row_partition_chunks() {
    auto max_it = std::max_element(_partition_key2partial_chunks.begin(), _partition_key2partial_chunks.end(),
                                   [](auto& lhs, auto& rhs) { return lhs.second.num_rows < rhs.second.num_rows; });

    return max_it->second;
}

} // namespace starrocks::pipeline
