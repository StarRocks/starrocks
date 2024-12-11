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
<<<<<<< HEAD
Status LocalExchangeSourceOperator::add_chunk(ChunkPtr chunk) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return Status::OK();
=======
void LocalExchangeSourceOperator::add_chunk(ChunkPtr chunk) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }
    size_t memory_usage = chunk->memory_usage();
    size_t num_rows = chunk->num_rows();
    _local_memory_usage += memory_usage;
    _full_chunk_queue.emplace(std::move(chunk));
    _memory_manager->update_memory_usage(memory_usage, num_rows);
<<<<<<< HEAD

    return Status::OK();
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
}

// Used for PartitionExchanger.
// Only enqueue the partition chunk information here, and merge chunk in pull_chunk().
Status LocalExchangeSourceOperator::add_chunk(ChunkPtr chunk, const std::shared_ptr<std::vector<uint32_t>>& indexes,
                                              uint32_t from, uint32_t size, size_t memory_usage) {
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

<<<<<<< HEAD
Status LocalExchangeSourceOperator::add_chunk(ChunkPtr chunk, const std::shared_ptr<std::vector<uint32_t>>& indexes,
                                              uint32_t from, uint32_t size, Columns& partition_columns,
                                              const std::vector<ExprContext*>& partition_expr_ctxs,
                                              size_t memory_usage) {
=======
Status LocalExchangeSourceOperator::add_chunk(const std::vector<std::string>& partition_key,
                                              std::unique_ptr<Chunk> chunk) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return Status::OK();
    }

    // unpack chunk's const column, since Chunk#append_selective cannot be const column
    chunk->unpack_and_duplicate_const_columns();
<<<<<<< HEAD

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

=======
    auto memory_usage = chunk->memory_usage();
    auto num_rows = chunk->num_rows();

    _partition_key2partial_chunks[partition_key].queue.push(std::move(chunk));
    _partition_key2partial_chunks[partition_key].num_rows += num_rows;
    _partition_key2partial_chunks[partition_key].memory_usage += memory_usage;

    _local_memory_usage += memory_usage;
    _memory_manager->update_memory_usage(memory_usage, num_rows);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
           _key_partition_max_rows() >= _factory->runtime_state()->chunk_size() ||
           (_is_finished && (_partition_rows_num > 0 || _key_partition_max_rows() > 0)) || _local_buffer_almost_full();
=======
           _key_partition_max_rows() > 0 || (_is_finished && _partition_rows_num > 0) || _local_buffer_almost_full();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
}

Status LocalExchangeSourceOperator::set_finished(RuntimeState* state) {
    std::lock_guard<std::mutex> l(_chunk_lock);
    _is_finished = true;
    // clear _full_chunk_queue
    { [[maybe_unused]] typeof(_full_chunk_queue) tmp = std::move(_full_chunk_queue); }
    // clear _partition_chunk_queue
    { [[maybe_unused]] typeof(_partition_chunk_queue) tmp = std::move(_partition_chunk_queue); }
    // clear _key_partition_pending_chunks
<<<<<<< HEAD
    { [[maybe_unused]] typeof(_partitions) tmp = std::move(_partitions); }
=======
    { [[maybe_unused]] typeof(_partition_key2partial_chunks) tmp = std::move(_partition_key2partial_chunks); }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    // Subtract the number of rows of buffered chunks from row_count of _memory_manager and make it unblocked.
    _memory_manager->update_memory_usage(-_local_memory_usage, -_partition_rows_num);
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

<<<<<<< HEAD
=======
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

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
    std::vector<PartitionChunk> selected_partition_chunks;
=======
    std::vector<ChunkPtr> selected_partition_chunks;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    size_t num_rows = 0;
    size_t memory_usage = 0;

    {
        std::lock_guard<std::mutex> l(_chunk_lock);

<<<<<<< HEAD
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
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        _local_memory_usage -= memory_usage;
        _memory_manager->update_memory_usage(-memory_usage, -num_rows);
    }

    // Unlock during merging partition chunks into a full chunk.
<<<<<<< HEAD
    ChunkPtr chunk = selected_partition_chunks[0].chunk->clone_empty_with_slot();
    chunk->reserve(num_rows);
    for (const auto& partition_chunk : selected_partition_chunks) {
        // NOTE: unpack column if `partition_chunk.chunk` constains const column
        chunk->append_selective(*partition_chunk.chunk, partition_chunk.indexes->data(), partition_chunk.from,
                                partition_chunk.size);
=======
    ChunkPtr chunk = selected_partition_chunks[0]->clone_empty_with_slot();
    chunk->reserve(num_rows);
    for (const auto& partition_chunk : selected_partition_chunks) {
        // NOTE: unpack column if `partition_chunk.chunk` constains const column
        chunk->append(*partition_chunk);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    return chunk;
}

int64_t LocalExchangeSourceOperator::_key_partition_max_rows() const {
    int64_t max_rows = 0;
<<<<<<< HEAD
    for (const auto& partition : _partitions) {
        max_rows = std::max(partition.second.partition_row_nums, max_rows);
=======
    for (const auto& partition : _partition_key2partial_chunks) {
        max_rows = std::max(partition.second.num_rows, max_rows);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    return max_rows;
}

<<<<<<< HEAD
LocalExchangeSourceOperator::PendingPartitionChunks& LocalExchangeSourceOperator::_max_row_partition_chunks() {
    using it_type = decltype(_partitions)::value_type;
    auto max_it = std::max_element(_partitions.begin(), _partitions.end(), [](const it_type& lhs, const it_type& rhs) {
        return lhs.second.partition_row_nums < rhs.second.partition_row_nums;
    });
=======
LocalExchangeSourceOperator::PartialChunks& LocalExchangeSourceOperator::_max_row_partition_chunks() {
    auto max_it = std::max_element(_partition_key2partial_chunks.begin(), _partition_key2partial_chunks.end(),
                                   [](auto& lhs, auto& rhs) { return lhs.second.num_rows < rhs.second.num_rows; });
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    return max_it->second;
}

} // namespace starrocks::pipeline
