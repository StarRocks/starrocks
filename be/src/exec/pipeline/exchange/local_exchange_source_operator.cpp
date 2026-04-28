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
#include "column/chunk_extra_data.h"
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
    auto memory_entry =
            std::make_shared<ChunkBufferMemoryEntry>(_memory_manager.get(), chunk->memory_usage(), chunk->num_rows());
    _full_chunk_queue.push(PassthroughChunk{std::move(chunk), std::move(memory_entry)});
}

// Used for PartitionExchanger.
// Only enqueue the partition chunk information here, and merge chunk in pull_chunk().
// The shared `memory_entry` accounts for the source chunk's memory once across all
// partition shards; it is only released back to the manager when the last shard is
// pulled (or cleared on set_finished). The caller (Partitioner::send_chunk) is
// responsible for unpacking const columns BEFORE constructing the entry, so the
// recorded memory matches the buffered post-unpack footprint.
Status LocalExchangeSourceOperator::add_chunk(ChunkPtr chunk, const std::shared_ptr<std::vector<uint32_t>>& indexes,
                                              uint32_t from, uint32_t size,
                                              std::shared_ptr<ChunkBufferMemoryEntry> memory_entry) {
    auto notify = defer_notify();
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return Status::OK();
    }

    _partition_chunk_queue.emplace(std::move(chunk), indexes, from, size, std::move(memory_entry));
    _partition_rows_num += size;

    return Status::OK();
}

Status LocalExchangeSourceOperator::add_chunk(const std::vector<std::optional<std::string>>& partition_key,
                                              const std::vector<std::pair<TypeDescriptor, ColumnPtr>>& partition_datum,
                                              ChunkUniquePtr chunk) {
    auto notify = defer_notify();
    std::lock_guard<std::mutex> l(_chunk_lock);
    if (_is_finished) {
        return Status::OK();
    }

    // unpack chunk's const column, since Chunk#append_selective cannot be const column
    chunk->unpack_and_duplicate_const_columns();
    auto num_rows = chunk->num_rows();
    auto memory_entry =
            std::make_shared<ChunkBufferMemoryEntry>(_memory_manager.get(), chunk->memory_usage(), num_rows);

    auto& partial = _partition_key2partial_chunks[partition_key];
    partial.queue.push(KeyPartitionChunk{std::move(chunk), std::move(memory_entry)});
    partial.num_rows += num_rows;
    partial.partition_key_datum = partition_datum;
    return Status::OK();
}

bool LocalExchangeSourceOperator::is_finished() const {
    std::lock_guard<std::mutex> l(_chunk_lock);
    return _is_finished && _full_chunk_queue.empty() && !_partition_rows_num && _key_partition_pending_chunk_empty();
}

bool LocalExchangeSourceOperator::has_output() const {
    std::lock_guard<std::mutex> l(_chunk_lock);

    return !_full_chunk_queue.empty() || _partition_rows_num >= _factory->runtime_state()->chunk_size() ||
           _key_partition_max_rows() > 0 || ((_is_finished || _memory_manager->is_full()) && _partition_rows_num > 0);
}

Status LocalExchangeSourceOperator::set_finished(RuntimeState* state) {
    auto* exchanger = down_cast<LocalExchangeSourceOperatorFactory*>(_factory)->exchanger();
    exchanger->finish_source();
    // notify local-exchange sink
    // notify-condition 1. mem-buffer full 2. all finished
    auto notify = exchanger->defer_notify_sink();
    std::lock_guard<std::mutex> l(_chunk_lock);
    _is_finished = true;

    // Drop all buffered chunks. Every queue entry holds a ChunkBufferMemoryEntry whose
    // destructor refunds memory/rows back to the shared manager, so no manual accounting
    // is needed here.
    _full_chunk_queue = {};
    _partition_chunk_queue = {};
    _partition_key2partial_chunks.clear();
    _partition_rows_num = 0;
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
    size_t max_memory_usage = min_local_memory_limit * _memory_manager->get_max_input_dop();
    _memory_manager->update_max_memory_usage(max_memory_usage);
}

void LocalExchangeSourceOperator::set_execute_mode(int performance_level) {
    enter_release_memory_mode();
}

ChunkPtr LocalExchangeSourceOperator::_pull_passthrough_chunk(RuntimeState* state) {
    PassthroughChunk popped;
    {
        std::lock_guard<std::mutex> l(_chunk_lock);
        if (_full_chunk_queue.empty()) {
            return nullptr;
        }
        popped = std::move(_full_chunk_queue.front());
        _full_chunk_queue.pop();
    }
    // popped.memory_entry destructs at function exit and refunds memory/rows to the
    // manager outside the lock.
    return std::move(popped.chunk);
}

ChunkPtr LocalExchangeSourceOperator::_pull_shuffle_chunk(RuntimeState* state) {
    std::vector<PartitionChunk> selected_partition_chunks;
    size_t num_rows = 0;
    // Lock during pop partition chunks from queue.
    {
        std::lock_guard<std::mutex> l(_chunk_lock);

        DCHECK(!_partition_chunk_queue.empty());

        while (!_partition_chunk_queue.empty() &&
               num_rows + _partition_chunk_queue.front().size <= state->chunk_size()) {
            num_rows += _partition_chunk_queue.front().size;
            selected_partition_chunks.emplace_back(std::move(_partition_chunk_queue.front()));
            _partition_chunk_queue.pop();
        }
        _partition_rows_num -= num_rows;
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
    // selected_partition_chunks goes out of scope here. Each PartitionChunk drops its
    // shared_ptr<ChunkBufferMemoryEntry>; once the last shard for a given source chunk is
    // released, the entry destructor refunds its memory/rows to the manager.
    return chunk;
}

ChunkPtr LocalExchangeSourceOperator::_pull_key_partition_chunk(RuntimeState* state) {
    std::vector<KeyPartitionChunk> selected_partition_chunks;
    size_t num_rows = 0;
    ChunkExtraColumnsDataPtr chunk_extra_data;
    std::vector<std::pair<TypeDescriptor, ColumnPtr>> partition_key_datum;
    {
        std::lock_guard<std::mutex> l(_chunk_lock);
        auto it = _max_row_partition_chunks();
        PartialChunks& partial_chunks = it->second;
        partition_key_datum = partial_chunks.partition_key_datum;
        DCHECK(!partial_chunks.queue.empty());

        while (!partial_chunks.queue.empty() &&
               num_rows + partial_chunks.queue.front().chunk->num_rows() <= state->chunk_size()) {
            num_rows += partial_chunks.queue.front().chunk->num_rows();
            selected_partition_chunks.push_back(std::move(partial_chunks.queue.front()));
            partial_chunks.queue.pop();
        }

        partial_chunks.num_rows -= num_rows;
    }
    Columns partition_key_columns;
    std::vector<ChunkExtraColumnsMeta> extra_metas;
    for (auto& datum : partition_key_datum) {
        auto res = ColumnHelper::create_column(datum.first, true);
        res->append_datum(datum.second->get(0));
        auto ptr = ConstColumn::create(std::move(res), 1);
        partition_key_columns.emplace_back(ptr);
        extra_metas.push_back(ChunkExtraColumnsMeta{datum.first, true /*useless*/, true /*useless*/});
    }
    chunk_extra_data = std::make_shared<ChunkExtraColumnsData>(extra_metas, std::move(partition_key_columns));
    // Unlock during merging partition chunks into a full chunk.
    ChunkPtr chunk = selected_partition_chunks[0].chunk->clone_empty_with_slot();
    chunk->reserve(num_rows);
    chunk->set_extra_data(chunk_extra_data);
    for (const auto& partition_chunk : selected_partition_chunks) {
        // NOTE: unpack column if `partition_chunk.chunk` constains const column
        chunk->append(*partition_chunk.chunk);
    }
    // selected_partition_chunks goes out of scope here; each entry destructor refunds
    // its chunk's memory/rows to the manager outside the lock.
    return chunk;
}

int64_t LocalExchangeSourceOperator::_key_partition_max_rows() const {
    int64_t max_rows = 0;
    for (const auto& partition : _partition_key2partial_chunks) {
        max_rows = std::max(partition.second.num_rows, max_rows);
    }

    return max_rows;
}

std::map<std::vector<std::optional<std::string>>, LocalExchangeSourceOperator::PartialChunks>::iterator
LocalExchangeSourceOperator::_max_row_partition_chunks() {
    auto max_it = std::max_element(_partition_key2partial_chunks.begin(), _partition_key2partial_chunks.end(),
                                   [](auto& lhs, auto& rhs) { return lhs.second.num_rows < rhs.second.num_rows; });

    return max_it;
}

} // namespace starrocks::pipeline
