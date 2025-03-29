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

#include "exec/pipeline/exchange/mem_limited_chunk_queue.h"

#include <algorithm>
#include <memory>

#include "common/logging.h"
#include "exec/pipeline/exchange/multi_cast_local_exchange.h"
#include "exec/pipeline/exchange/multi_cast_local_exchange_sink_operator.h"
#include "exec/spill/block_manager.h"
#include "exec/spill/data_stream.h"
#include "exec/spill/dir_manager.h"
#include "exec/spill/executor.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/options.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "serde/column_array_serde.h"
#include "serde/protobuf_serde.h"
#include "testutil/sync_point.h"
#include "util/defer_op.h"
#include "util/raw_container.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

MemLimitedChunkQueue::MemLimitedChunkQueue(RuntimeState* state, int32_t consumer_number, Options opts)
        : _state(state),
          _consumer_number(consumer_number),
          _opened_source_opcount(consumer_number),
          _consumer_progress(consumer_number),
          _opts(std::move(opts)) {
    Block* block = new Block(consumer_number);
    _head = block;
    _tail = block;
    _next_flush_block = block;
    // push a dummy Cell
    Cell dummy;
    dummy.used_count = consumer_number;
    _tail->cells.emplace_back(dummy);

    for (int32_t i = 0; i < consumer_number; i++) {
        // init as dummy cell
        _consumer_progress[i] = std::make_unique<Iterator>(_head, 0);
        _opened_source_opcount[i] = 0;
    }

    _iterator.block = _head;
    _iterator.index = 0;
}

MemLimitedChunkQueue::~MemLimitedChunkQueue() {
    while (_head) {
        Block* next = _head->next;
        delete _head;
        _head = next;
    }
}

Status MemLimitedChunkQueue::init_metrics(RuntimeProfile* parent) {
    _peak_memory_bytes_counter = parent->AddHighWaterMarkCounter(
            "ExchangerPeakMemoryUsage", TUnit::BYTES,
            RuntimeProfile::Counter::create_strategy(TUnit::BYTES, TCounterMergeType::SKIP_FIRST_MERGE));
    _peak_memory_rows_counter = parent->AddHighWaterMarkCounter(
            "ExchangerPeakBufferRowSize", TUnit::UNIT,
            RuntimeProfile::Counter::create_strategy(TUnit::UNIT, TCounterMergeType::SKIP_FIRST_MERGE));

    _flush_io_timer = ADD_TIMER(parent, "FlushIOTime");
    _flush_io_count = ADD_COUNTER(parent, "FlushIOCount", TUnit::UNIT);
    _flush_io_bytes = ADD_COUNTER(parent, "FlushIOBytes", TUnit::BYTES);
    _read_io_timer = ADD_TIMER(parent, "ReadIOTime");
    _read_io_count = ADD_COUNTER(parent, "ReadIOCount", TUnit::UNIT);
    _read_io_bytes = ADD_COUNTER(parent, "ReadIOBytes", TUnit::BYTES);
    return Status::OK();
}

Status MemLimitedChunkQueue::push(const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_get_io_task_status());
    std::unique_lock l(_mutex);
    if (UNLIKELY(_chunk_builder == nullptr)) {
        _chunk_builder = chunk->clone_empty();
    }
    int32_t closed_source_number = _consumer_number - _opened_source_number;

    DCHECK(_tail != nullptr) << "tail won't be null";
    DCHECK(_tail->next == nullptr) << "tail should be the last block";

    // create a new block
    if (_tail->memory_usage >= _opts.block_size) {
        Block* block = new Block(_consumer_number);
        block->next = nullptr;
        _tail->next = block;
        _tail = block;
    }

    _total_accumulated_rows += chunk->num_rows();
    _total_accumulated_bytes += chunk->memory_usage();

    Cell cell;
    cell.chunk = chunk;
    cell.used_count = closed_source_number;
    cell.accumulated_rows = _total_accumulated_rows;
    cell.accumulated_bytes = _total_accumulated_bytes;
    _tail->cells.emplace_back(cell);
    _tail->memory_usage += chunk->memory_usage();

#ifndef BE_TEST
    size_t in_memory_rows = _total_accumulated_rows - _flushed_accumulated_rows + _current_load_rows;
    size_t in_memory_bytes = _total_accumulated_bytes - _flushed_accumulated_bytes + _current_load_bytes;
    _peak_memory_rows_counter->set(in_memory_rows);
    _peak_memory_bytes_counter->set(in_memory_bytes);
#endif
    return Status::OK();
}

bool MemLimitedChunkQueue::can_push() {
    std::shared_lock l(_mutex);
    size_t unconsumed_bytes = _total_accumulated_bytes - _fastest_accumulated_bytes;
    // if the fastest consumer still has a lot of data to consume, it will no longer accept new input.
    if (unconsumed_bytes >= _opts.max_unconsumed_bytes) {
        return false;
    }

    size_t in_memory_bytes = _total_accumulated_bytes - _flushed_accumulated_bytes;
    if (in_memory_bytes >= _opts.memory_limit && _next_flush_block->next != nullptr) {
        if (bool expected = false; _has_flush_io_task.compare_exchange_strong(expected, true)) {
            TEST_SYNC_POINT("MemLimitedChunkQueue::can_push::before_submit_flush_task");
            auto status = _submit_flush_task();
            if (!status.ok()) {
                _update_io_task_status(status);
                return true;
            }
        }
        return false;
    }

    return true;
}

StatusOr<ChunkPtr> MemLimitedChunkQueue::pop(int32_t consumer_index) {
    TEST_SYNC_POINT("MemLimitedChunkQueue::pop::before_pop");
    DCHECK(consumer_index <= _consumer_number);
    RETURN_IF_ERROR(_get_io_task_status());
    std::unique_lock l(_mutex);

    DCHECK(_consumer_progress[consumer_index] != nullptr);
    auto iter = _consumer_progress[consumer_index].get();
    if (!iter->has_next()) {
        if (_opened_sink_number == 0) {
            return Status::EndOfFile("no more data");
        }
        return Status::InternalError("unreachable path");
    }
    // if the block is flushed forcely between can_pop and pop,
    // we should return null and trigger load io task in next can_pop
    if (!iter->next().get_block()->in_mem) {
        return nullptr;
    }
    iter->move_to_next();
    DCHECK(iter->get_block()->in_mem);
    Cell* cell = iter->get_cell();
    DCHECK(cell->chunk != nullptr);

    cell->used_count += 1;
    VLOG_ROW << fmt::format(
            "[MemLimitedChunkQueue] pop chunk for consumer[{}] from block[{}], accumulated_rows[{}], "
            "accumulated_bytes[{}], used_count[{}]",
            consumer_index, (void*)(iter->get_block()), cell->accumulated_rows, cell->accumulated_bytes,
            cell->used_count);
    iter->get_block()->remove_pending_reader(consumer_index);
    auto result = cell->chunk;

    _update_progress(iter);
    return result;
}

void MemLimitedChunkQueue::_evict_loaded_block() {
    if (_loaded_blocks.size() < _consumer_number) {
        return;
    }
    // because each consumer can only read one block at a time,
    // we can definitely find a block that no one is reading
    Block* block = nullptr;
    for (auto& loaded_block : _loaded_blocks) {
        if (!loaded_block->has_pending_reader()) {
            block = loaded_block;
            break;
        }
    }
    DCHECK(block != nullptr) << "can't find evicted block";
    _loaded_blocks.erase(block);
    _current_load_rows -= block->flush_rows;
    _current_load_bytes -= block->flush_bytes;
    for (auto& cell : block->cells) {
        if (cell.chunk != nullptr) {
            cell.chunk.reset();
        }
    }
    block->in_mem = false;
    VLOG_ROW << fmt::format("[MemLimitedChunkQueue] evict block [{}]", (void*)block);
}

bool MemLimitedChunkQueue::can_pop(int32_t consumer_index) {
    DCHECK(consumer_index < _consumer_number);
    std::shared_lock l(_mutex);
    DCHECK(_consumer_progress[consumer_index] != nullptr);

    auto iter = _consumer_progress[consumer_index].get();
    if (iter->has_next()) {
        auto next_iter = iter->next();
        if (next_iter.block->in_mem) {
            next_iter.block->add_pending_reader(consumer_index);
            TEST_SYNC_POINT("MemLimitedChunkQueue::can_pop::return_true::1");
            return true;
        }
        TEST_SYNC_POINT_CALLBACK("MemLimitedChunkQueue::can_pop::before_submit_load_task", next_iter.block);
        if (bool expected = false; next_iter.block->has_load_task.compare_exchange_strong(expected, true)) {
            VLOG_ROW << fmt::format("[MemLimitedChunkQueue] submit load task for block [{}]", (void*)next_iter.block);
            auto status = _submit_load_task(next_iter.block);
            if (!status.ok()) {
                _update_io_task_status(status);
                return true;
            }
        }
        return false;
    } else if (_opened_sink_number == 0) {
        TEST_SYNC_POINT("MemLimitedChunkQueue::can_pop::return_true::2");
        return true;
    }

    return false;
}

void MemLimitedChunkQueue::open_consumer(int32_t consumer_index) {
    std::unique_lock l(_mutex);
    if (_opened_source_opcount[consumer_index] == 0) {
        _opened_source_number++;
    }
    _opened_source_opcount[consumer_index]++;
}

void MemLimitedChunkQueue::close_consumer(int32_t consumer_index) {
    std::unique_lock l(_mutex);
    _opened_source_opcount[consumer_index] -= 1;
    if (_opened_source_opcount[consumer_index] == 0) {
        _opened_source_number--;
        _close_consumer(consumer_index);
    }
}

void MemLimitedChunkQueue::open_producer() {
    std::unique_lock l(_mutex);
    _opened_sink_number++;
}

void MemLimitedChunkQueue::close_producer() {
    std::unique_lock l(_mutex);
    _opened_sink_number--;
}

void MemLimitedChunkQueue::_update_progress(Iterator* iter) {
    if (iter != nullptr) {
        Cell* cell = iter->get_cell();
        _fastest_accumulated_rows = std::max(_fastest_accumulated_rows, cell->accumulated_rows);
        _fastest_accumulated_bytes = std::max(_fastest_accumulated_bytes, cell->accumulated_bytes);
    } else {
        // nullptr means one consumer is closed, should update
        _fastest_accumulated_rows = 0;
        _fastest_accumulated_bytes = 0;
        for (int32_t i = 0; i < _consumer_number; i++) {
            auto iter = _consumer_progress[i].get();
            if (iter == nullptr) {
                continue;
            }
            if (!iter->has_next()) {
                // all data is consumed
                _fastest_accumulated_rows = _total_accumulated_rows;
                _fastest_accumulated_bytes = _total_accumulated_bytes;
                break;
            }
            Cell* cell = iter->get_cell();
            _fastest_accumulated_rows = std::max(_fastest_accumulated_rows, cell->accumulated_rows);
            _fastest_accumulated_bytes = std::max(_fastest_accumulated_bytes, cell->accumulated_bytes);
        }
    }

    while (_iterator.valid()) {
        Cell* cell = _iterator.get_cell();
        if (cell->used_count != _consumer_number) {
            break;
        }
        _head_accumulated_rows = cell->accumulated_rows;
        _head_accumulated_bytes = cell->accumulated_bytes;
        // advance flushed position
        _flushed_accumulated_rows = std::max(_flushed_accumulated_rows, _head_accumulated_rows);
        _flushed_accumulated_bytes = std::max(_flushed_accumulated_bytes, _head_accumulated_bytes);
        VLOG_ROW << fmt::format("release chunk, current head_accumulated_rows[{}], head_accumulated_bytes[{}]",
                                _head_accumulated_rows, _head_accumulated_bytes);
        cell->chunk.reset();
        if (!_iterator.has_next()) {
            break;
        }
        _iterator.move_to_next();
    }
}

void MemLimitedChunkQueue::_close_consumer(int32_t consumer_index) {
    DCHECK(_consumer_progress[consumer_index] != nullptr);
    auto iter = _consumer_progress[consumer_index].get();
    if (iter->has_next()) {
        do {
            iter->move_to_next();
            Cell* cell = iter->get_cell();
            cell->used_count += 1;
        } while (iter->has_next());
    }
    _consumer_progress[consumer_index].reset();
    _update_progress();
}

Status MemLimitedChunkQueue::_flush() {
    Block* block = nullptr;
    std::vector<ChunkPtr> chunks;
    {
        std::unique_lock l(_mutex);

        std::vector<size_t> flush_idx;
        // 1. find block to flush
        while (_next_flush_block) {
            // don't flush the last block
            if (_next_flush_block->next == nullptr) {
                break;
            }
            if (_next_flush_block->block) {
                // already flushed, should skip it
                _next_flush_block = _next_flush_block->next;
            } else {
                flush_idx.clear();
                for (size_t i = 0; i < _next_flush_block->cells.size(); i++) {
                    auto& cell = _next_flush_block->cells[i];
                    if (cell.chunk == nullptr || cell.used_count == _consumer_number) {
                        continue;
                    }
                    flush_idx.push_back(i);
                }
                if (!flush_idx.empty()) {
                    break;
                }
                _next_flush_block = _next_flush_block->next;
            }
        }
        block = _next_flush_block;
        DCHECK(block != nullptr) << "block can't be null";
        TEST_SYNC_POINT_CALLBACK("MemLimitedChunkQueue::flush::after_find_block_to_flush", block);
        if (block->next == nullptr) {
            return Status::OK();
        }
        if (block->block != nullptr) {
            return Status::OK();
        }
        // If a consumer is about to read this block, we should avoid flushing it.
        // However, there is one exception: when the fatest consumer has no data to consume,
        // in order to avoid blocking the producer, we must flush it.
        // In this case, if the consumer reads this block, we will return null and trigger a new load task on the next can_pop
        if (block->has_pending_reader()) {
            size_t unconsumed_bytes = _total_accumulated_bytes - _fastest_accumulated_bytes;
            if (unconsumed_bytes > 0) {
                VLOG_ROW << fmt::format("block [{}] has pending reader, should skip flush it", (void*)block);
                return Status::OK();
            }
            VLOG_ROW << fmt::format("force flush block [{}] to avoid blocking producer", (void*)block);
        }

        for (const auto idx : flush_idx) {
            auto& cell = block->cells[idx];
            cell.flushed = true;
            chunks.emplace_back(cell.chunk);
        }
    }

    DCHECK(!chunks.empty());

    size_t max_serialize_size = 0;
    size_t flushed_rows = 0, flushed_bytes = 0;
    for (auto& chunk : chunks) {
        for (const auto& column : chunk->columns()) {
            max_serialize_size += serde::ColumnArraySerde::max_serialized_size(*column, _opts.encode_level);
        }
        flushed_rows += chunk->num_rows();
        flushed_bytes += chunk->memory_usage();
    }
    std::shared_ptr<spill::Block> spill_block;

    TEST_SYNC_POINT_CALLBACK("MemLimitedChunkQueue::after_calculate_max_serialize_size", &max_serialize_size);
    raw::RawString serialize_buffer;
    serialize_buffer.resize(max_serialize_size);
    uint8_t* buf = reinterpret_cast<uint8_t*>(serialize_buffer.data());
    uint8_t* begin = buf;
    // 2. serialize data
    for (auto& chunk : chunks) {
        for (const auto& column : chunk->columns()) {
            buf = serde::ColumnArraySerde::serialize(*column, buf, false, _opts.encode_level);
            RETURN_IF(buf == nullptr, Status::InternalError("serialize data error"));
        }
    }
    size_t content_length = buf - begin;
    // 3. acquire block
    spill::AcquireBlockOptions options;
    options.block_size = content_length;
    options.direct_io = false;
    options.query_id = _state->query_id();
    options.fragment_instance_id = _state->fragment_instance_id();
    options.name = "mcast_local_exchange";
    options.plan_node_id = _opts.plan_node_id;
    ASSIGN_OR_RETURN(spill_block, _opts.block_manager->acquire_block(options));

    // 4. flush serialized data
    std::vector<Slice> data;
    data.emplace_back(serialize_buffer.data(), content_length);
    {
        SCOPED_TIMER(_flush_io_timer);
        RETURN_IF_ERROR(spill_block->append(data));
        RETURN_IF_ERROR(spill_block->flush());
        COUNTER_UPDATE(_flush_io_count, 1);
        COUNTER_UPDATE(_flush_io_bytes, content_length);
    }

    {
        std::unique_lock l(_mutex);
        block->block = spill_block;
        block->in_mem = false;
        // 5. clear flushed data in memory
        for (auto& cell : block->cells) {
            if (cell.flushed) {
                cell.chunk.reset();
                block->flush_chunks++;
            }
        }
        block->flush_rows = flushed_rows;
        block->flush_bytes = flushed_bytes;
        _flushed_accumulated_rows = block->cells.back().accumulated_rows;
        _flushed_accumulated_bytes = block->cells.back().accumulated_bytes;
        _next_flush_block = block->next;
        VLOG_ROW << fmt::format("flush block [{}], rows[{}], bytes[{}], flushed bytes[{}]", (void*)block,
                                _flushed_accumulated_rows, _flushed_accumulated_bytes, content_length);
        TEST_SYNC_POINT_CALLBACK("MemLimitedChunkQueue::after_flush_block", block);
    }
    return Status::OK();
}

Status MemLimitedChunkQueue::_submit_flush_task() {
    auto flush_task = [this, guard = RESOURCE_TLS_MEMTRACER_GUARD(_state)](auto& yield_ctx) {
        TEST_SYNC_POINT("MemLimitedChunkQueue::before_execute_flush_task");
        RETURN_IF(!guard.scoped_begin(), (void)0);
        DEFER_GUARD_END(guard);
        auto defer = DeferOp([&]() {
            TEST_SYNC_POINT("MemLimitedChunkQueue::after_execute_flush_task");
            _has_flush_io_task.store(false);
        });

        auto status = _flush();
        if (!status.ok()) {
            _update_io_task_status(status);
            return;
        }
    };

    auto io_task = workgroup::ScanTask(_state->fragment_ctx()->workgroup(), std::move(flush_task));
    RETURN_IF_ERROR(spill::IOTaskExecutor::submit(std::move(io_task)));
    return Status::OK();
}

// reload block from disk
Status MemLimitedChunkQueue::_load(Block* block) {
    std::shared_ptr<spill::BlockReader> block_reader;
    raw::RawString buffer;
    size_t block_len;
    size_t flush_chunks;
    // 1. read serialize data
    {
        std::shared_lock l(_mutex);
        DCHECK(!block->in_mem) << "block should not be in memory, " << (void*)(block);
        DCHECK(block->block != nullptr) << "block must have spill block";

        spill::BlockReaderOptions options;
        options.enable_buffer_read = true;
        options.read_io_timer = _read_io_timer;
        options.read_io_count = _read_io_count;
        options.read_io_bytes = _read_io_bytes;
        block_reader = block->block->get_reader(options);
        block_len = block->block->size();
        flush_chunks = block->flush_chunks;
    }
    VLOG_ROW << fmt::format("load block begin, block[{}], flushed_bytes[{}], flushed_chunks[{}]", (void*)block,
                            block_len, flush_chunks);
    buffer.resize(block_len);
    RETURN_IF_ERROR(block_reader->read_fully(buffer.data(), block_len));

    // 2. deserialize data
    uint8_t* buf = reinterpret_cast<uint8_t*>(buffer.data());
    const uint8_t* read_cursor = buf;
    std::vector<ChunkPtr> chunks(flush_chunks);
    for (auto& chunk : chunks) {
        chunk = _chunk_builder->clone_empty();
        for (auto& column : chunk->columns()) {
            read_cursor = serde::ColumnArraySerde::deserialize(read_cursor, column.get(), false, _opts.encode_level);
            RETURN_IF(read_cursor == nullptr, Status::InternalError("deserialize failed"));
        }
    }
    {
        std::unique_lock l(_mutex);
        for (size_t i = 0, j = 0; i < chunks.size() && j < block->cells.size(); j++) {
            if (block->cells[j].flushed) {
                block->cells[j].chunk = chunks[i++];
            }
        }
        block->in_mem = true;
        block->has_load_task = false;
        _evict_loaded_block();
        _loaded_blocks.insert(block);
        _current_load_rows += block->flush_rows;
        _current_load_bytes += block->flush_bytes;
        VLOG_ROW << fmt::format("load block done, block[{}], current load rows[{}], current load bytes[{}]",
                                (void*)block, _current_load_rows, _current_load_bytes);
    }

    return Status::OK();
}

Status MemLimitedChunkQueue::_submit_load_task(Block* block) {
    auto load_task = [this, block, guard = RESOURCE_TLS_MEMTRACER_GUARD(_state)](auto& yield_ctx) {
        TEST_SYNC_POINT_CALLBACK("MemLimitedChunkQueue::before_execute_load_task", block);
        RETURN_IF(!guard.scoped_begin(), (void)0);
        DEFER_GUARD_END(guard);
        auto status = _load(block);
        if (!status.ok()) {
            _update_io_task_status(status);
        }
    };
    auto io_task = workgroup::ScanTask(_state->fragment_ctx()->workgroup(), std::move(load_task));
    RETURN_IF_ERROR(spill::IOTaskExecutor::submit(std::move(io_task)));
    return Status::OK();
}

const size_t memory_limit_per_producer = 1L * 1024 * 1024;

void MemLimitedChunkQueue::enter_release_memory_mode() {
    std::unique_lock l(_mutex);
    size_t new_memory_limit = memory_limit_per_producer * _opened_sink_number;
    VLOG_ROW << fmt::format("change memory limit from [{}] to [{}]", _opts.memory_limit, new_memory_limit);
    _opts.memory_limit = new_memory_limit;
}

} // namespace starrocks::pipeline