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

#include "exec/spill/block_manager.h"
#include "exec/spill/data_stream.h"
#include "exec/spill/dir_manager.h"
#include "exec/spill/executor.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/options.h"
#include "fs/fs.h"
#include "serde/column_array_serde.h"
#include "serde/protobuf_serde.h"
#include "util/defer_op.h"
#include "util/raw_container.h"
#include "util/runtime_profile.h"
#include "fmt/format.h"
#include "exec/pipeline/exchange/multi_cast_local_exchange_sink_operator.h"
#include "testutil/sync_point.h"

namespace starrocks::pipeline {

MemLimitedChunkQueue::MemLimitedChunkQueue(RuntimeState* state, RuntimeProfile* profile, int32_t consumer_number, Options opts):
        _state(state), _consumer_number(consumer_number), _opened_source_opcount(consumer_number), _consumer_progress(consumer_number), _opts(std::move(opts)) {
    // what if no dummy block
    Block* block = new Block();
    block->next = nullptr;
    _head = block;
    _tail = block;
    _next_flush_block = block;
    // push a dummy Cell
    Cell dummy;
    dummy.used_count = consumer_number;
    _tail->cells.emplace_back(dummy);

    for (int32_t i = 0;i < consumer_number;i++) {
        // init as dummy cell
        _consumer_progress[i] = std::make_unique<Iterator>(_head, 0);
        _opened_source_opcount[i] = 0;
    }

    _iterator.block = _head;
    _iterator.index = 0;

    _peak_memory_bytes_counter = profile->AddHighWaterMarkCounter(
        "PeakMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    _peak_memory_rows_counter = profile->AddHighWaterMarkCounter(
        "PeakMemoryRows", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));

    // @TODO link to operator
    std::string parent = "SpillStatistics";
    ADD_COUNTER(profile, parent, TUnit::NONE);
    _read_io_timer = ADD_CHILD_TIMER(profile, "ReadIOTime", parent);
    _read_io_count = ADD_CHILD_COUNTER(profile, "ReadIOCount", TUnit::UNIT, parent);
    _read_io_bytes = ADD_CHILD_COUNTER(profile, "ReadIOBytes", TUnit::BYTES, parent);
}

MemLimitedChunkQueue::~MemLimitedChunkQueue() {
    while (_head) {
        Block* next = _head->next;
        delete _head;
        _head = next;
    }
}

Status MemLimitedChunkQueue::push(const ChunkPtr& chunk, MultiCastLocalExchangeSinkOperator* sink_operator) {
    std::unique_lock l(_mutex);
    // @TODO we can move it into flush task
    if (UNLIKELY(_chunk_builder == nullptr)) {
        _chunk_builder = chunk->clone_empty();
    }
    int32_t closed_source_number = _consumer_number - _opened_source_number;
    
    DCHECK(_tail != nullptr) << "tail won't be null";
    DCHECK(_tail->next == nullptr) << "tail should be the last block";

    // create a new block
    if (_tail->memory_usage >= _opts.block_size) {
        Block* block = new Block();
        block->next = nullptr;
        LOG(INFO) << fmt::format("create a new block {}, prev block {}, mem {}, cells {}", (void*)(block), (void*)(_tail), _tail->memory_usage, _tail->cells.size());
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

    // @TODO should consider load
    size_t in_memory_rows = _total_accumulated_rows - _flushed_accumulated_rows + _current_load_rows;
    size_t in_memory_bytes = _total_accumulated_bytes - _flushed_accumulated_bytes + _current_load_bytes;
    _peak_memory_rows_counter->set(in_memory_rows);
    _peak_memory_bytes_counter->set(in_memory_bytes);
    // @TODO consider load mem bytes
#ifndef BE_TEST
    sink_operator->update_counter(in_memory_bytes, in_memory_rows); 
#endif
    LOG(INFO) << fmt::format("push chunk to block {}, rows {}, bytes {}, cells {}, mem {}, in_mem_rows {}, in_mem_bytes {}, load bytes {}, load rows {}",
        (void*)(_tail), _total_accumulated_rows, _total_accumulated_rows, _tail->cells.size(), _tail->memory_usage,
        in_memory_rows, in_memory_bytes, _current_load_bytes, _current_load_rows);

    return Status::OK();
}

bool MemLimitedChunkQueue::can_push() {
    // @TODO add some limit
    // 1. _total_accumulated_bytes - _fatest_accmulated_bytes >= limit, can't push
    // 2. if _total_accumulated_bytes - _flushed_accumulated_bytes >= limit, should trigger a flush io task
    std::unique_lock l(_mutex);
    size_t unconsumed_bytes = _total_accumulated_bytes - _fastest_accumulated_bytes;
    if (unconsumed_bytes >= _opts.max_unconsumed_bytes) {
        LOG(INFO) << fmt::format("too many pending data, should wait {}", unconsumed_bytes);
        return false;
    }

    // size_t in_memory_bytes = _total_accumulated_bytes - _head_accumulated_bytes;
    size_t in_memory_bytes = _total_accumulated_bytes - _flushed_accumulated_bytes;
    // LOG(INFO) << "in_memory_bytes: " << in_memory_bytes;
    // @TODO shoule update non_flushed_block??
    if (in_memory_bytes >= _opts.memory_limit && _next_flush_block->next != nullptr) {
        // LOG(INFO) << fmt::format("too many in memory data, should flush {}", in_memory_bytes);
        // @TODO can change to atomic var, use CAS
        if (!_has_flush_io_task) {
            // @TODO choose block
            // pick block that need flush
            TEST_SYNC_POINT("MemLimitedChunkQueue::can_push::before_submit_flush_task");
            _has_flush_io_task = true;
            auto status = submit_flush_task();
            if (!status.ok()) {
                _io_task_status = status;
                return true;
            }
        } else {
            LOG(INFO) << fmt::format("already has flush io task, skip trigger...");
        }
        return false;
    }

    return true;
}

StatusOr<ChunkPtr> MemLimitedChunkQueue::pop(int32_t consumer_index) {
    DCHECK(consumer_index <= _consumer_number);
    std::unique_lock l(_mutex);
    RETURN_IF_ERROR(_io_task_status);

    DCHECK(_consumer_progress[consumer_index] != nullptr);
    auto iter = _consumer_progress[consumer_index].get();
    if (!iter->has_next()) {
        if (_opened_sink_number == 0) {
            LOG(INFO) << fmt::format("no data for consumer {}", consumer_index);
            return Status::EndOfFile("no more data");
        }
        return Status::InternalError("unreachable path");
    }
    iter->move_to_next();
    Cell* cell = iter->get_cell();
    DCHECK(cell->chunk != nullptr);

    cell->used_count += 1;
    // @TODO update progress

    LOG(INFO) << fmt::format("pop chunk, {}, cell {}, used_count {}", (void*)(cell->chunk.get()), (void*)cell, cell->used_count);
    auto result = cell->chunk;

    _update_progress(iter);
    LOG(INFO) << fmt::format("pop chunk, rows {}, used_count {}, index {}, consumer numer {}, {}, load bytes {}, load rows {}",
        cell->accumulated_rows, cell->used_count, consumer_index, _consumer_number, (void*)(result.get()), _current_load_bytes, _current_load_rows);
    result->check_or_die();
    return result;
}

void MemLimitedChunkQueue::evict_loaded_block() {
    // evict by used count?
    if (_loaded_blocks.size() < _consumer_number) {
        return;
    }
    Block* block = _loaded_blocks.front();
    _loaded_blocks.pop();
    LOG(INFO) << fmt::format("evict block {}, rows {}", (void*)block, block->flush_rows);
    _current_load_rows -= block->flush_rows;
    _current_load_bytes -= block->flush_bytes;
    for (auto& cell : block->cells) {
        if (cell.chunk != nullptr) {
            cell.chunk.reset();
        }
    }
    block->in_mem = false;
}

bool MemLimitedChunkQueue::can_pop(int32_t consumer_index) {
    DCHECK(consumer_index < _consumer_number);
    std::unique_lock l(_mutex);
    DCHECK(_consumer_progress[consumer_index] != nullptr);
    if (_opened_sink_number == 0) {
        return true;
    }
    // @TODO can_push -> can_pop -> flush -> pull_chunk
    // @TODO in_mem = true -> true -> false - > false
    auto iter = _consumer_progress[consumer_index].get();
    if (iter->has_next()) {
        auto next_iter = iter->next();
        // @TODO what if flushed when read
        if (next_iter.block->in_mem) {
            return true;
        }
        TEST_SYNC_POINT_CALLBACK("MemLimitedChunkQueue::can_pop::before_submit_load_task", next_iter.block);
        if (!next_iter.block->has_load_task) {
            LOG(INFO) << "block is not in memory, should trigger load io task for index "<< consumer_index << ", block " << (void*)(next_iter.block);
            next_iter.block->has_load_task = true;
            auto status = submit_load_task(next_iter.block);
            if (!status.ok()) {
                _io_task_status = status;
                return true;
            }
        }
        // @TODO should trigger a load io task
        return false;
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
    LOG(INFO) << "close consumer: " << consumer_index << ", opcount " << _opened_source_opcount[consumer_index];
    if (_opened_source_opcount[consumer_index] == 0) {
        _opened_source_number--;
        // @TODO
        _close_consumer(consumer_index);
    }
}

void MemLimitedChunkQueue::open_producer() {
    std::unique_lock l(_mutex);
    _opened_sink_number++;
    LOG(INFO) << "open producer: " << _opened_sink_number;
}

void MemLimitedChunkQueue::close_producer() {
    std::unique_lock l(_mutex);
    _opened_sink_number--;
    LOG(INFO) << "close producer: " << _opened_sink_number;
}

void MemLimitedChunkQueue::_update_progress(Iterator* iter) {
    if(iter != nullptr) {
        Cell* cell = iter->get_cell();
        _fastest_accumulated_rows = std::max(_fastest_accumulated_rows, cell->accumulated_rows);
        _fastest_accumulated_bytes = std::max(_fastest_accumulated_bytes, cell->accumulated_bytes);
    } else {
        // nullptr means one consumer is closed, should update
        _fastest_accumulated_rows = 0;
        _fastest_accumulated_bytes = 0;
        for (int32_t i = 0;i < _consumer_number;i++) {
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

    // @TODO mark all used cell and release memory
    while (_iterator.valid()) {
        Cell* cell = _iterator.get_cell();
        if(cell->used_count == _consumer_number) {
            _head_accumulated_rows = cell->accumulated_rows;
            _head_accumulated_bytes = cell->accumulated_bytes;

            // advance flushed position
            _flushed_accumulated_rows = std::max(_flushed_accumulated_rows, _head_accumulated_rows);
            _flushed_accumulated_bytes = std::max(_flushed_accumulated_bytes, _head_accumulated_bytes);
            // @TODO may be reset twice
            LOG(INFO) << fmt::format("release chunk, rows {}, mem {}, head acc rows {}, bytes {} ref {}",
                cell->accumulated_rows,(cell->chunk != nullptr ? cell->chunk->memory_usage(): 0),
                _head_accumulated_rows, _head_accumulated_bytes, cell->chunk.use_count());
            cell->chunk.reset();
            if(_iterator.has_next()) {
                LOG(INFO) << fmt::format("no next, skip clean");
                _iterator.move_to_next();
            } else {
                break;
            }
        } else {
            break;
        }
    }

    // @TODO head is dummy, should handle dummy

    // @TODO we should update _head
    /*
    while (_head) {
        if (_head->cells.empty()) {
            LOG(INFO) << "empty head, not expected";
            break;
        }
        size_t last_acc_rows = _head->cells.back().accumulated_rows;
        if (last_acc_rows > _head_accumulated_rows) {
            // not all cells are consumed
            LOG(INFO) << fmt::format("last acc rows {}, head {}, skip gc", last_acc_rows, _head_accumulated_rows);
            break;
        }
        Block* next = _head->next;
        if (next == nullptr) {
            break;
        }
        LOG(INFO) << fmt::format("delete head, rows {}", last_acc_rows);
        delete _head;
        _head = next;   
    }
    */
}

void MemLimitedChunkQueue::_close_consumer(int32_t consumer_index) {
    LOG(INFO) << fmt::format("close consumer, index: {}", consumer_index);
    DCHECK(_consumer_progress[consumer_index] != nullptr);
    auto iter = _consumer_progress[consumer_index].get();
    if (iter->has_next()) {
        do {
            iter->move_to_next();
            Cell* cell = iter->get_cell();
            cell->used_count += 1;
            LOG(INFO) << fmt::format("update cell, rows {}, used count{}", cell->accumulated_rows, cell->used_count);
        } while (iter->has_next());
    }
    _consumer_progress[consumer_index].reset();
    _update_progress();
}

Status MemLimitedChunkQueue::flush() {
    // @TODO opt lock
    LOG(INFO) << "invoke flush";
    std::unique_lock l(_mutex);

    // @block maybe in mem becasue of load
    // DCHECK(block->block == nullptr) << "block should not be flushed, " << (void*)(block);

    // 1. find block to flush
    LOG(INFO) << "find block to flush";
    while (_next_flush_block) {
        // don't flush the last block
        if (_next_flush_block->next == nullptr) {
            break;
        }
        if (_next_flush_block->block) {
            // already flushed, should skip it
            _next_flush_block = _next_flush_block->next;
        } else {
            // check if need flush
            bool need_flush = false;
            for (auto& cell : _next_flush_block->cells) {
                if (cell.chunk == nullptr || cell.used_count == _consumer_number) {
                    continue;
                }
                need_flush = true;
            }
            if (need_flush) {
                break;
            }
            _next_flush_block = _next_flush_block->next;
        }
    }
    LOG(INFO) << "find block done";
    Block* block = _next_flush_block;
    DCHECK(block != nullptr) << "block can't be null";
    if (block->next == nullptr) {
        LOG(INFO) << "last block, skip flush..." << (void*)(block);
        return Status::OK();
    }

    LOG(INFO) << "begin flush block, " << (void*)(block);
    if (block->has_load_task) {
        LOG(INFO) << "block has load task, we should skip this flush, " << (void*)(block);
        block->has_flush_task = false;
        return Status::OK();
    }
    if (block->block != nullptr) {
        LOG(INFO) << "block is already flushed, skip it..." << (void*)(block);
        return Status::OK();
    }

    size_t max_serialize_size = 0;
    // @TODO advance non_flushed_block
    // current block that pending consumed
    // @TODO find target block
    // block = _next_flush_block;
    // @TODO should advance non-flushed block
    if (!block->in_mem) {
        LOG(INFO) << "block already flushed, skip..." << (void*)(block);
        return Status::OK();
    }
    // @TODO offset maybe missed because chunk will be release after consumed???
    LOG(INFO) << "flush block " << (void*)(block) << ", cell num: " << block->cells.size()
        << ", rows " << block->cells.front().accumulated_rows << "," << block->cells.back().accumulated_rows;
    // 1. calculate max_serialize_size
    for(auto& cell : block->cells) {
        if (cell.chunk == nullptr || cell.used_count == _consumer_number) {
            // only dummy cell has empty chunk
            // DCHECK(cell.accumulated_rows == 0);
            continue;
        }
        // @TODO if used count == 3
        auto& chunk = cell.chunk;
        for (const auto& column: chunk->columns()) {
            max_serialize_size += serde::ColumnArraySerde::max_serialized_size(*column);
        } 
        block->flush_rows += chunk->num_rows();
        block->flush_bytes += chunk->memory_usage();
    }
    TEST_SYNC_POINT_CALLBACK("MemLimitedChunkQueue::after_calculate_max_serialize_size", &max_serialize_size);
    if (max_serialize_size > 0) {
        raw::RawString serialize_buffer;
        serialize_buffer.resize(max_serialize_size);
        uint8_t* buf = reinterpret_cast<uint8_t*>(serialize_buffer.data());
        uint8_t* begin = buf;
        // 2. serialize data
        for (auto& cell : block->cells) {
            if (cell.chunk == nullptr) {
                continue;
            }
            auto chunk = cell.chunk;
            for (const auto& column: chunk->columns()) {
                buf = serde::ColumnArraySerde::serialize(*column, buf, false);
                if (UNLIKELY(buf == nullptr)) {
                    return Status::InternalError("serialize data error");
                }
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
        // @TODO set plan node id
        options.plan_node_id = 9999;
        LOG(INFO) << "block size: " << content_length;
        ASSIGN_OR_RETURN(block->block, _opts.block_manager->acquire_block(options));
        LOG(INFO) << "spill block: " << block->block->debug_string();

        // 4. flush serialized data
        std::vector<Slice> data;
        data.emplace_back(serialize_buffer.data(), content_length);
        RETURN_IF_ERROR(block->block->append(data));
        RETURN_IF_ERROR(block->block->flush());

        // 5. clear data in memory
        for (auto& cell : block->cells) {
            // @TODO skip all used data
            if (cell.chunk == nullptr || cell.used_count == _consumer_number) {
                continue;
            }
            // reset chunk data
            LOG(INFO) << fmt::format("flush chunk, rows {}, used_count {}, num_rows {} {} ref {}",
                cell.accumulated_rows, cell.used_count, cell.chunk->num_rows(), (void*)(cell.chunk.get()), cell.chunk.use_count());
            // @TODO clear in memory data

            // cell.chunk->reset();
            cell.chunk.reset();
        }
    }
    TEST_SYNC_POINT_CALLBACK("MemLimitedChunkQueue::after_flush_block", block);
    block->in_mem = false;
    _flushed_accumulated_rows = block->cells.back().accumulated_rows;
    _flushed_accumulated_bytes = block->cells.back().accumulated_bytes;
    _next_flush_block = block->next;
    LOG(INFO) << fmt::format("flush block done, block[{}], flushed_acc_rows[{}], flushed_acc_bytes[{}]",
        (void*)block, _flushed_accumulated_rows, _flushed_accumulated_bytes);
    //@TODO should update in memory bytes???
    return Status::OK();
}

Status MemLimitedChunkQueue::submit_flush_task() {
    // @TODO should consider be ut mode
    auto flush_task = [this, guard = RESOURCE_TLS_MEMTRACER_GUARD(_state)] (auto& yield_ctx) {
        TEST_SYNC_POINT("MemLimitedChunkQueue::before_execute_flush_task");
        RETURN_IF(!guard.scoped_begin(), (void)0);
        DEFER_GUARD_END(guard);
        auto defer = DeferOp([&] () {
            _has_flush_io_task = false;
            TEST_SYNC_POINT("MemLimitedChunkQueue::after_execute_flush_task");
        });

        // find next flushed block
        // Block* block = _iterator.get_block();

        // DCHECK(_next_flush_block->next != nullptr) << "last block should not flushed";
        // @TODO should find next flushed block
        auto status = flush();
        WARN_IF_ERROR(status, "flush block error");
        if (!status.ok()) {
            std::unique_lock l(_mutex);
            _io_task_status = status;
            return;
        }
        // @TODO what if next is null
        // _next_flush_block = _next_flush_block->next;
    };

    auto io_task = workgroup::ScanTask(_state->fragment_ctx()->workgroup().get(), std::move(flush_task));
    RETURN_IF_ERROR(spill::IOTaskExecutor::submit(std::move(io_task)));
    return Status::OK();
}

// reload block from disk
Status MemLimitedChunkQueue::load(Block* block) {
    // @TODO before load, should evict already loaded block, not maintain LRU
    // @TODO optimize lock
    std::unique_lock l(_mutex);
    LOG(INFO) << "load block begin " << (void*)(block);
    DCHECK(block->has_load_task) << "block should not have load task " << (void*)(block);
    DCHECK(!block->in_mem) << "block should not be in memory, " << (void*)(block) ;
    DCHECK(block->block != nullptr) << "block must have spill block";

    // 1. read serialized data
    spill::BlockReaderOptions options;
    options.enable_buffer_read = true;
    options.read_io_timer = _read_io_timer;
    options.read_io_count = _read_io_count;
    options.read_io_bytes = _read_io_bytes;
    // @TODO update spill profile
    auto block_reader = block->block->get_reader(options);
    size_t block_len = block->block->size();
    raw::RawString buffer;
    buffer.resize(block_len);
    RETURN_IF_ERROR(block_reader->read_fully(buffer.data(), block_len));

    // 2. deserialize data
    uint8_t* buf = reinterpret_cast<uint8_t*>(buffer.data());
    const uint8_t* read_cursor = buf;

    // @TODO should mark which cell is flushed??
    for (auto& cell : block->cells) {
        if (cell.used_count == _consumer_number || cell.accumulated_rows == 0) {
            continue;
        }
        // auto chunk = cell.chunk;
        cell.chunk = _chunk_builder->clone_empty();
        auto& chunk = cell.chunk;
        LOG(INFO) << fmt::format("load chunk begin, rows {}, used_count {}", cell.accumulated_rows, cell.used_count);
        LOG(INFO) << "load chunk begin, rows " << cell.accumulated_rows << ", used_count " << cell.used_count << ", " << (void*)(cell.chunk.get());
        for (auto& column: chunk->columns()) {
            read_cursor = serde::ColumnArraySerde::deserialize(read_cursor, column.get(), false);
            DCHECK(read_cursor != nullptr) << "deserialize error";
        }
        chunk->check_or_die();
        LOG(INFO) << "load chunk done, rows " << cell.accumulated_rows << ", used_count " << cell.used_count << ", " << (void*)(cell.chunk.get()) << ", " << chunk->num_rows();
    }
    // @TODO verify??
    block->in_mem = true;
    block->has_load_task = false;
    evict_loaded_block();
    _loaded_blocks.push(block);
    _current_load_rows += block->flush_rows;
    _current_load_bytes += block->flush_bytes;
    LOG(INFO) << fmt::format("load block done, block[{}], load rows[{}], load bytes[{}]", (void*)block, _current_load_rows, _current_load_bytes);

    return Status::OK();
}

Status MemLimitedChunkQueue::submit_load_task(Block* block) {
    auto load_task = [this, block, guard = RESOURCE_TLS_MEMTRACER_GUARD(_state)] (auto& yield_ctx) {
        TEST_SYNC_POINT_CALLBACK("MemLimitedChunkQueue::before_execute_load_task", block);
        RETURN_IF(!guard.scoped_begin(), (void)0);
        DEFER_GUARD_END(guard);
        // @TODO
        auto status = load(block);
        if (!status.ok()) {
            std::unique_lock l(_mutex);
            _io_task_status = status;
        }
    };
    auto io_task = workgroup::ScanTask(_state->fragment_ctx()->workgroup().get(), std::move(load_task));
    RETURN_IF_ERROR(spill::IOTaskExecutor::submit(std::move(io_task)));
    return Status::OK();
}

}