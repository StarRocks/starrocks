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

#include <glog/logging.h>
#include <memory>
#include "common/compiler_util.h"
#include "common/status.h"
#include "exec/pipeline/exchange/multi_cast_local_exchange.h"

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

namespace starrocks::pipeline {

class MemLimitedChunkQueue {
private:
    struct Cell {
        ChunkPtr chunk;
        size_t accumulated_rows = 0;
        size_t accumulated_bytes = 0;
        int32_t used_count = 0;
    };
    // won't be very large
    struct Block {
        // still need a link
        std::vector<Cell> cells;
        bool in_mem = true;
        bool has_load_task = false;
        bool has_flush_task = false;
        // @TODO no need, use last - first
        size_t memory_usage = 0;
        // memory
        Block* next = nullptr;
        // spillable block, only used when spill happens
        spill::BlockPtr block;
        // @TODO should record read ref??
    };

    struct Iterator {
        Iterator() = default;
        Iterator(Block* blk, size_t idx): block(blk), index(idx) {}
        Block* block = nullptr;
        // @TODO canot use index as internal iterator
        size_t index = 0;

        bool valid() const {
            return block != nullptr && index < block->cells.size();
        }

        //@TODO how to diff empty and end
        bool has_next() const {
            if (block == nullptr) {
                return false;
            }
            if (index + 1 == block->cells.size()) {
                // current block is end, should check next
                if (block->next != nullptr && !block->next->cells.empty()) {
                    return true;
                }
                return false;
            }
            // still has un-consumed data in current block
            return true;
        }

        // return an copy of next cell
        Iterator next() const {
            DCHECK(has_next());
            Iterator iter(block, index);
            iter.move_to_next();
            return iter;
        }
        void move_to_next() {
            DCHECK(has_next());
            index++;
            DCHECK_LE(index, block->cells.size());
            if (index == block->cells.size()) {
                block = block->next;
                index = 0;
            }
        }

        Cell* get_cell() {
            DCHECK(block != nullptr);
            DCHECK(index < block->cells.size()) << ", invalid index: " << index << ", size: " << block->cells.size();
            return &(block->cells[index]);
        }

        Block* get_block() {
            return block;
        }
    };
public:
    struct Options {
        // use this to query
        std::string name = "multi_cast_local_exchange";
        int32_t plan_node_id = 0;

        // memory block size
        size_t block_size = 1024 * 1024 * 2;
        CompressionTypePB compress_type = CompressionTypePB::LZ4;
        spill::BlockManager* block_manager = nullptr;
        workgroup::WorkGroupPtr wg;
    };
    // @TODO mem control strategy
    // 1. for each consumer, only keep one Block in memory
    // 2. for producer, keep on block in memory

    MemLimitedChunkQueue(RuntimeState* state, RuntimeProfile* profile, int32_t consumer_number, Options opts):
        _state(state), _consumer_number(consumer_number), _opened_source_opcount(consumer_number), _consumer_progress(consumer_number), _opts(std::move(opts)) {
        // what if no dummy block
        Block* block = new Block();
        block->next = nullptr;
        _head = block;
        _tail = block;
        _non_flushed_block = block;
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
    }

    Status push(const ChunkPtr& chunk, MultiCastLocalExchangeSinkOperator* sink_operator) {
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

        // @TODO in_memory_rows = total_acc_rows - flushed_rows + loaded_rows;
        // @TODO should consider load
        size_t in_memory_rows = _total_accumulated_rows - _flushed_accumulated_rows;
        size_t in_memory_bytes = _total_accumulated_bytes - _flushed_accumulated_bytes;
        _peak_memory_rows_counter->set(in_memory_rows);
        _peak_memory_bytes_counter->set(in_memory_bytes);
        sink_operator->update_counter(in_memory_bytes, in_memory_rows); 
        LOG(INFO) << fmt::format("push chunk to block {}, rows {}, bytes {}, cells {}, mem {}, in_mem_rows {}, in_mem_bytes {}",
            (void*)(_tail), _total_accumulated_rows, _total_accumulated_rows, _tail->cells.size(), _tail->memory_usage, in_memory_rows, in_memory_bytes);

        return Status::OK();
    }

    bool can_push() {
        // @TODO add some limit
        // 1. _total_accumulated_bytes - _fatest_accmulated_bytes >= limit, can't push
        // 2. if _total_accumulated_bytes - _flushed_accumulated_bytes >= limit, should trigger a flush io task
        std::unique_lock l(_mutex);
        const size_t unconsumed_limit = 1024 * 1024;
        size_t unconsumed_bytes = _total_accumulated_bytes - _fastest_accumulated_bytes;
        if (unconsumed_bytes >= unconsumed_limit) {
            LOG(INFO) << fmt::format("too many pending data, should wait {}", unconsumed_bytes);
            return false;
        }

        // size_t in_memory_bytes = _total_accumulated_bytes - _head_accumulated_bytes;
        size_t in_memory_bytes = _total_accumulated_bytes - _flushed_accumulated_bytes;
        // flush all data
        const size_t bytes_limit = 1024 * 1024;
        if (in_memory_bytes >= bytes_limit && _non_flushed_block->next != nullptr) {
            LOG(INFO) << fmt::format("too many in memory data, should flush {}", in_memory_bytes);
            if (!_has_flush_io_task) {
                // @TODO choose block
                // pick block that need flush
                auto status = submit_flush_task();
                if (!status.ok()) {
                    _io_task_status = status;
                    return true;
                }
                _has_flush_io_task = true;
            } else {
                LOG(INFO) << fmt::format("already has flush io task, skip trigger...");
            }
            return false;
        }

        return true;
    }

    StatusOr<ChunkPtr> pop(int32_t consumer_index) {
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
        cell->used_count += 1;
        // @TODO update progress

        LOG(INFO) << fmt::format("pop chunk, {} ref {}", (void*)(cell->chunk.get()), cell->chunk.use_count());
        auto result = cell->chunk;

        _update_progress(iter);
        LOG(INFO) << fmt::format("pop chunk, rows {}, used_count {}, index {}, consumer numer {}, {}",
            cell->accumulated_rows, cell->used_count, consumer_index, _consumer_number, (void*)(result.get()));
        result->check_or_die();
        return result;
    }

    bool can_pop(int32_t consumer_index) {
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

    void open_consumer(int32_t consumer_index) {
        std::unique_lock l(_mutex);
        if (_opened_source_opcount[consumer_index] == 0) {
            _opened_source_number++;
        }
        _opened_source_opcount[consumer_index]++;
    }
    void close_consumer(int32_t consumer_index) {
        std::unique_lock l(_mutex);
        _opened_source_opcount[consumer_index] -= 1;
        LOG(INFO) << "close consumer: " << consumer_index << ", opcount " << _opened_source_opcount[consumer_index];
        if (_opened_source_opcount[consumer_index] == 0) {
            _opened_source_number--;
            // @TODO
            _close_consumer(consumer_index);
        }
    }
    void open_producer() {
        std::unique_lock l(_mutex);
        _opened_sink_number++;
        LOG(INFO) << "open producer: " << _opened_sink_number;
    }
    void close_producer() {
        std::unique_lock l(_mutex);
        _opened_sink_number--;
        LOG(INFO) << "close producer: " << _opened_sink_number;
    }

private:
    void _update_progress(Iterator* iter = nullptr) {
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
    void _close_consumer(int32_t consumer_index) {
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


    RuntimeState* _state = nullptr;
    std::mutex _mutex;
    // an empty chunk, only keep meta
    ChunkPtr _chunk_builder;
    Block* _head = nullptr;
    Block* _tail = nullptr;

    Block* _non_flushed_block = nullptr;
    // an iterator point to head;
    Iterator _iterator;

    int32_t _consumer_number;
    int32_t _opened_source_number = 0;
    int32_t _opened_sink_number = 0;
    std::vector<int32_t> _opened_source_opcount;
    // record consume progress

    std::vector<std::unique_ptr<Iterator>> _consumer_progress;

    // all 
    size_t _total_accumulated_rows = 0;
    size_t _total_accumulated_bytes = 0;
    // head
    size_t _head_accumulated_rows = 0;
    size_t _head_accumulated_bytes = 0;

    // not flushed
    size_t _flushed_accumulated_rows = 0;
    size_t _flushed_accumulated_bytes = 0;

    // consumer
    size_t _fastest_accumulated_rows = 0;
    size_t _fastest_accumulated_bytes = 0;

    size_t _current_load_rows = 0;
    size_t _current_load_bytes = 0;

    // spill related
    Options _opts;
    Status _io_task_status;

    bool _has_flush_io_task = false;


    Status flush() {
        // @TODO opt lock
        std::unique_lock l(_mutex);

        // @block maybe in mem becasue of load
        // DCHECK(block->block == nullptr) << "block should not be flushed, " << (void*)(block);

        // 1. find block to flush
        while (_non_flushed_block) {
            // don't flush the last block
            if (_non_flushed_block->next == nullptr) {
                break;
            }
            if (_non_flushed_block->block) {
                // already flushed, should skip it
                _non_flushed_block = _non_flushed_block->next;
            } else {
                // check if need flush
                bool need_flush = false;
                for (auto& cell : _non_flushed_block->cells) {
                    if (cell.chunk == nullptr || cell.used_count == _consumer_number) {
                        continue;
                    }
                    need_flush = true;
                }
                if (need_flush) {
                    break;
                }
                _non_flushed_block = _non_flushed_block->next;
            }
        }
        Block* block = _non_flushed_block;
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
        // block = _non_flushed_block;
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
        }
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
            // @TODO reset
            // @TODO keep chunk builder
            // cell.chunk->reset();
            // @TODO update memory usage
        }
        block->in_mem = false;
        LOG(INFO) << "flush block done, " << (void*)(block);
        return Status::OK();
    }

    // trigger flush task
    Status submit_flush_task() {
        auto flush_task = [this, guard = RESOURCE_TLS_MEMTRACER_GUARD(_state)] (auto& yield_ctx) {
            RETURN_IF(!guard.scoped_begin(), (void)0);
            DEFER_GUARD_END(guard);
            auto defer = DeferOp([&] () {
                std::unique_lock l(_mutex);
                _has_flush_io_task = false;
            });

            // find next flushed block
            // Block* block = _iterator.get_block();

            // DCHECK(_non_flushed_block->next != nullptr) << "last block should not flushed";
            // @TODO should find next flushed block
            auto status = flush();
            WARN_IF_ERROR(status, "flush block error");
            if (!status.ok()) {
                std::unique_lock l(_mutex);
                _io_task_status = status;
                return;
            }
            // @TODO what if next is null
            // _non_flushed_block = _non_flushed_block->next;
        };

        auto io_task = workgroup::ScanTask(_state->fragment_ctx()->workgroup().get(), std::move(flush_task));
        RETURN_IF_ERROR(spill::IOTaskExecutor::submit(std::move(io_task)));
        return Status::OK();
    }

    // reload block from disk
    Status load(Block* block) {
        std::unique_lock l(_mutex);
        LOG(INFO) << "load block begin " << (void*)(block);
        DCHECK(block->has_load_task) << "block should not have load task " << (void*)(block);
        DCHECK(!block->in_mem) << "block should not be in memory, " << (void*)(block) ;
        DCHECK(block->block != nullptr) << "block must have spill block";

        // 1. read serialized data
        auto block_reader = block->block->get_reader();
        size_t block_len = block->block->size();
        raw::RawString buffer;
        buffer.resize(block_len);
        RETURN_IF_ERROR(block_reader->read_fully(buffer.data(), block_len));

        // 2. deserialize data
        uint8_t* buf = reinterpret_cast<uint8_t*>(buffer.data());
        const uint8_t* read_cursor = buf;

        // @TODO should mark which cell is flushed??
        for (auto& cell : block->cells) {
            if (cell.chunk == nullptr || cell.used_count == _consumer_number) {
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
            _current_load_rows += chunk->num_rows();
            _current_load_bytes += chunk->memory_usage();
            LOG(INFO) << "load chunk done, rows " << cell.accumulated_rows << ", used_count " << cell.used_count << ", " << (void*)(cell.chunk.get()) << ", " << chunk->num_rows();
        }
        // @TODO verify??
        block->in_mem = true;
        block->has_load_task = false;
        LOG(INFO) << "load block done " << (void*)(block);

        return Status::OK();
    }

    Status submit_load_task(Block* block) {
        auto load_task = [this, block, guard = RESOURCE_TLS_MEMTRACER_GUARD(_state)] (auto& yield_ctx) {
            RETURN_IF(!guard.scoped_begin(), (void)0);
            DEFER_GUARD_END(guard);
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



    RuntimeProfile::HighWaterMarkCounter* _peak_memory_bytes_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_memory_rows_counter = nullptr;
};





SpillableMultiCastLocalExchanger::SpillableMultiCastLocalExchanger(RuntimeState* runtime_state, size_t consumer_number, int64_t memory_limit)
    : _runtime_state(runtime_state), _consumer_number(consumer_number), _progress(consumer_number), _opened_source_opcount(consumer_number), _has_load_io_task(consumer_number) {
    Cell* dummy = new Cell();
    _head = dummy;
    _tail = dummy;
    for (size_t i = 0; i < consumer_number; i++) {
        // _progress[i] = _cells.begin();
        _progress[i] = _tail;
        _opened_source_opcount[i] = 0;
        _has_load_io_task[i] = false;
    }

    MemLimitedChunkQueue::Options opts;
    // @TODO init opts
    opts.block_manager = runtime_state->query_ctx()->spill_manager()->block_manager();

    _runtime_profile = std::make_unique<RuntimeProfile>("SpillableMultiCastLocalExchanger");
    _queue = std::make_shared<MemLimitedChunkQueue>(runtime_state, _runtime_profile.get(), consumer_number, opts);
    _peak_memory_usage_counter = _runtime_profile->AddHighWaterMarkCounter(
            "PeakMemoryUsage", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    _peak_buffer_row_size_counter = _runtime_profile->AddHighWaterMarkCounter(
            "PeakBufferRowSize", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));

}
SpillableMultiCastLocalExchanger::~SpillableMultiCastLocalExchanger() {

    // while(_head) {
    //     auto* t = _head->next;
    //     _current_memory_usage -= _head->memory_usage;
    //     delete _head;
    //     _head = t;
    // }
}

bool SpillableMultiCastLocalExchanger::can_pull_chunk(int32_t mcast_consumer_index) const {
    return _queue->can_pop(mcast_consumer_index);
/*
    DCHECK(mcast_consumer_index < _consumer_number);

    std::unique_lock l(_mutex);
    DCHECK(_progress[mcast_consumer_index] != nullptr);
    if (_opened_sink_number == 0) return true;
    auto* cell = _progress[mcast_consumer_index];
    if (cell->next != nullptr) {
        // @TODO if cell not in memory, need trigger an io task
        // @TODO should main an file
        // @TODO each consumer has at most one io task to read data
        if (cell->next->in_mem) {
            return true;
        }
        if (!_has_load_io_task[mcast_consumer_index]) {
            LOG(INFO) << "cell not in memory, trigger load io task, index: " << mcast_consumer_index << ", cell:" << cell->next->accumulated_row_size;
            // @TODO trigger a load task
            auto load_task =  [this, mcast_consumer_index, guard = RESOURCE_TLS_MEMTRACER_GUARD(_runtime_state)] (auto& yield_ctx) {
                // @TODO should know read witch data
                RETURN_IF(!guard.scoped_begin(), (void)0);
                DEFER_GUARD_END(guard);
                // @TODO know offset
                auto* cell = _progress[mcast_consumer_index];
                DCHECK(cell != nullptr && cell->next != nullptr);
                size_t offset = cell->next->offset;
                size_t length = cell->next->length;
                LOG(INFO) << "reload offset " << offset << ", len " << length;
                // after load, how to ecvit?
                // @TODO need a lru cache???
                // std::string file_name = fmt::format("{}/{}/{}-{}-{}", dir->dir(), print_id(_runtime_state->query_id()),
                //         print_id(_runtime_state->fragment_instance_id()), "mcast_local_exchange", std::to_string(reinterpret_cast<uintptr_t>(this)));
                    // @TODO should know spill dir?
                auto file_st = _dir->fs()->new_random_access_file(_file_name);
                if (!file_st.ok()) {
                    LOG(INFO) << "create read file error: " << file_st.status().to_string();
                    return;
                }
                auto read_file = std::move(file_st.value());
                WARN_IF_ERROR(read_file->seek(offset),  "seek error");
                std::string buffer;
                buffer.resize(length);
                WARN_IF_ERROR(read_file->read(buffer.data(), length), "read error");

                // @TODO need build an empty chunk, used to deserialize
                // reset chnk
                // deserialize chunk
                auto& columns = cell->next->chunk->columns();
                const uint8_t* buf = reinterpret_cast<uint8_t*>(buffer.data());
                for (size_t i = 0;i < columns.size();i++) {
                    buf = serde::ColumnArraySerde::deserialize(buf, columns[i].get(), false);
                }
                LOG(INFO) << "reload chunk, " << cell->next->chunk->num_rows() << ", cell: " << cell->next->accumulated_row_size;
                cell->next->in_mem = true;
                _has_load_io_task[mcast_consumer_index] = false;
            };

            auto io_task = workgroup::ScanTask(_runtime_state->fragment_ctx()->workgroup().get(), std::move(load_task));
            WARN_IF_ERROR(spill::IOTaskExecutor::submit(std::move(io_task)), "submit io task error");
            _has_load_io_task[mcast_consumer_index] = true;
            return false;
        } 
        return false;
    }
    // @TOODO
    return false;
*/
}

bool SpillableMultiCastLocalExchanger::can_push_chunk() const {
    return _queue->can_push();
/*
    std::unique_lock l(_mutex);
    // if for the fastest consumer, the exchanger still has enough chunk to be consumed.
    // the exchanger does not need any input.
    if (_current_memory_usage >= _memory_limit) {
        // if no io task is triggered, submit an io task
        // flush all data in [start, end)
        if (!_has_flush_io_task) {
            
            auto flush_task = [this, guard = RESOURCE_TLS_MEMTRACER_GUARD(_runtime_state)] (auto& yield_ctx) {
                RETURN_IF(!guard.scoped_begin(), (void)0);
                DEFER_GUARD_END(guard);
                // @TODO should record flush cont

                // @TODO maybe we can't use file directly since it maybe very large
                // @TODO should we maintain multi blocks?
                // block->block->block
                if (_file == nullptr) {
                    // create file first
                    auto dir_mgr = ExecEnv::GetInstance()->spill_dir_mgr();
                    spill::AcquireDirOptions opts;
                    // @TODO we should acquired dir first
                    auto dir_st = dir_mgr->acquire_writable_dir(opts);
                    if (!dir_st.ok()) {
                        LOG(INFO) << "acqure dir file: " << dir_st.status().to_string();
                    }
                    _dir = dir_st.value();
                    // @TODO should know seq
                    std::string dir_path = fmt::format("{}/{}", _dir->dir(), print_id(_runtime_state->query_id()));
                    WARN_IF_ERROR(_dir->fs()->create_dir_if_missing(dir_path), "create dir error");
                    _file_name = fmt::format("{}/{}/{}-{}-{}", _dir->dir(), print_id(_runtime_state->query_id()),
                        print_id(_runtime_state->fragment_instance_id()), "mcast_local_exchange", std::to_string(reinterpret_cast<uintptr_t>(this)));
                    // @TODO should know spill dir?
                    WritableFileOptions opt;
                    opt.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
                    // @TODO should pass io task status
                    if (auto st = _dir->fs()->new_writable_file(opt, _file_name);!st.ok()) {
                        LOG(INFO) << "create file error, " << _file_name << ", " << st.status().to_string();
                    } else {
                        _file = std::move(st.value());
                    }
                    // _flush_point = _head;
                }
                LOG(INFO) << "before flush, file size: " << _file->size();
                std::unique_lock l(_mutex);
                std::vector<Slice> data;
                size_t max_size = 0;
                if (_flush_point == nullptr) {
                    _flush_point = _head;
                }
                Cell* begin = _flush_point;
                Cell* end = _flush_point;
                while (end != _tail) {
                    if(end->chunk == nullptr || end->used_count == _consumer_number) {
                        LOG(INFO) << "skip chunk";
                        end = end->next;
                        continue;
                    }
                    auto chunk = end->chunk;
                    for (const auto& column: chunk->columns()) {
                        max_size += serde::ColumnArraySerde::max_serialized_size(*column);
                    }
                    end = end->next;
                }
                LOG(INFO) << "flush point: " << _flush_point->accumulated_row_size;
                raw::RawString serialize_buffer;
                serialize_buffer.resize(max_size);
                uint8_t* buf = reinterpret_cast<uint8_t*>(serialize_buffer.data());
                size_t start_offset = _file->size();
                for (Cell* c = begin; c != end; c = c->next) {
                    if (c->chunk == nullptr || c->used_count == _consumer_number) {
                        continue;
                    }
                    auto chunk = c->chunk;
                    uint8_t* begin = buf;
                    for (const auto& column: chunk->columns()) {
                        buf = serde::ColumnArraySerde::serialize(*column, buf, false);
                        if (UNLIKELY(buf == nullptr)) {
                            LOG(INFO) << "serialize error";
                        }
                    }
                    size_t content_length = buf - begin;
                    data.emplace_back(begin, content_length);
                    // @TODO should maintain logical offset sinice we may put data into multi file
                    _current_memory_usage -= c->chunk->memory_usage();
                    c->chunk->reset();
                    c->offset = start_offset;
                    c->length = content_length;
                    c->in_mem = false;
                    LOG(INFO) << "serialize chunk, offset:" << start_offset << ", len:" << content_length <<", row " << c->accumulated_row_size
                        << ", cur mem: " << _current_memory_usage << ", use count: " << c->used_count;
                    start_offset += content_length;
                }
                WARN_IF_ERROR(_file->appendv(data.data(), data.size()), "append error");
                _flush_point = end;
                _has_flush_io_task = false;
            };

            auto io_task = workgroup::ScanTask(_runtime_state->fragment_ctx()->workgroup().get(), std::move(flush_task));
            WARN_IF_ERROR(spill::IOTaskExecutor::submit(std::move(io_task)), "submit io task error");
            _has_flush_io_task = true;
        }
        return false;
    }

    return true;
*/
}

Status SpillableMultiCastLocalExchanger::push_chunk(const ChunkPtr& chunk, int32_t sink_driver_sequence, MultiCastLocalExchangeSinkOperator* sink_operator) {
    return _queue->push(chunk, sink_operator);
/*
    Cell* cell = new Cell();
    cell->chunk = chunk;
    cell->memory_usage = chunk->memory_usage();
    {
        std::unique_lock l(_mutex);

        int32_t closed_source_numer = _consumer_number - _opened_source_number;

        cell->used_count = closed_source_numer;
        cell->accumulated_row_size = _current_accumulated_row_size;;
        cell->next = nullptr;

        _tail->next = cell;
        _tail = cell;
        _current_accumulated_row_size += chunk->num_rows();
        LOG(INFO) << "add chunk, mem: " << cell->memory_usage << ", new mem: " << _current_memory_usage << ", rows: " << cell->accumulated_row_size;
        _current_memory_usage += cell->memory_usage;
        _current_row_size += chunk->num_rows();
        // _current_row_size = _current_accumulated_row_size - _head->accumulated_row_size;

        // _cells.emplace_back(cell);
        // _last_index++;
        _peak_memory_usage_counter->set(_current_memory_usage);
        _peak_buffer_row_size_counter->set(_current_row_size);
        sink_operator->update_counter(_current_memory_usage, _current_row_size);
    }

    return Status::OK();
*/
}

StatusOr<ChunkPtr> SpillableMultiCastLocalExchanger::pull_chunk(RuntimeState* state, int32_t mcast_consumer_index) {
    return _queue->pop(mcast_consumer_index);

/*
    DCHECK(mcast_consumer_index < _consumer_number);

    std::unique_lock l(_mutex);
    // DCHECK(_progress[mcast_consumer_index] != _cells.end());
    DCHECK(_progress[mcast_consumer_index] != nullptr);
    // @TODO should use index?
    Cell* cell = _progress[mcast_consumer_index];
    if (cell->next == nullptr) {
        if (_opened_sink_number == 0) {
            LOG(INFO) << "eof, index:" << mcast_consumer_index;
            return Status::EndOfFile("mcast_local_exchanger eof");
        }
        return Status::InternalError("unreachable in multicast local exchanger");
    }
    cell = cell->next;
    _progress[mcast_consumer_index] = cell;
    cell->used_count += 1;
    LOG(INFO) << "pull_chunk, rows: " << cell->accumulated_row_size << ", used_count: " << cell->used_count << ", index:" << mcast_consumer_index;
    _update_progress(cell);
    return cell->chunk;
*/
}

void SpillableMultiCastLocalExchanger::open_source_operator(int32_t mcast_consumer_index) {
    _queue->open_consumer(mcast_consumer_index);
/*
    std::unique_lock l(_mutex);
    if (_opened_source_opcount[mcast_consumer_index] == 0) {
        _opened_source_number += 1;
    }
    _opened_source_opcount[mcast_consumer_index] += 1;
*/
}

void SpillableMultiCastLocalExchanger::close_source_operator(int32_t mcast_consumer_index) {
    _queue->close_consumer(mcast_consumer_index);
/*
    std::unique_lock l(_mutex);
    _opened_source_opcount[mcast_consumer_index] -= 1;
    if (_opened_source_opcount[mcast_consumer_index] == 0) {
        _opened_source_number--;
        _close_consumer(mcast_consumer_index);
    }
*/
}

void SpillableMultiCastLocalExchanger::open_sink_operator() {
    _queue->open_producer();
/*
    std::unique_lock l(_mutex);
    _opened_sink_number++;
*/
}

void SpillableMultiCastLocalExchanger::close_sink_operator() {
    _queue->close_producer();
/*
    std::unique_lock l(_mutex);
    _opened_sink_number--;
    */
}

void SpillableMultiCastLocalExchanger::_close_consumer(int32_t mcast_consumer_index) {
    LOG(INFO) << "_close consumer: " << mcast_consumer_index;
    Cell* now = _progress[mcast_consumer_index];
    now = now->next;
    while (now) {
        now->used_count += 1;
        // LOG(INFO) << "used_count: " << now->used_count;
        now = now->next;
    }
    _progress[mcast_consumer_index] = nullptr;
    _update_progress();
}

void SpillableMultiCastLocalExchanger::_update_progress(Cell* fast) {
    if (fast != nullptr) {
        _fast_accumulated_row_size = std::max(_fast_accumulated_row_size, fast->accumulated_row_size);
    } else {
        // update the fastest consumer.
        _fast_accumulated_row_size = 0;
        for (size_t i = 0; i < _consumer_number; i++) {
            Cell* c = _progress[i];
            if (c == nullptr) continue;
            _fast_accumulated_row_size = std::max(_fast_accumulated_row_size, c->accumulated_row_size);
        }
    }
    // release chunk if no one needs it.
    while (_head) {

    }
    while (_head && _head->used_count == _consumer_number) {
        Cell* t = _head->next;
        _current_memory_usage -= _head->memory_usage;
        LOG(INFO) << "release chunk, mem: " << _head->memory_usage << ", new mem: " << _current_memory_usage - _head->memory_usage;
        // @TODO free here
        delete _head;
        if (t == nullptr) break;
        _head = t;
    }
    if (_head != nullptr) {
        _current_row_size = _current_accumulated_row_size - _head->accumulated_row_size;
    }

}
}