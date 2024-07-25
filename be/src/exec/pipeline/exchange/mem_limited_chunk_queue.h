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

#pragma once

#include <mutex>
#include <queue>

#include "exec/pipeline/exchange/multi_cast_local_exchange.h"
#include "serde/encode_context.h"
#include "util/bit_mask.h"
#include "util/phmap/phmap.h"
#include "util/spinlock.h"

namespace starrocks::pipeline {

class MemLimitedChunkQueue {
private:
    struct Cell {
        ChunkPtr chunk;
        size_t accumulated_rows = 0;
        size_t accumulated_bytes = 0;
        int32_t used_count = 0;
        bool flushed = false;
    };

    struct Block {
        Block(int32_t consumer_number) : reader_mask(consumer_number) {}

        std::vector<Cell> cells;
        bool in_mem = true;
        std::atomic_bool has_load_task = false;
        // a bit mask to record which consumer is pendind read
        // there may be a flush task between can_pop and pop,
        // we should prevent the block that is about to be read from being flushed.
        SpinLock lock;
        BitMask reader_mask;
        size_t memory_usage = 0;
        Block* next = nullptr;
        // spillable block, only used when spill happens
        spill::BlockPtr block;
        size_t flush_chunks = 0;
        size_t flush_rows = 0;
        size_t flush_bytes = 0;

        void add_pending_reader(int32_t consumer_index) {
            std::lock_guard<SpinLock> l(lock);
            reader_mask.set_bit(consumer_index);
        }
        void remove_pending_reader(int32_t consumer_index) {
            std::lock_guard<SpinLock> l(lock);
            reader_mask.clear_bit(consumer_index);
        }
        bool has_pending_reader() {
            std::lock_guard<SpinLock> l(lock);
            return !reader_mask.all_bits_zero();
        }
    };

    struct Iterator {
        Iterator() = default;
        Iterator(Block* blk, size_t idx) : block(blk), index(idx) {}
        Block* block = nullptr;
        size_t index = 0;

        bool valid() const { return block != nullptr && index < block->cells.size(); }

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

        Block* get_block() { return block; }
    };

    static const size_t kDefaultBlockSize = 8L * 1024 * 1024;
    static const size_t kDefaultMaxUnconsumedBytes = 16L * 1024 * 1024;
    static const size_t kDefaultMemoryLimit = 16L * 1024 * 1024;

public:
    struct Options {
        int32_t plan_node_id = 0;
        int encode_level = 7;
        // memory block size
        size_t block_size = kDefaultBlockSize;
        size_t memory_limit = kDefaultMemoryLimit;
        size_t max_unconsumed_bytes = kDefaultMaxUnconsumedBytes;
        spill::BlockManager* block_manager = nullptr;
        workgroup::WorkGroupPtr wg;
    };

    MemLimitedChunkQueue(RuntimeState* state, int32_t consumer_number, Options opts);
    ~MemLimitedChunkQueue();

    Status init_metrics(RuntimeProfile* parent);

    Status push(const ChunkPtr& chunk);
    bool can_push();

    StatusOr<ChunkPtr> pop(int32_t consumer_index);
    bool can_pop(int32_t consumer_index);

    void open_consumer(int32_t consumer_index);

    void close_consumer(int32_t consumer_index);

    void open_producer();

    void close_producer();

    void enter_release_memory_mode();

private:
    void _update_progress(Iterator* iter = nullptr);

    void _close_consumer(int32_t consumer_index);

    Status _flush();
    // trigger flush task
    Status _submit_flush_task();
    // reload block from disk
    Status _load(Block* block);

    Status _submit_load_task(Block* block);

    void _evict_loaded_block();

    inline void _update_io_task_status(const Status& status) {
        if (status.ok() || _io_task_status.load() != nullptr) {
            return;
        }
        Status* old_status = nullptr;
        if (_io_task_status.compare_exchange_strong(old_status, &_status)) {
            _status = status;
        }
    }
    Status _get_io_task_status() const {
        auto* status = _io_task_status.load();
        return status == nullptr ? Status::OK() : *status;
    }

    RuntimeState* _state = nullptr;
    std::shared_mutex _mutex;
    // an empty chunk, only keep meta
    ChunkPtr _chunk_builder;
    Block* _head = nullptr;
    Block* _tail = nullptr;
    Block* _next_flush_block = nullptr;
    // an iterator point to head;
    Iterator _iterator;

    int32_t _consumer_number;
    int32_t _opened_source_number = 0;
    int32_t _opened_sink_number = 0;
    std::vector<int32_t> _opened_source_opcount;
    // consumption progress of each consumer
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

    Options _opts;

    // @TODO we can use atomic shared_ptr directly after upgrading compiler
    std::atomic<Status*> _io_task_status = nullptr;
    Status _status;

    std::atomic_bool _has_flush_io_task = false;
    phmap::flat_hash_set<Block*> _loaded_blocks;

    RuntimeProfile::HighWaterMarkCounter* _peak_memory_bytes_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_memory_rows_counter = nullptr;

    RuntimeProfile::Counter* _flush_io_timer = nullptr;
    RuntimeProfile::Counter* _flush_io_count = nullptr;
    RuntimeProfile::Counter* _flush_io_bytes = nullptr;
    // io stats of load task
    RuntimeProfile::Counter* _read_io_timer = nullptr;
    RuntimeProfile::Counter* _read_io_count = nullptr;
    RuntimeProfile::Counter* _read_io_bytes = nullptr;
};

} // namespace starrocks::pipeline