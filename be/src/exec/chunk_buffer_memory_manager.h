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

#include <atomic>
#include <cstdint>
#include <limits>

#include "base/testutil/sync_point.h"
#include "base/utility/updater.h"
#include "common/logging.h"

namespace starrocks::pipeline {
// Manage the memory usage for local exchange
class ChunkBufferMemoryManager {
public:
    ChunkBufferMemoryManager(size_t max_input_dop, int64_t per_driver_mem_limit,
                             int64_t max_buffered_rows = std::numeric_limits<int64_t>::max())
            : _max_input_dop(max_input_dop) {
        _max_buffered_rows = max_buffered_rows;
        if (per_driver_mem_limit > 0) {
            _max_memory_usage_per_driver = per_driver_mem_limit;
        } else {
            LOG(WARNING) << "invalid per_driver_mem_limit";
        }
        size_t res = max_input_dop * _max_memory_usage_per_driver;
        if (res < _max_memory_usage) {
            _max_memory_usage = res;
        }
    }

    // Records a full -> not-full event for LocalExchanger::notify_sink() to wake up OUTPUT_FULL sinks.
    // is_full() is `memory over limit || rows over limit` and can only become false when one of the two counters
    // falls below its limit. Each counter's falling edge is detected exactly from its own fetch_add() result, so no
    // full -> not-full transition is missed even under concurrent updates. Do not derive the edge from a separate
    // is_full() read or from a combined state of both counters: neither is a consistent snapshot and may lose the
    // edge, leaving sink drivers parked forever with the event scheduler.
    //
    // How the old `prev_full = is_full(); fetch_add(); if (prev_full != now_full) set flag` lost it
    // (consumer C, producer P, consumer D, limit = 2 chunks, mem = 1 chunk):
    //   C: prev_full = is_full();   // false, then C is preempted
    //   P: fetch_add(+chunk);       // mem = 2, full: flag = true; sinks park in OUTPUT_FULL
    //   D: notify_sink();           // consumes the flag while still full; woken sinks re-park
    //   C: fetch_add(-chunk);       // mem = 1, not full, but prev_full == now_full == false: no flag
    //   C: notify_sink();           // flag is false: no wake-up, the buffer is not full and every sink is parked
    // A standalone model of this schedule is attached to PR #64858 (with a 200us delay in the window: 18/100 runs
    // hang with the old code, 0/400 with this one).
    void update_memory_usage(int64_t memory_usage, int64_t num_rows) {
        TEST_SYNC_POINT_CALLBACK("ChunkBufferMemoryManager::update_memory_usage::begin", &memory_usage);
        int64_t prev_memusage = _memory_usage.fetch_add(memory_usage);
        TEST_SYNC_POINT_CALLBACK("ChunkBufferMemoryManager::update_memory_usage::between_counters", &memory_usage);
        int64_t prev_num_rows = _buffered_num_rows.fetch_add(num_rows);
        _update_peak_memusage();
        _update_peak_num_rows();
        // Load the limit after fetch_add(): update_max_memory_usage() may lower it concurrently, and a sink that
        // parked on the lower limit must still see the fall below it.
        const auto max_memory_usage = static_cast<int64_t>(_max_memory_usage.load());
        const auto max_buffered_rows = static_cast<int64_t>(_max_buffered_rows);
        bool memory_fell = prev_memusage >= max_memory_usage && prev_memusage + memory_usage < max_memory_usage;
        bool rows_fell = prev_num_rows > max_buffered_rows && prev_num_rows + num_rows <= max_buffered_rows;
        if (memory_fell || rows_fell) {
            _full_events_changed.store(true, std::memory_order_release);
        }
    }

    size_t get_memory_limit_per_driver() const { return _max_memory_usage_per_driver; }

    int64_t get_memory_usage() const { return _memory_usage; }

    int64_t get_peak_memory_usage() const { return _peak_memory_usage.load(); }
    int64_t get_peak_num_rows() const { return _peak_num_rows.load(); }

    bool is_full() const { return _memory_usage >= _max_memory_usage || _buffered_num_rows > _max_buffered_rows; }

    bool is_half_full() const { return _memory_usage * 2 >= _max_memory_usage; }

    size_t get_max_input_dop() const { return _max_input_dop; }

    void update_max_memory_usage(size_t max_memory_usage) {
        DCHECK(max_memory_usage > 0);
        _max_memory_usage = max_memory_usage;
        // A sink may have combined a counter value read before this store with the new limit and parked on a
        // "full" state that never existed, while the concurrent refund compared against the old limit and saw no
        // falling edge. Record an event so the caller's notify_sink() makes every sink re-check the current state.
        _full_events_changed.store(true, std::memory_order_release);
    }

    void clear() {
        _memory_usage = 0;
        _buffered_num_rows = 0;
    }

    bool full_events_changed() {
        if (!_full_events_changed.load(std::memory_order_acquire)) {
            return false;
        }
        bool val = true;
        return _full_events_changed.compare_exchange_strong(val, false);
    }

private:
    void _update_peak_memusage() { atomic_max(_peak_memory_usage, _memory_usage); }

    void _update_peak_num_rows() { atomic_max(_peak_num_rows, _buffered_num_rows); }

    std::atomic<size_t> _max_memory_usage{128UL * 1024 * 1024 * 1024}; // 128GB
    size_t _max_memory_usage_per_driver = 128 * 1024 * 1024UL;         // 128MB
    size_t _max_buffered_rows{};
    std::atomic<int64_t> _memory_usage{};
    std::atomic<int64_t> _buffered_num_rows{};
    std::atomic<int64_t> _peak_memory_usage{};
    std::atomic<int64_t> _peak_num_rows{};
    size_t _max_input_dop;
    std::atomic<bool> _full_events_changed{};
};

// RAII account record for a single chunk buffered in a local exchange queue.
// Construction adds the chunk's memory/num_rows to the shared manager exactly once;
// destruction subtracts them. By holding it via shared_ptr across all queue entries that
// reference the same chunk, the manager's record is released only when the last
// reference is consumed.
class ChunkBufferMemoryEntry {
public:
    ChunkBufferMemoryEntry(ChunkBufferMemoryManager* manager, size_t memory_usage, size_t num_rows)
            : _manager(manager), _memory_usage(memory_usage), _num_rows(num_rows) {
        _manager->update_memory_usage(static_cast<int64_t>(_memory_usage), static_cast<int64_t>(_num_rows));
    }

    ~ChunkBufferMemoryEntry() {
        _manager->update_memory_usage(-static_cast<int64_t>(_memory_usage), -static_cast<int64_t>(_num_rows));
    }

    ChunkBufferMemoryEntry(const ChunkBufferMemoryEntry&) = delete;
    ChunkBufferMemoryEntry& operator=(const ChunkBufferMemoryEntry&) = delete;

    size_t memory_usage() const { return _memory_usage; }
    size_t num_rows() const { return _num_rows; }

private:
    ChunkBufferMemoryManager* _manager;
    const size_t _memory_usage;
    const size_t _num_rows;
};
} // namespace starrocks::pipeline
