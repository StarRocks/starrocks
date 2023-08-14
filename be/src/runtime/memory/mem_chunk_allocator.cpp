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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/memory/chunk_allocator.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/memory/mem_chunk_allocator.h"

#include <memory>
#include <mutex>

#include "gutil/dynamic_annotations.h"
#include "runtime/current_thread.h"
#include "runtime/memory/mem_chunk.h"
#include "runtime/memory/system_allocator.h"
#include "util/bit_util.h"
#include "util/cpu_info.h"
#include "util/defer_op.h"
#include "util/failpoint/fail_point.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

MemChunkAllocator* MemChunkAllocator::_s_instance = nullptr;

static IntCounter local_core_alloc_count(MetricUnit::NOUNIT);
static IntCounter other_core_alloc_count(MetricUnit::NOUNIT);
static IntCounter system_alloc_count(MetricUnit::NOUNIT);
static IntCounter system_free_count(MetricUnit::NOUNIT);
static IntCounter system_alloc_cost_ns(MetricUnit::NANOSECONDS);
static IntCounter system_free_cost_ns(MetricUnit::NANOSECONDS);

#ifdef BE_TEST
static std::mutex s_mutex;
MemChunkAllocator* MemChunkAllocator::instance() {
    std::lock_guard<std::mutex> l(s_mutex);
    if (_s_instance == nullptr) {
        CpuInfo::init();
        MemChunkAllocator::init_instance(nullptr, 4096);
    }
    return _s_instance;
}
#endif

// Keep free chunk's ptr in size separated free list.
// This class is thread-safe.
class ChunkArena {
public:
    ChunkArena(MemTracker* mem_tracker) : _mem_tracker(mem_tracker), _chunk_lists(64) {}

    ~ChunkArena() {
        for (int i = 0; i < 64; ++i) {
            if (_chunk_lists[i].empty()) continue;
            size_t size = (uint64_t)1 << i;
            for (auto ptr : _chunk_lists[i]) {
                SystemAllocator::free(_mem_tracker, ptr, size);
            }
        }
    }

    // Try to pop a free chunk from corresponding free list.
    // Return true if success
    bool pop_free_chunk(size_t size, uint8_t** ptr) {
        int idx = BitUtil::Log2Ceiling64(size);
        auto& free_list = _chunk_lists[idx];

        std::lock_guard<SpinLock> l(_lock);
        if (free_list.empty()) {
            return false;
        }
        *ptr = free_list.back();
        free_list.pop_back();
        ASAN_UNPOISON_MEMORY_REGION(*ptr, size);
        return true;
    }

    void push_free_chunk(uint8_t* ptr, size_t size) {
        int idx = BitUtil::Log2Ceiling64(size);
        // Poison this chunk to make asan can detect invalid access
        ASAN_POISON_MEMORY_REGION(ptr, size);
        std::lock_guard<SpinLock> l(_lock);
        _chunk_lists[idx].push_back(ptr);
    }

private:
    MemTracker* _mem_tracker = nullptr;
    SpinLock _lock;
    std::vector<std::vector<uint8_t*>> _chunk_lists;
};

void MemChunkAllocator::init_instance(MemTracker* mem_tracker, size_t reserve_limit) {
    if (_s_instance != nullptr) return;
    _s_instance = new MemChunkAllocator(mem_tracker, reserve_limit);

#define REGISTER_METIRC_WITH_NAME(name, metric) StarRocksMetrics::instance()->metrics()->register_metric(#name, &metric)

#define REGISTER_METIRC_WITH_PREFIX(prefix, name) REGISTER_METIRC_WITH_NAME(prefix##name, name)

#define REGISTER_METIRC(name) REGISTER_METIRC_WITH_PREFIX(chunk_pool_, name)

    REGISTER_METIRC(local_core_alloc_count);
    REGISTER_METIRC(other_core_alloc_count);
    REGISTER_METIRC(system_alloc_count);
    REGISTER_METIRC(system_free_count);
    REGISTER_METIRC(system_alloc_cost_ns);
    REGISTER_METIRC(system_free_cost_ns);
}

MemChunkAllocator::MemChunkAllocator(MemTracker* mem_tracker, size_t reserve_limit)
        : _mem_tracker(mem_tracker),
          _reserve_bytes_limit(reserve_limit),
          _reserved_bytes(0),
          _arenas(CpuInfo::get_max_num_cores()) {
    for (auto& _arena : _arenas) {
        _arena = std::make_unique<ChunkArena>(_mem_tracker);
    }
}

bool MemChunkAllocator::allocate(size_t size, MemChunk* chunk) {
    FAIL_POINT_TRIGGER_RETURN(random_error, false);
    bool ret = true;
#ifndef BE_TEST
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_mem_tracker);
    DeferOp op([&] {
        if (ret) {
            if (LIKELY(_mem_tracker != nullptr)) {
                _mem_tracker->release(chunk->size);
            }
            if (LIKELY(prev_tracker != nullptr)) {
                prev_tracker->consume(chunk->size);
            }
        }
        tls_thread_status.set_mem_tracker(prev_tracker);
    });
#endif

    // fast path: allocate from current core arena
    int core_id = CpuInfo::get_current_core();
    chunk->size = size;
    chunk->core_id = core_id;

    if (_arenas[core_id]->pop_free_chunk(size, &chunk->data)) {
        _reserved_bytes.fetch_sub(size);
        local_core_alloc_count.increment(1);
        ret = true;
        return ret;
    }
    if (_reserved_bytes > size) {
        // try to allocate from other core's arena
        ++core_id;
        for (int i = 1; i < _arenas.size(); ++i, ++core_id) {
            if (_arenas[core_id % _arenas.size()]->pop_free_chunk(size, &chunk->data)) {
                _reserved_bytes.fetch_sub(size);
                other_core_alloc_count.increment(1);
                // reset chunk's core_id to other
                chunk->core_id = core_id % _arenas.size();
                ret = true;
                return ret;
            }
        }
    }

    int64_t cost_ns = 0;
    {
        SCOPED_RAW_TIMER(&cost_ns);
        // allocate from system allocator
        chunk->data = SystemAllocator::allocate(_mem_tracker, size);
    }
    system_alloc_count.increment(1);
    system_alloc_cost_ns.increment(cost_ns);
    if (chunk->data == nullptr) {
        ret = false;
        return ret;
    }
    ret = true;
    return ret;
}

void MemChunkAllocator::free(const MemChunk& chunk) {
#ifndef BE_TEST
    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_mem_tracker);
    DeferOp op([&] {
        int64_t chunk_size = chunk.size;
        if (LIKELY(prev_tracker != nullptr)) {
            prev_tracker->release(chunk_size);
        }
        if (LIKELY(_mem_tracker != nullptr)) {
            _mem_tracker->consume(chunk_size);
        }
        tls_thread_status.set_mem_tracker(prev_tracker);
    });
#endif

    int64_t old_reserved_bytes = _reserved_bytes;
    int64_t new_reserved_bytes = 0;
    do {
        new_reserved_bytes = old_reserved_bytes + chunk.size;
        if (new_reserved_bytes > _reserve_bytes_limit) {
            int64_t cost_ns = 0;
            {
                SCOPED_RAW_TIMER(&cost_ns);
                SystemAllocator::free(_mem_tracker, chunk.data, chunk.size);
            }
            system_free_count.increment(1);
            system_free_cost_ns.increment(cost_ns);

            return;
        }
    } while (!_reserved_bytes.compare_exchange_weak(old_reserved_bytes, new_reserved_bytes));

    _arenas[chunk.core_id]->push_free_chunk(chunk.data, chunk.size);
}

} // namespace starrocks
