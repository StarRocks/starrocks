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

#include "storage/index/inverted/tantivy/random_access_bridge.h"

#include <algorithm>
#include <cstdlib>

#include "fs/fs.h"
#include "runtime/current_thread.h"

namespace starrocks {

TantivyReadBufferPool::TantivyReadBufferPool(size_t capacity_bytes, size_t max_buffer_bytes, MemTracker* mem_tracker)
        : _capacity_bytes(capacity_bytes),
          _max_buffer_bytes(std::min(max_buffer_bytes, kMinBufferBytes << (kNumSizeClasses - 1))),
          _mem_tracker(mem_tracker) {}

TantivyReadBufferPool::~TantivyReadBufferPool() {
    prune();
}

void TantivyReadBufferPool::prune() {
    for (size_t index = 0; index < _classes.size(); ++index) {
        auto& size_class = _classes[index];
        std::vector<uint8_t*> buffers;
        {
            std::lock_guard lock(size_class.mutex);
            buffers.swap(size_class.buffers);
        }
        const size_t released_bytes = buffers.size() * (kMinBufferBytes << index);
        for (auto* buffer : buffers) {
            _free(buffer);
        }
        if (released_bytes > 0) {
            _cached_bytes.fetch_sub(released_bytes, std::memory_order_relaxed);
        }
    }
}

size_t TantivyReadBufferPool::_round_capacity(size_t requested_bytes) const {
    if (requested_bytes > _max_buffer_bytes || _max_buffer_bytes < kMinBufferBytes) {
        return requested_bytes;
    }
    size_t capacity = kMinBufferBytes;
    while (capacity < requested_bytes) {
        capacity <<= 1;
    }
    return capacity;
}

size_t TantivyReadBufferPool::_size_class_index(size_t capacity_bytes) const {
    size_t index = 0;
    for (size_t size = kMinBufferBytes; size < capacity_bytes; size <<= 1) {
        ++index;
    }
    return index;
}

uint8_t* TantivyReadBufferPool::_allocate(size_t capacity_bytes) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker);
    return static_cast<uint8_t*>(std::malloc(capacity_bytes));
}

void TantivyReadBufferPool::_free(uint8_t* buffer) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker);
    std::free(buffer);
}

uint8_t* TantivyReadBufferPool::acquire(size_t requested_bytes, size_t* capacity_bytes) {
    if (requested_bytes == 0 || capacity_bytes == nullptr) {
        return nullptr;
    }
    _acquire_count.fetch_add(1, std::memory_order_relaxed);
    const size_t capacity = _round_capacity(requested_bytes);
    *capacity_bytes = capacity;
    if (capacity <= _max_buffer_bytes && capacity >= kMinBufferBytes) {
        auto& size_class = _classes[_size_class_index(capacity)];
        {
            std::lock_guard lock(size_class.mutex);
            if (!size_class.buffers.empty()) {
                auto* buffer = size_class.buffers.back();
                size_class.buffers.pop_back();
                _cached_bytes.fetch_sub(capacity, std::memory_order_relaxed);
                _in_use_bytes.fetch_add(capacity, std::memory_order_relaxed);
                _hit_count.fetch_add(1, std::memory_order_relaxed);
                return buffer;
            }
        }
    }
    auto* buffer = _allocate(capacity);
    if (buffer != nullptr) {
        _in_use_bytes.fetch_add(capacity, std::memory_order_relaxed);
        _miss_count.fetch_add(1, std::memory_order_relaxed);
    }
    return buffer;
}

void TantivyReadBufferPool::release(uint8_t* buffer, size_t capacity_bytes) {
    if (buffer == nullptr || capacity_bytes == 0) {
        return;
    }
    _release_count.fetch_add(1, std::memory_order_relaxed);
    _in_use_bytes.fetch_sub(capacity_bytes, std::memory_order_relaxed);
    if (capacity_bytes <= _max_buffer_bytes && capacity_bytes >= kMinBufferBytes) {
        size_t cached = _cached_bytes.load(std::memory_order_relaxed);
        while (cached + capacity_bytes <= _capacity_bytes) {
            if (_cached_bytes.compare_exchange_weak(cached, cached + capacity_bytes, std::memory_order_relaxed)) {
                auto& size_class = _classes[_size_class_index(capacity_bytes)];
                std::lock_guard lock(size_class.mutex);
                size_class.buffers.push_back(buffer);
                return;
            }
        }
    }
    _free(buffer);
}

TantivyReadBufferPoolStats TantivyReadBufferPool::stats() const {
    return {
            .acquire = _acquire_count.load(std::memory_order_relaxed),
            .hit = _hit_count.load(std::memory_order_relaxed),
            .miss = _miss_count.load(std::memory_order_relaxed),
            .release = _release_count.load(std::memory_order_relaxed),
            .capacity_bytes = _capacity_bytes,
            .max_buffer_bytes = _max_buffer_bytes,
            .cached_bytes = _cached_bytes.load(std::memory_order_relaxed),
            .in_use_bytes = _in_use_bytes.load(std::memory_order_relaxed),
    };
}

} // namespace starrocks

extern "C" int sr_random_access_read(void* handle, uint64_t offset, uint8_t* buf, size_t len) {
    if (handle == nullptr || buf == nullptr) {
        return -1;
    }
    auto* file = static_cast<starrocks::RandomAccessFile*>(handle);
    auto st = file->read_at_fully(static_cast<int64_t>(offset), buf, static_cast<int64_t>(len));
    return st.ok() ? 0 : -1;
}

extern "C" uint8_t* sr_tantivy_read_buffer_acquire(void* pool, size_t requested_bytes, size_t* capacity_bytes) {
    if (pool == nullptr) {
        return nullptr;
    }
    return static_cast<starrocks::TantivyReadBufferPool*>(pool)->acquire(requested_bytes, capacity_bytes);
}

extern "C" void sr_tantivy_read_buffer_release(void* pool, uint8_t* buffer, size_t capacity_bytes) {
    if (pool == nullptr || buffer == nullptr) {
        return;
    }
    static_cast<starrocks::TantivyReadBufferPool*>(pool)->release(buffer, capacity_bytes);
}
