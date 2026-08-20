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

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

namespace starrocks {

class MemTracker;

struct TantivyReadBufferPoolStats {
    uint64_t acquire = 0;
    uint64_t hit = 0;
    uint64_t miss = 0;
    uint64_t release = 0;
    size_t capacity_bytes = 0;
    size_t max_buffer_bytes = 0;
    size_t cached_bytes = 0;
    size_t in_use_bytes = 0;
};

// Process-local size-class pool for buffers returned to Rust as OwnedBytes.
// A leased buffer is returned only when the last OwnedBytes clone is dropped.
class TantivyReadBufferPool {
public:
    TantivyReadBufferPool(size_t capacity_bytes, size_t max_buffer_bytes, MemTracker* mem_tracker);
    ~TantivyReadBufferPool();

    uint8_t* acquire(size_t requested_bytes, size_t* capacity_bytes);
    void release(uint8_t* buffer, size_t capacity_bytes);
    void prune();
    TantivyReadBufferPoolStats stats() const;

private:
    static constexpr size_t kMinBufferBytes = 1024;
    static constexpr size_t kNumSizeClasses = 11; // 1 KiB through 1 MiB.

    struct SizeClass {
        std::mutex mutex;
        std::vector<uint8_t*> buffers;
    };

    size_t _round_capacity(size_t requested_bytes) const;
    size_t _size_class_index(size_t capacity_bytes) const;
    uint8_t* _allocate(size_t capacity_bytes);
    void _free(uint8_t* buffer);

    size_t _capacity_bytes;
    size_t _max_buffer_bytes;
    MemTracker* _mem_tracker;
    std::array<SizeClass, kNumSizeClasses> _classes;
    std::atomic<uint64_t> _acquire_count{0};
    std::atomic<uint64_t> _hit_count{0};
    std::atomic<uint64_t> _miss_count{0};
    std::atomic<uint64_t> _release_count{0};
    std::atomic<size_t> _cached_bytes{0};
    std::atomic<size_t> _in_use_bytes{0};
};

} // namespace starrocks

// FFI callback invoked by Rust PullDirectory to read bytes from a
// RandomAccessFile. Defined as extern "C" so the Rust side can link
// against it directly (no mangling).
//
// `handle` is a `RandomAccessFile*` cast to `void*`.
// Returns 0 on success, -1 on failure.
extern "C" int sr_random_access_read(void* handle, uint64_t offset, uint8_t* buf, size_t len);

// Lease/release a stable buffer owned by the C++ Tantivy read-buffer pool.
// `pool` is a `starrocks::TantivyReadBufferPool*`. The pool outlives every
// Rust PullDirectory and OwnedBytes object that references it.
extern "C" uint8_t* sr_tantivy_read_buffer_acquire(void* pool, size_t requested_bytes, size_t* capacity_bytes);
extern "C" void sr_tantivy_read_buffer_release(void* pool, uint8_t* buffer, size_t capacity_bytes);
