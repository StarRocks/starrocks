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

#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

namespace starrocks {

// Wraps an arrow::MemoryPool and enforces a hard upper bound on bytes allocated
// while decoding a discrete Arrow IPC stream message (for example, LZ4/ZSTD
// decompressed body buffers).
//
// LIFETIME INVARIANT: Any arrow::RecordBatch decoded using this pool may hold
// arrow::PoolBuffer instances whose destructors call pool_->Free(). Therefore,
// the owning ArrowBoundedMemoryPool MUST outlive all RecordBatches decoded from it.
class ArrowBoundedMemoryPool final : public arrow::MemoryPool {
public:
    using arrow::MemoryPool::Allocate;
    using arrow::MemoryPool::Free;
    using arrow::MemoryPool::Reallocate;

    explicit ArrowBoundedMemoryPool(int64_t max_bytes, arrow::MemoryPool* delegate = arrow::default_memory_pool())
            : _max_bytes(max_bytes), _delegate(delegate) {}

    ~ArrowBoundedMemoryPool() override = default;

    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
        if (size < 0) {
            return arrow::Status::Invalid("Negative Arrow allocation size: ", size);
        }
        int64_t current = _bytes_allocated.fetch_add(size, std::memory_order_relaxed) + size;
        if (_max_bytes > 0 && current > _max_bytes) {
            _bytes_allocated.fetch_sub(size, std::memory_order_relaxed);
            return arrow::Status::OutOfMemory("Arrow IPC decoded allocation of ", size,
                                              " bytes exceeds per-message limit of ", _max_bytes,
                                              " bytes (current allocated: ", current - size, ")");
        }
        arrow::Status st = _delegate->Allocate(size, alignment, out);
        if (!st.ok()) {
            _bytes_allocated.fetch_sub(size, std::memory_order_relaxed);
            return st;
        }
        _total_bytes_allocated.fetch_add(size, std::memory_order_relaxed);
        _num_allocations.fetch_add(1, std::memory_order_relaxed);
        update_peak(current);
        return arrow::Status::OK();
    }

    arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override {
        if (new_size < 0) {
            return arrow::Status::Invalid("Negative Arrow reallocation size: ", new_size);
        }
        int64_t delta = new_size - old_size;
        int64_t current = _bytes_allocated.fetch_add(delta, std::memory_order_relaxed) + delta;
        if (_max_bytes > 0 && delta > 0 && current > _max_bytes) {
            _bytes_allocated.fetch_sub(delta, std::memory_order_relaxed);
            return arrow::Status::OutOfMemory("Arrow IPC decoded reallocation of ", new_size,
                                              " bytes exceeds per-message limit of ", _max_bytes, " bytes");
        }
        arrow::Status st = _delegate->Reallocate(old_size, new_size, alignment, ptr);
        if (!st.ok()) {
            _bytes_allocated.fetch_sub(delta, std::memory_order_relaxed);
            return st;
        }
        if (delta > 0) {
            _total_bytes_allocated.fetch_add(delta, std::memory_order_relaxed);
            update_peak(current);
        }
        return arrow::Status::OK();
    }

    void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
        _delegate->Free(buffer, size, alignment);
        _bytes_allocated.fetch_sub(size, std::memory_order_relaxed);
    }

    int64_t bytes_allocated() const override { return _bytes_allocated.load(std::memory_order_relaxed); }

    int64_t max_memory() const override { return _peak_bytes.load(std::memory_order_relaxed); }

    int64_t total_bytes_allocated() const override { return _total_bytes_allocated.load(std::memory_order_relaxed); }

    int64_t num_allocations() const override { return _num_allocations.load(std::memory_order_relaxed); }

    std::string backend_name() const override { return "starrocks_bounded_arrow_pool"; }

private:
    void update_peak(int64_t current) {
        int64_t prev_peak = _peak_bytes.load(std::memory_order_relaxed);
        while (current > prev_peak &&
               !_peak_bytes.compare_exchange_weak(prev_peak, current, std::memory_order_relaxed)) {
        }
    }

    const int64_t _max_bytes;
    arrow::MemoryPool* _delegate;
    std::atomic<int64_t> _bytes_allocated{0};
    std::atomic<int64_t> _peak_bytes{0};
    std::atomic<int64_t> _total_bytes_allocated{0};
    std::atomic<int64_t> _num_allocations{0};
};

// Zero-copy arrow::Buffer that co-owns a StarRocks ByteBufferPtr (or any
// shared_ptr buffer exposing ptr and remaining()) so sliced child buffers
// inside arrow::RecordBatch keep the underlying memory alive without coupling
// ColumnCore to Runtime.
class OwningByteBufferArrowBuffer final : public arrow::Buffer {
public:
    template <typename ByteBufferT>
    explicit OwningByteBufferArrowBuffer(std::shared_ptr<ByteBufferT> buf)
            : arrow::Buffer(reinterpret_cast<const uint8_t*>(buf->ptr), static_cast<int64_t>(buf->remaining())),
              _owner(std::move(buf)) {}

    ~OwningByteBufferArrowBuffer() override = default;

private:
    std::shared_ptr<void> _owner;
};

template <typename ByteBufferT>
inline std::shared_ptr<arrow::Buffer> make_owning_arrow_buffer(std::shared_ptr<ByteBufferT> buf) {
    return std::make_shared<OwningByteBufferArrowBuffer>(std::move(buf));
}

} // namespace starrocks
