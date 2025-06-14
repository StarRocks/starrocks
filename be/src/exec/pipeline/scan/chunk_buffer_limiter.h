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
#include <memory>
#include <mutex>

#include "gutil/macros.h"

namespace starrocks::pipeline {

class ChunkBufferToken;
using ChunkBufferTokenPtr = std::unique_ptr<ChunkBufferToken>;
class ChunkBufferLimiter;
using ChunkBufferLimiterPtr = std::unique_ptr<ChunkBufferLimiter>;

class ChunkBufferToken {
public:
    virtual ~ChunkBufferToken() = default;
};

// Limit the capacity of a chunk buffer.
// - Before creating a new chunk, should use `pin()` to pin a position in the buffer and return a token.
// - After a chunk is popped from buffer, should desctruct the token to unpin the position.
// All the methods are thread-safe.
class ChunkBufferLimiter {
public:
    virtual ~ChunkBufferLimiter() = default;

    // Update the chunk memory usage statistics.
    // `added_sum_row_bytes` is the bytes of the new reading rows.
    // `added_num_rows` is the number of the new read rows.
    virtual void update_avg_row_bytes(size_t added_sum_row_bytes, size_t added_num_rows, size_t max_chunk_rows) {}

    // Pin a position in the buffer and return a token.
    // When desctructing the token, the position will be unpinned.
    virtual ChunkBufferTokenPtr pin(int num_chunks) = 0;

    // Returns true, when it cannot pin a position for now.
    virtual bool is_full() const = 0;
    // The number of already pinned positions.
    virtual size_t size() const = 0;
    // The max number of positions able to be pinned.
    virtual size_t capacity() const = 0;
    // The default capacity when there isn't chunk memory usage statistics.
    virtual size_t default_capacity() const = 0;
    // Update mem limit of this chunk buffer
    virtual void update_mem_limit(int64_t value) {}
    virtual bool has_full_events() { return false; }
};

// The capacity of this limiter is unlimited.
class UnlimitedChunkBufferLimiter final : public ChunkBufferLimiter {
public:
    class Token final : public ChunkBufferToken {
    public:
        Token(std::atomic<int>& pinned_tokens_counter, int num_tokens)
                : _pinned_tokens_counter(pinned_tokens_counter), _num_tokens(num_tokens) {}

        ~Token() override { _pinned_tokens_counter.fetch_sub(_num_tokens); }

        DISALLOW_COPY_AND_MOVE(Token);

    private:
        std::atomic<int>& _pinned_tokens_counter;
        const int _num_tokens;
    };

public:
    ~UnlimitedChunkBufferLimiter() override = default;

    ChunkBufferTokenPtr pin(int num_chunks) override {
        _pinned_chunks_counter.fetch_add(num_chunks);
        return std::make_unique<UnlimitedChunkBufferLimiter::Token>(_pinned_chunks_counter, num_chunks);
    }

    bool is_full() const override { return false; }
    size_t size() const override { return _pinned_chunks_counter; }
    size_t capacity() const override { return std::numeric_limits<int64_t>::max(); }
    size_t default_capacity() const override { return std::numeric_limits<int64_t>::max(); }

private:
    std::atomic<int> _pinned_chunks_counter = 0;
};

// Use the dynamic chunk memory usage statistics to compute the capacity.
class DynamicChunkBufferLimiter final : public ChunkBufferLimiter {
public:
    class Token final : public ChunkBufferToken {
    public:
        Token(DynamicChunkBufferLimiter& limiter, int num_tokens) : _limiter(limiter), _num_tokens(num_tokens) {}

        ~Token() override { _limiter.unpin(_num_tokens); }

        DISALLOW_COPY_AND_MOVE(Token);

    private:
        DynamicChunkBufferLimiter& _limiter;
        const int _num_tokens;
    };

public:
    DynamicChunkBufferLimiter(size_t max_capacity, size_t default_capacity, int64_t mem_limit, int chunk_size)
            : _capacity(default_capacity),
              _max_capacity(max_capacity),
              _default_capacity(default_capacity),
              _mem_limit(mem_limit) {}

    ~DynamicChunkBufferLimiter() override = default;

    void update_avg_row_bytes(size_t added_sum_row_bytes, size_t added_num_rows, size_t max_chunk_rows) override;

    ChunkBufferTokenPtr pin(int num_chunks) override;
    void unpin(int num_chunks);

    bool is_full() const override {
        if (_pinned_chunks_counter >= _capacity) {
            _returned_full_event.store(true, std::memory_order_release);
            return true;
        }
        return false;
    }
    size_t size() const override { return _pinned_chunks_counter; }
    size_t capacity() const override { return _capacity; }
    size_t default_capacity() const override { return _default_capacity; }
    void update_mem_limit(int64_t value) override;
    bool has_full_events() override {
        if (!_has_full_event.load(std::memory_order_acquire)) {
            return false;
        }
        bool val = true;
        return _has_full_event.compare_exchange_strong(val, false);
    }

private:
private:
    std::mutex _mutex;
    size_t _sum_row_bytes = 0;
    size_t _num_rows = 0;

    size_t _capacity;
    const size_t _max_capacity;
    const size_t _default_capacity;

    std::atomic<int64_t> _mem_limit;

    std::atomic<int> _pinned_chunks_counter = 0;

    std::atomic<bool> _has_full_event{};

    mutable std::atomic<bool> _returned_full_event{};
};

} // namespace starrocks::pipeline
