// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <memory>
#include <mutex>

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
};

// The capacity of this limiter is unlimited.
class UnlimitedChunkBufferLimiter final : public ChunkBufferLimiter {
public:
    class Token final : public ChunkBufferToken {
    public:
        ~Token() override = default;
    };

public:
    ~UnlimitedChunkBufferLimiter() override = default;

    ChunkBufferTokenPtr pin(int num_chunks) override { return std::make_unique<Token>(); }

    bool is_full() const override { return false; }
    size_t size() const override { return 0; }
    size_t capacity() const override { return 0; }
    size_t default_capacity() const override { return 0; }
};

// Use the dynamic chunk memory usage statistics to compute the capacity.
class DynamicChunkBufferLimiter final : public ChunkBufferLimiter {
public:
    class Token final : public ChunkBufferToken {
    public:
        Token(std::atomic<int>& acquired_tokens_counter, int num_tokens)
                : _acquired_tokens_counter(acquired_tokens_counter), _num_tokens(num_tokens) {}

        ~Token() override { _acquired_tokens_counter.fetch_sub(_num_tokens); }

        // Disable copy/move ctor and assignment.
        Token(const Token&) = delete;
        Token& operator=(const Token&) = delete;
        Token(Token&&) = delete;
        Token& operator=(Token&&) = delete;

    private:
        std::atomic<int>& _acquired_tokens_counter;
        const int _num_tokens;
    };

public:
    DynamicChunkBufferLimiter(size_t max_capacity, size_t default_capacity, int64_t mem_limit, int chunk_size)
            : _capacity(default_capacity),
              _max_capacity(max_capacity),
              _default_capacity(default_capacity),
              _mem_limit(mem_limit),
              _chunk_size(chunk_size) {}
    ~DynamicChunkBufferLimiter() override = default;

    void update_avg_row_bytes(size_t added_sum_row_bytes, size_t added_num_rows, size_t max_chunk_rows) override;

    ChunkBufferTokenPtr pin(int num_chunks) override;

    bool is_full() const override { return _pinned_chunks_counter >= _capacity; }
    size_t size() const override { return _pinned_chunks_counter; }
    size_t capacity() const override { return _capacity; }
    size_t default_capacity() const override { return _default_capacity; }

private:
    void _unpin(int num_chunks);

private:
    std::mutex _mutex;
    size_t _sum_row_bytes = 0;
    size_t _num_rows = 0;

    size_t _capacity;
    const size_t _max_capacity;
    const size_t _default_capacity;

    const int64_t _mem_limit;
    const int _chunk_size;

    std::atomic<int> _pinned_chunks_counter = 0;
};

} // namespace starrocks::pipeline
