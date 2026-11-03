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

#include "column/arrow/arrow_memory_pool.h"

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <gtest/gtest.h>

#include <cstring>
#include <memory>

namespace starrocks {

namespace {

// Matches starrocks::ByteBuffer's data/window contract without pulling Runtime
// symbols into ColumnCore's column_test binary.
struct TestByteBuffer {
    explicit TestByteBuffer(size_t capacity_) : ptr(new char[capacity_]), limit(capacity_), capacity(capacity_) {}
    ~TestByteBuffer() { delete[] ptr; }

    void put_bytes(const char* data, size_t size) {
        std::memcpy(ptr + pos, data, size);
        pos += size;
    }

    void flip_to_read() {
        limit = pos;
        pos = 0;
    }

    size_t remaining() const { return limit - pos; }

    char* const ptr;
    size_t pos{0};
    size_t limit;
    const size_t capacity;
};

} // namespace

TEST(ArrowMemoryPoolTest, AllowsAllocationsWithinLimitAndFreesCleanly) {
    ArrowBoundedMemoryPool pool(1024);
    uint8_t* buf = nullptr;
    ASSERT_TRUE(pool.Allocate(512, &buf).ok());
    ASSERT_NE(buf, nullptr);
    EXPECT_EQ(pool.bytes_allocated(), 512);
    EXPECT_GE(pool.max_memory(), 512);
    pool.Free(buf, 512);
    EXPECT_EQ(pool.bytes_allocated(), 0);
}

TEST(ArrowMemoryPoolTest, RejectsAllocationExceedingLimit) {
    ArrowBoundedMemoryPool pool(1024);
    uint8_t* buf1 = nullptr;
    ASSERT_TRUE(pool.Allocate(768, &buf1).ok());

    uint8_t* buf2 = nullptr;
    arrow::Status st = pool.Allocate(512, &buf2);
    EXPECT_TRUE(st.IsCapacityError()) << st.ToString();
    EXPECT_EQ(pool.bytes_allocated(), 768);

    pool.Free(buf1, 768);
    EXPECT_EQ(pool.bytes_allocated(), 0);
}

TEST(ArrowMemoryPoolTest, RejectsReallocateExceedingLimit) {
    ArrowBoundedMemoryPool pool(1024);
    uint8_t* buf = nullptr;
    ASSERT_TRUE(pool.Allocate(512, &buf).ok());

    arrow::Status st = pool.Reallocate(512, 2048, &buf);
    EXPECT_TRUE(st.IsCapacityError()) << st.ToString();
    EXPECT_EQ(pool.bytes_allocated(), 512);

    pool.Free(buf, 512);
}

TEST(ArrowMemoryPoolTest, PoolBufferDestructionOrderSafetyUnderASan) {
    // Mirrors ArrowScanner::decode_and_convert_discrete_message(): the pool is declared BEFORE the batches
    // whose PoolBuffers free into it, so the batches are destroyed first.
    struct ScannerSimulatedState {
        std::unique_ptr<ArrowBoundedMemoryPool> pool;
        std::shared_ptr<arrow::RecordBatch> batch;
    };

    ScannerSimulatedState state;
    state.pool = std::make_unique<ArrowBoundedMemoryPool>(64 * 1024);
    auto buf_res = arrow::AllocateBuffer(4096, state.pool.get());
    ASSERT_TRUE(buf_res.ok());
    std::shared_ptr<arrow::Buffer> pool_buf = std::move(buf_res).ValueOrDie();
    std::memset(pool_buf->mutable_data(), 0x2A, 4096);

    auto field = arrow::field("x", arrow::int32());
    auto schema = arrow::schema({field});
    auto array_data = arrow::ArrayData::Make(arrow::int32(), 1024, {nullptr, pool_buf}, 0);
    auto array = arrow::MakeArray(array_data);
    state.batch = arrow::RecordBatch::Make(schema, 1024, {array});
    pool_buf.reset();
    array.reset();
    array_data.reset();

    EXPECT_EQ(state.pool->bytes_allocated(), 4096);
    // When state is destroyed (or batch is reset before pool), ~PoolBuffer() frees into live pool.
    state.batch.reset();
    EXPECT_EQ(state.pool->bytes_allocated(), 0);
    state.pool.reset();
}

TEST(ArrowMemoryPoolTest, OwningByteBufferKeepsSliceAliveAfterCallerReset) {
    auto byte_buf = std::make_shared<TestByteBuffer>(64);
    const char payload[] = "starrocks-arrow-zero-copy";
    byte_buf->put_bytes(payload, sizeof(payload));
    byte_buf->flip_to_read();

    std::shared_ptr<arrow::Buffer> root_buf = make_owning_arrow_buffer(byte_buf);
    std::shared_ptr<arrow::Buffer> slice_buf = arrow::SliceBuffer(root_buf, 10, 5);

    byte_buf.reset();
    root_buf.reset();

    ASSERT_EQ(slice_buf->size(), 5);
    EXPECT_EQ(std::memcmp(slice_buf->data(), "arrow", 5), 0);
}

TEST(ArrowMemoryPoolTest, OverflowingAllocateAndReallocateReturnCapacityErrorWithoutCallingDelegate) {
    class CountingMemoryPool final : public arrow::MemoryPool {
    public:
        using arrow::MemoryPool::Allocate;
        using arrow::MemoryPool::Free;
        using arrow::MemoryPool::Reallocate;

        arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
            ++allocate_calls;
            return arrow::default_memory_pool()->Allocate(size, alignment, out);
        }
        arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override {
            ++reallocate_calls;
            return arrow::default_memory_pool()->Reallocate(old_size, new_size, alignment, ptr);
        }
        void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
            arrow::default_memory_pool()->Free(buffer, size, alignment);
        }
        int64_t bytes_allocated() const override { return arrow::default_memory_pool()->bytes_allocated(); }
        int64_t max_memory() const override { return arrow::default_memory_pool()->max_memory(); }
        int64_t total_bytes_allocated() const override { return arrow::default_memory_pool()->total_bytes_allocated(); }
        int64_t num_allocations() const override { return arrow::default_memory_pool()->num_allocations(); }
        std::string backend_name() const override { return "counting"; }

        int64_t allocate_calls{0};
        int64_t reallocate_calls{0};
    };

    CountingMemoryPool delegate;
    ArrowBoundedMemoryPool pool(64LL * 1024 * 1024, &delegate);

    uint8_t* buf1 = nullptr;
    ASSERT_TRUE(pool.Allocate(4096, &buf1).ok());
    ASSERT_NE(buf1, nullptr);
    EXPECT_EQ(pool.bytes_allocated(), 4096);
    EXPECT_EQ(delegate.allocate_calls, 1);

    // 4096 + (INT64_MAX - 1023) would overflow signed int64_t to a negative number if added before checking.
    uint8_t* overflow_buf = nullptr;
    arrow::Status alloc_st = pool.Allocate(std::numeric_limits<int64_t>::max() - 1023, &overflow_buf);
    EXPECT_TRUE(alloc_st.IsCapacityError()) << alloc_st.ToString();
    EXPECT_EQ(delegate.allocate_calls, 1);
    EXPECT_EQ(pool.bytes_allocated(), 4096);

    // Reallocating a 2048-byte buffer (while 4096 total bytes are live) to INT64_MAX - 1023 has delta = INT64_MAX - 3071,
    // which would also overflow _bytes_allocated + delta in signed int64_t.
    arrow::Status realloc_st = pool.Reallocate(2048, std::numeric_limits<int64_t>::max() - 1023, &buf1);
    EXPECT_TRUE(realloc_st.IsCapacityError()) << realloc_st.ToString();
    EXPECT_EQ(delegate.reallocate_calls, 0);
    EXPECT_EQ(pool.bytes_allocated(), 4096);

    arrow::Status neg_old_st = pool.Reallocate(-1, 128, &buf1);
    EXPECT_TRUE(neg_old_st.IsInvalid()) << neg_old_st.ToString();
    EXPECT_EQ(delegate.reallocate_calls, 0);
    EXPECT_EQ(pool.bytes_allocated(), 4096);

    pool.Free(buf1, 4096);
    EXPECT_EQ(pool.bytes_allocated(), 0);
}

} // namespace starrocks
