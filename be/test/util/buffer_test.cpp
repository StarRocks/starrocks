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

#include "util/buffer.h"

#include <gtest/gtest.h>

#include <vector>

#include "runtime/memory/memory_allocator.h"

namespace starrocks {

class BufferTest : public testing::Test {
public:
    BufferTest() = default;
    ~BufferTest() override = default;

protected:
    memory::JemallocAllocator<false> _allocator;
};
template <class T, size_t padding>
using RawBuffer = util::RawBuffer<T, padding>;
template <class T, size_t padding>
using Buffer = util::Buffer<T, padding>;

namespace {

struct CountingType {
    static inline int ctor_count = 0;
    static inline int dtor_count = 0;
    static inline int copy_count = 0;
    static inline int move_count = 0;

    int value = 0;

    CountingType() { ++ctor_count; }
    explicit CountingType(int v) : value(v) { ++ctor_count; }
    CountingType(const CountingType& other) : value(other.value) { ++copy_count; }
    CountingType(CountingType&& other) noexcept : value(other.value) { ++move_count; }
    CountingType& operator=(const CountingType&) = default;
    CountingType& operator=(CountingType&&) = default;
    ~CountingType() { ++dtor_count; }

    static void reset() {
        ctor_count = 0;
        dtor_count = 0;
        copy_count = 0;
        move_count = 0;
    }
};

} // namespace

// Test Buffer basic operations
TEST_F(BufferTest, BufferBasic) {
    Buffer<int, 16> buf(&_allocator);

    ASSERT_TRUE(buf.empty());
    ASSERT_EQ(0, buf.size());

    // Push back elements
    for (int i = 0; i < 10; ++i) {
        buf.push_back(i);
        ASSERT_EQ(i + 1, buf.size());
        ASSERT_EQ(i, buf[i]);
    }

    // Pop back elements
    for (int i = 9; i >= 0; --i) {
        ASSERT_EQ(i, buf[i]);
        buf.pop_back();
        ASSERT_EQ(i, buf.size());
    }
    ASSERT_TRUE(buf.empty());
}

// Test Buffer reserve and resize
TEST_F(BufferTest, BufferReserveResize) {
    Buffer<int, 16> buf(&_allocator);

    buf.reserve(20);
    ASSERT_GE(buf.capacity(), 20);

    buf.resize(10);
    ASSERT_EQ(10, buf.size());

    buf.resize(15);
    ASSERT_EQ(15, buf.size());
}

// Test Buffer assign
TEST_F(BufferTest, BufferAssign) {
    Buffer<int, 16> buf(&_allocator);

    buf.assign(5, 42);
    ASSERT_EQ(5, buf.size());
    for (size_t i = 0; i < buf.size(); ++i) {
        ASSERT_EQ(42, buf[i]);
    }

    std::vector<int> vec = {1, 2, 3};
    buf.assign(vec.begin(), vec.end());
    ASSERT_EQ(3, buf.size());
    ASSERT_EQ(1, buf[0]);
    ASSERT_EQ(2, buf[1]);
    ASSERT_EQ(3, buf[2]);

    buf.assign({10, 20});
    ASSERT_EQ(2, buf.size());
    ASSERT_EQ(10, buf[0]);
    ASSERT_EQ(20, buf[1]);
}

// Test Buffer append
TEST_F(BufferTest, BufferAppend) {
    Buffer<int, 16> buf(&_allocator);

    buf.push_back(1);
    buf.push_back(3);

    auto it = buf.append(1, 0);
    ASSERT_EQ(0, *it);
    ASSERT_EQ(3, buf.size());
    ASSERT_EQ(1, buf[0]);
    ASSERT_EQ(3, buf[1]);
    ASSERT_EQ(0, buf[2]);

    it = buf.append(1, 2);
    ASSERT_EQ(2, *it);
    ASSERT_EQ(4, buf.size());
    ASSERT_EQ(1, buf[0]);
    ASSERT_EQ(3, buf[1]);
    ASSERT_EQ(0, buf[2]);
    ASSERT_EQ(2, buf[3]);

    // Append with initializer_list at end
    buf.append({10, 11});
    ASSERT_EQ(6, buf.size());
    ASSERT_EQ(10, buf[4]);
    ASSERT_EQ(11, buf[5]);
}

// Test Buffer swap
TEST_F(BufferTest, BufferSwap) {
    Buffer<int, 16> buf1(&_allocator);
    Buffer<int, 16> buf2(&_allocator);

    for (int i = 0; i < 5; ++i) {
        buf1.push_back(i);
    }

    size_t size1 = buf1.size();

    buf1.swap(buf2);

    ASSERT_EQ(0, buf1.size());
    ASSERT_EQ(size1, buf2.size());

    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(i, buf2[i]);
    }
}

// Test Buffer clear
TEST_F(BufferTest, BufferClear) {
    Buffer<int, 16> buf(&_allocator);

    for (int i = 0; i < 10; ++i) {
        buf.push_back(i);
    }

    ASSERT_EQ(10, buf.size());
    size_t cap = buf.capacity();

    buf.clear();
    ASSERT_EQ(0, buf.size());
    ASSERT_EQ(cap, buf.capacity()); // Capacity should remain
}

// Test Buffer shrink_to_fit
TEST_F(BufferTest, BufferShrinkToFit) {
    Buffer<int, 16> buf(&_allocator);

    buf.reserve(100);
    size_t large_cap = buf.capacity();

    for (int i = 0; i < 10; ++i) {
        buf.push_back(i);
    }

    buf.shrink_to_fit();
    ASSERT_EQ(10, buf.size());
    ASSERT_LE(buf.capacity(), large_cap);
    ASSERT_GE(buf.capacity(), 10);
}

// Test Buffer move semantics
TEST_F(BufferTest, BufferMove) {
    Buffer<int, 16> buf1(&_allocator);

    for (int i = 0; i < 10; ++i) {
        buf1.push_back(i);
    }

    size_t size1 = buf1.size();
    size_t cap1 = buf1.capacity();

    Buffer<int, 16> buf2(std::move(buf1));

    ASSERT_EQ(0, buf1.size());
    ASSERT_EQ(size1, buf2.size());
    ASSERT_EQ(cap1, buf2.capacity());

    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(i, buf2[i]);
    }
}

// Test Buffer release
TEST_F(BufferTest, BufferRelease) {
    Buffer<int, 16> buf(&_allocator);

    buf.reserve(10);
    ASSERT_GE(buf.capacity(), 10);

    buf.release();
    ASSERT_EQ(0, buf.size());
    ASSERT_EQ(0, buf.capacity());
}

// Test Buffer with different types
TEST_F(BufferTest, BufferDifferentTypes) {
    Buffer<double, 16> double_buf(&_allocator);
    for (int i = 0; i < 5; ++i) {
        double_buf.push_back(i * 1.5);
    }
    ASSERT_EQ(5, double_buf.size());
    ASSERT_DOUBLE_EQ(0.0, double_buf[0]);
    ASSERT_DOUBLE_EQ(6.0, double_buf[4]);

    Buffer<std::string, 16> str_buf(&_allocator);
    str_buf.push_back("hello");
    str_buf.push_back("world");
    ASSERT_EQ(2, str_buf.size());
    ASSERT_EQ("hello", str_buf[0]);
    ASSERT_EQ("world", str_buf[1]);
}

// Test Buffer resize(count, value) for non-trivial type
TEST_F(BufferTest, BufferResizeValueNonTrivial) {
    CountingType::reset();
    Buffer<CountingType, 16> buf(&_allocator);

    buf.resize(3, CountingType(7));
    ASSERT_EQ(3, buf.size());
    for (size_t i = 0; i < buf.size(); ++i) {
        ASSERT_EQ(7, buf[i].value);
    }

    buf.resize(1);
    ASSERT_EQ(1, buf.size());
    ASSERT_GE(CountingType::dtor_count, 2);
}

// Test Buffer append with iterator ranges and empty ranges
TEST_F(BufferTest, BufferAppendRange) {
    Buffer<int, 16> buf(&_allocator);
    std::vector<int> vec = {1, 2, 3};

    auto it = buf.append(vec.begin(), vec.end());
    ASSERT_EQ(3, buf.size());
    ASSERT_EQ(1, *it);
    ASSERT_EQ(3, buf[2]);

    it = buf.append(vec.begin(), vec.begin());
    ASSERT_EQ(buf.end(), it);
    ASSERT_EQ(3, buf.size());
}

// Test Buffer reserve/shrink/pop on no-op paths
TEST_F(BufferTest, BufferNoopPaths) {
    Buffer<int, 16> buf(&_allocator);

    buf.reserve(8);
    size_t cap = buf.capacity();
    buf.reserve(4);
    ASSERT_EQ(cap, buf.capacity());

    buf.resize(cap);
    buf.shrink_to_fit();
    ASSERT_EQ(cap, buf.capacity());

    buf.pop_back();
    ASSERT_EQ(cap - 1, buf.size());
}

// Test Buffer move assignment and allocator swap
TEST_F(BufferTest, BufferMoveAssignAndAllocatorSwap) {
    memory::JemallocAllocator<false> allocator2;
    Buffer<int, 16> buf1(&_allocator);
    Buffer<int, 16> buf2(&allocator2);

    for (int i = 0; i < 3; ++i) {
        buf1.push_back(i);
    }
    buf2 = std::move(buf1);
    ASSERT_EQ(3, buf2.size());
    ASSERT_EQ(0, buf1.size());

    ASSERT_EQ(&_allocator, buf2.allocator());
    ASSERT_EQ(&allocator2, buf1.allocator());

    buf1.swap(buf2);
    ASSERT_EQ(&allocator2, buf2.allocator());
    ASSERT_EQ(&_allocator, buf1.allocator());
}

// Test Buffer emplace_back and rvalue push_back
TEST_F(BufferTest, BufferEmplaceAndRvalue) {
    CountingType::reset();
    Buffer<CountingType, 16> buf(&_allocator);

    buf.emplace_back(1);
    buf.push_back(CountingType(2));

    ASSERT_EQ(2, buf.size());
    ASSERT_EQ(1, buf[0].value);
    ASSERT_EQ(2, buf[1].value);
    ASSERT_GE(CountingType::move_count, 1);
}

// Test padding parameter
TEST_F(BufferTest, BufferPadding) {
    constexpr size_t element_size = sizeof(int);
    constexpr int num_elements = 10;

    // Test different padding values
    // padding = 0: kPadding should be 0 (aligned to element_size)
    Buffer<int, 0> buf0(&_allocator);
    for (int i = 0; i < num_elements; ++i) {
        buf0.push_back(i);
    }
    size_t expected_padding0 = ((0 + element_size - 1) / element_size) * element_size;
    size_t expected_allocated0 = buf0.capacity() * element_size + expected_padding0;
    ASSERT_EQ(expected_allocated0, buf0.allocated_bytes());
    size_t padding0 = RawBuffer<int, 0>::kPadding;
    ASSERT_EQ(expected_padding0, padding0);

    // padding = 1: kPadding should be aligned to element_size (4 for int)
    Buffer<int, 1> buf1(&_allocator);
    for (int i = 0; i < num_elements; ++i) {
        buf1.push_back(i);
    }
    size_t expected_padding1 = ((1 + element_size - 1) / element_size) * element_size;
    size_t expected_allocated1 = buf1.capacity() * element_size + expected_padding1;
    ASSERT_EQ(expected_allocated1, buf1.allocated_bytes());
    size_t padding1 = RawBuffer<int, 1>::kPadding;
    ASSERT_EQ(expected_padding1, padding1);
    ASSERT_EQ(element_size, padding1); // Should be 4 for int

    // padding = 8: kPadding should be 8
    Buffer<int, 8> buf8(&_allocator);
    for (int i = 0; i < num_elements; ++i) {
        buf8.push_back(i);
    }
    size_t expected_padding8 = ((8 + element_size - 1) / element_size) * element_size;
    size_t expected_allocated8 = buf8.capacity() * element_size + expected_padding8;
    ASSERT_EQ(expected_allocated8, buf8.allocated_bytes());
    size_t padding8 = RawBuffer<int, 8>::kPadding;
    ASSERT_EQ(expected_padding8, padding8);
    ASSERT_EQ(8, padding8);

    // padding = 16: kPadding should be 16
    Buffer<int, 16> buf16(&_allocator);
    for (int i = 0; i < num_elements; ++i) {
        buf16.push_back(i);
    }
    size_t expected_padding16 = ((16 + element_size - 1) / element_size) * element_size;
    size_t expected_allocated16 = buf16.capacity() * element_size + expected_padding16;
    ASSERT_EQ(expected_allocated16, buf16.allocated_bytes());
    size_t padding16 = RawBuffer<int, 16>::kPadding;
    ASSERT_EQ(expected_padding16, padding16);
    ASSERT_EQ(16, padding16);

    // padding = 32: kPadding should be 32
    Buffer<int, 32> buf32(&_allocator);
    for (int i = 0; i < num_elements; ++i) {
        buf32.push_back(i);
    }
    size_t expected_padding32 = ((32 + element_size - 1) / element_size) * element_size;
    size_t expected_allocated32 = buf32.capacity() * element_size + expected_padding32;
    ASSERT_EQ(expected_allocated32, buf32.allocated_bytes());
    size_t padding32 = RawBuffer<int, 32>::kPadding;
    ASSERT_EQ(expected_padding32, padding32);
    ASSERT_EQ(32, padding32);

    // Verify all buffers have the same size and data
    ASSERT_EQ(num_elements, buf0.size());
    ASSERT_EQ(num_elements, buf1.size());
    ASSERT_EQ(num_elements, buf8.size());
    ASSERT_EQ(num_elements, buf16.size());
    ASSERT_EQ(num_elements, buf32.size());

    for (int i = 0; i < num_elements; ++i) {
        ASSERT_EQ(i, buf0[i]);
        ASSERT_EQ(i, buf1[i]);
        ASSERT_EQ(i, buf8[i]);
        ASSERT_EQ(i, buf16[i]);
        ASSERT_EQ(i, buf32[i]);
    }

    // allocated_bytes = capacity * element_size + kPadding; capacity depends on growth path
    // (doubling), so with push_back-only fill it is not monotonic in padding. To compare
    // across padding, use the same capacity (e.g. reserve first).
    ASSERT_EQ(buf0.allocated_bytes(), buf0.capacity() * element_size + padding0);
    ASSERT_EQ(buf1.allocated_bytes(), buf1.capacity() * element_size + padding1);
    ASSERT_EQ(buf8.allocated_bytes(), buf8.capacity() * element_size + padding8);
    ASSERT_EQ(buf16.allocated_bytes(), buf16.capacity() * element_size + padding16);
    ASSERT_EQ(buf32.allocated_bytes(), buf32.capacity() * element_size + padding32);
}

} // namespace starrocks
