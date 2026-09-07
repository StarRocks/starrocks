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

#include "base/container/raw_container.h"

#include <gtest/gtest.h>

#include "common/memory/column_allocator.h"

namespace starrocks {

TEST(TestRawContainer, testResizeWithStdAllocator) {
    std::vector<int> v;
    raw::make_room(&v, 5);
    ASSERT_EQ(v.size(), 5);
    raw::stl_vector_resize_uninitialized(&v, 10);
    ASSERT_EQ(v.size(), 10);
}

TEST(TestRawContainer, testResizeWithColumnAllocator) {
    std::vector<int, ColumnAllocator<int>> v;
    raw::make_room(&v, 5);
    ASSERT_EQ(v.size(), 5);
    raw::stl_vector_resize_uninitialized(&v, 10);
    ASSERT_EQ(v.size(), 10);
}

// RawStringPage exists so O_DIRECT writers can hand their buffer straight to pwritev, which
// rejects an unaligned iov_base with EINVAL. A plain RawString only guarantees max_align_t.
//
// The guarantee is conditional, and the condition is easy to miss: basic_string keeps short
// contents inside the object itself, which never reaches the allocator, so data() is only
// page-aligned once the buffer is big enough to be heap-allocated (measured threshold on
// libstdc++: contents of 16 bytes and up). Every O_DIRECT caller clears that by construction
// because it resizes to ALIGN_UP(n, page) -- so assert the heap sizes hold the alignment, and
// assert separately that the page-rounded sizes callers actually request always do.
TEST(TestRawContainer, rawStringPageIsPageAligned) {
    constexpr size_t kPageSize = 4096;
    constexpr size_t kInlineCapacity = 15;

    const std::vector<size_t> heap_sizes = {kInlineCapacity + 1, 100,           kPageSize - 1, kPageSize,
                                            kPageSize + 1,       4 * kPageSize, 1024 * 1024};
    for (size_t n : heap_sizes) {
        raw::RawStringPage s;
        s.resize(n);
        ASSERT_EQ(n, s.size());
        EXPECT_EQ(0u, reinterpret_cast<uintptr_t>(s.data()) % kPageSize) << "unaligned at size " << n;
    }

    // Growing an existing buffer must not lose the alignment either: the spill path resizes the
    // same SerdeContext buffer for every chunk.
    raw::RawStringPage grown;
    for (size_t n = kInlineCapacity + 1; n <= 64 * 1024; n *= 2) {
        grown.resize(n);
        EXPECT_EQ(0u, reinterpret_cast<uintptr_t>(grown.data()) % kPageSize) << "unaligned after growing to " << n;
    }

    // What the O_DIRECT path actually asks for: any payload length rounded up to a page. This is
    // the invariant pwritev depends on, including for payloads far below the inline capacity.
    for (size_t payload : {size_t{1}, size_t{12}, kInlineCapacity, kPageSize, kPageSize + 1}) {
        raw::RawStringPage s;
        s.resize((payload + kPageSize - 1) / kPageSize * kPageSize);
        EXPECT_EQ(0u, reinterpret_cast<uintptr_t>(s.data()) % kPageSize) << "unaligned for payload " << payload;
    }
}
} // namespace starrocks