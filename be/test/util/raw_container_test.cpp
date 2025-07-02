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

#include "util/raw_container.h"

#include <gtest/gtest.h>

#include "runtime/memory/column_allocator.h"

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
} // namespace starrocks