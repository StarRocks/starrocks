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

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "column/binary_column.h"

namespace starrocks {
namespace {

std::string slice_to_string(const Slice& s) {
    return std::string(s.data, s.size);
}

} // namespace

TEST(BinaryColumnCoreTest, BinaryAppendSelectiveAndSerialize) {
    auto src = BinaryColumn::create();
    src->append("alpha");
    src->append("beta");
    src->append("gamma");
    src->append("delta");

    auto dst = BinaryColumn::create();
    const uint32_t indexes[] = {3, 1, 0, 2};
    dst->append_selective(*src, indexes, 1, 2);
    ASSERT_EQ(2, dst->size());
    EXPECT_EQ("beta", slice_to_string(dst->get_slice(0)));
    EXPECT_EQ("alpha", slice_to_string(dst->get_slice(1)));

    std::vector<uint8_t> serialized(src->serialize_size(2));
    src->serialize(2, serialized.data());
    auto restored = BinaryColumn::create();
    restored->deserialize_and_append(serialized.data());
    ASSERT_EQ(1, restored->size());
    EXPECT_EQ("gamma", slice_to_string(restored->get_slice(0)));
}

TEST(BinaryColumnCoreTest, LargeBinaryAppendSelective) {
    auto src = LargeBinaryColumn::create();
    src->append("long-value-1");
    src->append("long-value-2");
    src->append("long-value-3");

    auto dst = LargeBinaryColumn::create();
    const uint32_t indexes[] = {2, 0, 1};
    dst->append_selective(*src, indexes, 0, 2);
    ASSERT_EQ(2, dst->size());
    EXPECT_EQ("long-value-3", slice_to_string(dst->get_slice(0)));
    EXPECT_EQ("long-value-1", slice_to_string(dst->get_slice(1)));
}

} // namespace starrocks
