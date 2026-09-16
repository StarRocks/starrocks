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

#include <limits>
#include <string>
#include <vector>

#include "column/binary_column.h"

namespace starrocks {
namespace {

constexpr uint64_t kLargeOffset = static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;

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

TEST(BinaryColumnCoreTest, StickyLargeOffsetsRemainBinaryAndAppend) {
    auto src = BinaryColumn::create();
    src->append("a");
    src->append("bb");
    src->append("ccc");
    src->get_offset().ensure_width_for_value(kLargeOffset);
    ASSERT_TRUE(src->get_offset().is_large());
    EXPECT_TRUE(src->is_binary());
    EXPECT_FALSE(src->has_large_column());
    EXPECT_EQ(6, src->byte_size(1));

    auto upgraded = src->upgrade_if_overflow();
    ASSERT_TRUE(upgraded.ok());
    ASSERT_TRUE(upgraded.value() == nullptr);

    auto dst = BinaryColumn::create();
    dst->append(*src, 1, 2);
    ASSERT_EQ(2, dst->size());
    EXPECT_EQ("bb", slice_to_string(dst->get_slice(0)));
    EXPECT_EQ("ccc", slice_to_string(dst->get_slice(1)));

    const uint32_t selected_indexes[] = {2, 0, 1};
    auto selected = BinaryColumn::create();
    selected->append_selective(*src, selected_indexes, 0, 2);
    ASSERT_EQ(2, selected->size());
    EXPECT_EQ("ccc", slice_to_string(selected->get_slice(0)));
    EXPECT_EQ("a", slice_to_string(selected->get_slice(1)));

    auto large_dst = LargeBinaryColumn::create();
    large_dst->append(*src, 0, 2);
    ASSERT_EQ(2, large_dst->size());
    EXPECT_EQ("a", slice_to_string(large_dst->get_slice(0)));
    EXPECT_EQ("bb", slice_to_string(large_dst->get_slice(1)));

    Buffer<uint32_t> repeats = {0, 2, 2, 3};
    auto replicated = src->replicate(repeats);
    ASSERT_TRUE(replicated.ok());
    ASSERT_EQ(3, replicated.value()->size());
    EXPECT_EQ("a", replicated.value()->get(0).get_slice().to_string());
    EXPECT_EQ("a", replicated.value()->get(1).get_slice().to_string());
    EXPECT_EQ("ccc", replicated.value()->get(2).get_slice().to_string());
}

TEST(BinaryColumnCoreTest, StickyLargeBinaryDowngradesWhenOffsetsFit) {
    auto src = LargeBinaryColumn::create();
    src->append("x");
    src->append("yy");
    src->get_offset().ensure_width_for_value(kLargeOffset);
    ASSERT_TRUE(src->get_offset().is_large());

    auto downgraded = src->downgrade();
    ASSERT_TRUE(downgraded.ok());
    ASSERT_TRUE(downgraded.value() != nullptr);
    EXPECT_TRUE(downgraded.value()->is_binary());
    ASSERT_EQ(2, downgraded.value()->size());
    EXPECT_EQ("x", downgraded.value()->get(0).get_slice().to_string());
    EXPECT_EQ("yy", downgraded.value()->get(1).get_slice().to_string());
}

TEST(BinaryColumnCoreTest, AppendValueMultipleTimesHandlesStickyLargeOffsets) {
    auto src = BinaryColumn::create();
    src->append("a");
    src->append("bbbb");
    src->get_offset().ensure_width_for_value(kLargeOffset);

    auto dst = BinaryColumn::create();
    dst->get_offset().ensure_width_for_value(kLargeOffset);
    dst->append_value_multiple_times(*src, 1, 3);

    ASSERT_TRUE(dst->get_offset().is_large());
    ASSERT_EQ(3, dst->size());
    EXPECT_EQ("bbbb", slice_to_string(dst->get_slice(0)));
    EXPECT_EQ("bbbb", slice_to_string(dst->get_slice(1)));
    EXPECT_EQ("bbbb", slice_to_string(dst->get_slice(2)));
}

} // namespace starrocks
