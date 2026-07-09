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

#include "column/adaptive_offsets.h"

#include <gtest/gtest.h>

#include <limits>
#include <utility>

namespace starrocks {
namespace {

constexpr uint64_t kUint32Max = std::numeric_limits<uint32_t>::max();
constexpr uint64_t kLargeOffset = kUint32Max + 1;

} // namespace

TEST(AdaptiveOffsetsTest, ScalarAppendAndPromotionPreserveValues) {
    AdaptiveOffsets offsets;
    EXPECT_FALSE(offsets.is_large());
    EXPECT_TRUE(offsets.empty());

    offsets.push_back(0);
    offsets.push_back(7);
    offsets.push_back(kUint32Max);
    ASSERT_FALSE(offsets.is_large());
    ASSERT_EQ(3, offsets.size());
    EXPECT_EQ(0, offsets[0]);
    EXPECT_EQ(7, offsets[1]);
    EXPECT_EQ(kUint32Max, offsets[2]);

    offsets.push_back(kLargeOffset);
    ASSERT_TRUE(offsets.is_large());
    ASSERT_EQ(4, offsets.size());
    EXPECT_EQ(0, offsets[0]);
    EXPECT_EQ(7, offsets[1]);
    EXPECT_EQ(kUint32Max, offsets[2]);
    EXPECT_EQ(kLargeOffset, offsets[3]);
    EXPECT_EQ(sizeof(uint64_t), offsets.element_size());
}

TEST(AdaptiveOffsetsTest, EnsureWidthPreservesExistingValues) {
    AdaptiveOffsets offsets;
    offsets.push_back(0);
    offsets.push_back(11);

    offsets.ensure_width_for_value(kLargeOffset);

    ASSERT_TRUE(offsets.is_large());
    ASSERT_EQ(2, offsets.size());
    EXPECT_EQ(0, offsets[0]);
    EXPECT_EQ(11, offsets[1]);
}

TEST(AdaptiveOffsetsTest, SetPromotesAndPreservesValues) {
    AdaptiveOffsets offsets;
    offsets.resize(3, 0);
    offsets.set(0, 3);
    offsets.set(1, 5);
    offsets.set(2, 8);

    offsets.set(1, kLargeOffset);

    ASSERT_TRUE(offsets.is_large());
    ASSERT_EQ(3, offsets.size());
    EXPECT_EQ(3, offsets[0]);
    EXPECT_EQ(kLargeOffset, offsets[1]);
    EXPECT_EQ(8, offsets[2]);
}

TEST(AdaptiveOffsetsTest, ResizeWithSmallFillStaysSmall) {
    AdaptiveOffsets offsets;
    offsets.push_back(0);
    offsets.resize(4, 9);

    ASSERT_FALSE(offsets.is_large());
    ASSERT_EQ(4, offsets.size());
    EXPECT_EQ(0, offsets[0]);
    EXPECT_EQ(9, offsets[1]);
    EXPECT_EQ(9, offsets[2]);
    EXPECT_EQ(9, offsets[3]);
}

TEST(AdaptiveOffsetsTest, ResizeWithLargeFillPromotesAndFillsNewSlots) {
    AdaptiveOffsets offsets;
    offsets.push_back(0);
    offsets.push_back(4);

    offsets.resize(5, kLargeOffset);

    ASSERT_TRUE(offsets.is_large());
    ASSERT_EQ(5, offsets.size());
    EXPECT_EQ(0, offsets[0]);
    EXPECT_EQ(4, offsets[1]);
    EXPECT_EQ(kLargeOffset, offsets[2]);
    EXPECT_EQ(kLargeOffset, offsets[3]);
    EXPECT_EQ(kLargeOffset, offsets[4]);
}

TEST(AdaptiveOffsetsTest, ResizeWithLargeFillShrinksBeforePromotion) {
    AdaptiveOffsets offsets;
    offsets.push_back(0);
    offsets.push_back(4);
    offsets.push_back(9);
    offsets.push_back(16);

    offsets.resize(2, kLargeOffset);

    ASSERT_TRUE(offsets.is_large());
    ASSERT_EQ(2, offsets.size());
    EXPECT_EQ(0, offsets[0]);
    EXPECT_EQ(4, offsets[1]);
}

TEST(AdaptiveOffsetsTest, AppendEmptyValuesZeroCountIsNoopOnEmptyStorage) {
    AdaptiveOffsets offsets;
    offsets.append_empty_values(0);

    EXPECT_FALSE(offsets.is_large());
    EXPECT_TRUE(offsets.empty());
}

TEST(AdaptiveOffsetsTest, AppendEmptyValuesRepeatsLastOffset) {
    AdaptiveOffsets offsets;
    offsets.push_back(0);
    offsets.push_back(12);

    offsets.append_empty_values(3);

    ASSERT_FALSE(offsets.is_large());
    ASSERT_EQ(5, offsets.size());
    EXPECT_EQ(0, offsets[0]);
    EXPECT_EQ(12, offsets[1]);
    EXPECT_EQ(12, offsets[2]);
    EXPECT_EQ(12, offsets[3]);
    EXPECT_EQ(12, offsets[4]);

    offsets.push_back(kLargeOffset);
    offsets.append_empty_values(2);

    ASSERT_TRUE(offsets.is_large());
    ASSERT_EQ(8, offsets.size());
    EXPECT_EQ(kLargeOffset, offsets[5]);
    EXPECT_EQ(kLargeOffset, offsets[6]);
    EXPECT_EQ(kLargeOffset, offsets[7]);
}

TEST(AdaptiveOffsetsTest, MakeRoomDiscardsExistingValuesAndSelectsWidth) {
    AdaptiveOffsets offsets;
    offsets.push_back(0);
    offsets.push_back(100);

    offsets.make_room(3, kLargeOffset);

    ASSERT_TRUE(offsets.is_large());
    ASSERT_EQ(3, offsets.size());
    offsets.large_storage()[0] = 1;
    offsets.large_storage()[1] = kLargeOffset;
    offsets.large_storage()[2] = kLargeOffset + 9;
    EXPECT_EQ(1, offsets[0]);
    EXPECT_EQ(kLargeOffset, offsets[1]);
    EXPECT_EQ(kLargeOffset + 9, offsets[2]);
}

TEST(AdaptiveOffsetsTest, ResizeUninitializedWithLargeMaxPreservesPrefixAndPromotesBeforeGrow) {
    AdaptiveOffsets offsets;
    offsets.push_back(0);
    offsets.push_back(5);

    offsets.resize_uninitialized(4, kLargeOffset);

    ASSERT_TRUE(offsets.is_large());
    ASSERT_EQ(4, offsets.size());
    EXPECT_EQ(0, offsets[0]);
    EXPECT_EQ(5, offsets[1]);
    offsets.large_storage()[2] = kLargeOffset;
    offsets.large_storage()[3] = kLargeOffset + 3;
    EXPECT_EQ(kLargeOffset, offsets[2]);
    EXPECT_EQ(kLargeOffset + 3, offsets[3]);
}

TEST(AdaptiveOffsetsTest, ResizeUninitializedWithLargeMaxShrinksBeforePromotion) {
    AdaptiveOffsets offsets;
    offsets.push_back(0);
    offsets.push_back(6);
    offsets.push_back(15);

    offsets.resize_uninitialized(2, kLargeOffset);

    ASSERT_TRUE(offsets.is_large());
    ASSERT_EQ(2, offsets.size());
    EXPECT_EQ(0, offsets[0]);
    EXPECT_EQ(6, offsets[1]);
}

TEST(AdaptiveOffsetsTest, ResetReturnsToSmallMode) {
    AdaptiveOffsets offsets;
    offsets.push_back(kLargeOffset);
    ASSERT_TRUE(offsets.is_large());

    offsets.reset();

    EXPECT_FALSE(offsets.is_large());
    EXPECT_TRUE(offsets.empty());
    offsets.push_back(1);
    EXPECT_FALSE(offsets.is_large());
    EXPECT_EQ(1, offsets.back());
}

TEST(AdaptiveOffsetsTest, WholeBufferTransferReplacesActiveStorage) {
    AdaptiveOffsets offsets;

    AdaptiveOffsets::Large large = {0, kLargeOffset};
    offsets.set_large_buffer(std::move(large));
    ASSERT_TRUE(offsets.is_large());
    ASSERT_EQ(2, offsets.size());
    EXPECT_EQ(kLargeOffset, offsets[1]);

    AdaptiveOffsets::Small small = {0, 3, 8};
    offsets.set_small_buffer(std::move(small));
    ASSERT_FALSE(offsets.is_large());
    ASSERT_EQ(3, offsets.size());
    EXPECT_EQ(8, offsets[2]);
}

} // namespace starrocks
