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

#include "column/append_with_mask.h"
#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "gtest/gtest.h"

namespace starrocks {

TEST(AppendWithMaskCoreTest, appendFixedLengthWithPositiveAndNegativeMask) {
    auto src = Int32Column::create();
    for (int32_t value : {1, 2, 3, 4, 5}) {
        src->append(value);
    }
    const uint8_t mask[] = {1, 0, 1, 0, 1};

    auto positive = Int32Column::create();
    ASSERT_TRUE(append_with_mask<true>(positive.get(), *src, mask, 5).ok());
    EXPECT_EQ("[1, 3, 5]", positive->debug_string());

    auto negative = Int32Column::create();
    ASSERT_TRUE(append_with_mask<false>(negative.get(), *src, mask, 5).ok());
    EXPECT_EQ("[2, 4]", negative->debug_string());
}

TEST(AppendWithMaskCoreTest, appendBinaryColumnWithMask) {
    auto src = BinaryColumn::create();
    src->append("alpha");
    src->append("beta");
    src->append("gamma");
    src->append("delta");
    const uint8_t mask[] = {0, 1, 1, 0};

    auto dst = BinaryColumn::create();
    ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask, 4).ok());
    EXPECT_EQ("['beta', 'gamma']", dst->debug_string());
}

TEST(AppendWithMaskCoreTest, appendNullableColumnWithMask) {
    auto src = NullableColumn::create(Int32Column::create(), NullColumn::create());
    src->append_datum(Datum(static_cast<int32_t>(10)));
    src->append_nulls(1);
    src->append_datum(Datum(static_cast<int32_t>(30)));
    const uint8_t mask[] = {1, 1, 0};

    auto dst = NullableColumn::create(Int32Column::create(), NullColumn::create());
    ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask, 3).ok());

    ASSERT_EQ(2, dst->size());
    EXPECT_FALSE(dst->is_null(0));
    EXPECT_EQ(10, dst->get(0).get_int32());
    EXPECT_TRUE(dst->is_null(1));
}

} // namespace starrocks
