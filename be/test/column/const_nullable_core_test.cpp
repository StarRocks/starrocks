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

#include "column/const_column.h"
#include "column/nullable_column.h"

namespace starrocks {

TEST(ConstNullableCoreTest, ConstColumnSelectiveAndFilter) {
    auto data = Int32Column::create();
    data->append(7);

    auto col = ConstColumn::create(data, 4);
    ASSERT_EQ(4, col->size());

    auto dst = Int32Column::create();
    const uint32_t indexes[] = {0, 0, 0};
    dst->append_selective(*col, indexes, 0, 3);
    ASSERT_EQ(3, dst->size());
    EXPECT_EQ(7, dst->get(0).get_int32());
    EXPECT_EQ(7, dst->get(1).get_int32());
    EXPECT_EQ(7, dst->get(2).get_int32());

    Filter keep = {1, 0, 1, 0};
    col->filter(keep);
    ASSERT_EQ(2, col->size());
    EXPECT_EQ(7, col->get(0).get_int32());
    EXPECT_EQ(7, col->get(1).get_int32());
}

TEST(ConstNullableCoreTest, NullableSelectiveAndNullAccounting) {
    auto data = Int32Column::create();
    data->append(10);
    data->append(20);
    data->append(30);

    auto nulls = NullColumn::create();
    nulls->append(0);
    nulls->append(1);
    nulls->append(0);

    auto src = NullableColumn::create(data, nulls);
    ASSERT_TRUE(src->has_null());
    EXPECT_EQ(1, src->null_count());

    auto dst = NullableColumn::create(Int32Column::create(), NullColumn::create());
    const uint32_t indexes[] = {2, 1, 0};
    dst->append_selective(*src, indexes, 0, 3);

    ASSERT_EQ(3, dst->size());
    EXPECT_FALSE(dst->is_null(0));
    EXPECT_TRUE(dst->is_null(1));
    EXPECT_FALSE(dst->is_null(2));
    EXPECT_EQ(30, dst->get(0).get_int32());
    EXPECT_EQ(10, dst->get(2).get_int32());
}

} // namespace starrocks
