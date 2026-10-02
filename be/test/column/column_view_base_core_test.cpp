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

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/column_view/column_view.h"
#include "column/nullable_column.h"
#include "gutil/casts.h"

namespace starrocks {
namespace {

std::string slice_to_string(const Slice& s) {
    return std::string(s.data, s.size);
}

} // namespace

TEST(ColumnViewBaseCoreTest, AppendSelectiveToWithoutConcat) {
    auto view = ColumnView::create(BinaryColumn::create(), 1, -1);

    auto src0 = BinaryColumn::create();
    src0->append("a");
    src0->append("b");
    src0->append("c");
    view->append(*src0, 0, 2);

    auto src1 = BinaryColumn::create();
    src1->append("x");
    src1->append("y");
    const uint32_t selective[] = {1, 0};
    view->append_selective(*src1, selective, 0, 2);

    auto dst = BinaryColumn::create();
    const uint32_t indexes[] = {0, 1, 2, 3};
    view->append_selective_to(*dst, indexes, 0, 4);

    ASSERT_EQ(4, dst->size());
    EXPECT_EQ("a", slice_to_string(dst->get_slice(0)));
    EXPECT_EQ("b", slice_to_string(dst->get_slice(1)));
    EXPECT_EQ("y", slice_to_string(dst->get_slice(2)));
    EXPECT_EQ("x", slice_to_string(dst->get_slice(3)));
}

TEST(ColumnViewBaseCoreTest, AppendSelectiveToWithConcatAndNulls) {
    auto view = ColumnView::create(NullableColumn::create(BinaryColumn::create(), NullColumn::create()), -1, -1);

    auto data = BinaryColumn::create();
    data->append("v0");
    data->append("ignored");

    auto nulls = NullColumn::create();
    nulls->append(0);
    nulls->append(1);

    auto src = NullableColumn::create(data, nulls);
    view->append(*src, 0, 2);

    auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    const uint32_t indexes[] = {0, 1};
    view->append_selective_to(*dst, indexes, 0, 2);

    ASSERT_EQ(2, dst->size());
    EXPECT_FALSE(dst->is_null(0));
    EXPECT_TRUE(dst->is_null(1));

    auto* data_column = down_cast<BinaryColumn*>(dst->data_column_raw_ptr());
    EXPECT_EQ("v0", slice_to_string(data_column->get_slice(0)));
}

// ColumnViewBase::capacity_limit_reached() used to throw. It must return a Status for a view with or without a
// concatenated column, without forcing the deferred concatenation.
TEST(ColumnViewBaseCoreTest, CapacityLimitReached) {
    auto src = BinaryColumn::create();
    src->append("a");
    src->append("b");

    // Concatenation disabled by the rows limit: reads stay on the habitats.
    auto view = ColumnView::create(BinaryColumn::create(), 1, -1);
    view->append(*src, 0, 2);
    ASSERT_OK(view->capacity_limit_reached());
    auto dst = BinaryColumn::create();
    const uint32_t indexes[] = {0, 1};
    view->append_selective_to(*dst, indexes, 0, 2);
    ASSERT_OK(view->capacity_limit_reached());

    // Both limits are -1, so the first read concatenates the rows into _concat_column, which is then checked.
    auto concat_view = ColumnView::create(BinaryColumn::create(), -1, -1);
    concat_view->append(*src, 0, 2);
    ASSERT_OK(concat_view->capacity_limit_reached());
    auto concat_dst = BinaryColumn::create();
    concat_view->append_selective_to(*concat_dst, indexes, 0, 2);
    ASSERT_OK(concat_view->capacity_limit_reached());
    ASSERT_EQ(2, concat_dst->size());
    EXPECT_EQ("b", slice_to_string(concat_dst->get_slice(1)));
}

// The row count of a view is counted on append, before the deferred tasks read the source, so a view over a tiny
// source can claim more rows than the uint32_t positions can address. The view is never read, so no row is touched.
TEST(ColumnViewBaseCoreTest, CapacityLimitReachedRejectsTooManyRows) {
    const uint64_t capacity_limit = Column::MAX_CAPACITY_LIMIT;
    auto src = BinaryColumn::create();
    src->append("a");

    auto view = ColumnView::create(BinaryColumn::create(), 0, -1);
    view->append(*src, 0, capacity_limit);
    ASSERT_OK(view->capacity_limit_reached());
    view->append(*src, 0, 1);
    auto status = view->capacity_limit_reached();
    ASSERT_TRUE(status.is_capacity_limit_exceeded()) << status;
}

} // namespace starrocks
