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

#include "column/column_helper.h"

#include "column/column_builder.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "gtest/gtest.h"
#include "gutil/casts.h"
#include "testutil/assert.h"
#include "testutil/column_test_helper.h"

namespace starrocks {

class ColumnHelperTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    MutableColumnPtr create_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append(Slice("v1"));
        return builder.build(false);
    }

    MutableColumnPtr create_nullable_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append(Slice("v1"), true);
        return builder.build(false);
    }

    MutableColumnPtr create_const_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append(Slice("v1"));
        return builder.build(true);
    }

    MutableColumnPtr create_only_null_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append_null();
        return builder.build(true);
    }
};

TEST_F(ColumnHelperTest, cast_to_nullable_column) {
    auto col = ColumnHelper::cast_to_nullable_column(create_column());
    ASSERT_TRUE(col->is_nullable());
    ASSERT_FALSE(col->is_constant());

    col = ColumnHelper::cast_to_nullable_column(create_nullable_column());
    ASSERT_TRUE(col->is_nullable());
    ASSERT_FALSE(col->is_constant());
}

TEST_F(ColumnHelperTest, align_return_type) {
    auto nullable_column = create_nullable_column();
    auto not_null_column = create_column();
    auto col_size = not_null_column->size();
    nullable_column->append(*ColumnHelper::align_return_type(
            std::move(not_null_column), TypeDescriptor::from_logical_type(TYPE_VARCHAR), col_size, true));
    ASSERT_TRUE(nullable_column->is_nullable());
    ASSERT_FALSE(nullable_column->is_constant());
}

TEST_F(ColumnHelperTest, get_data_column_by_type) {
    auto column = create_nullable_column();
    const auto* data_column = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(column.get());
    ASSERT_TRUE(data_column->is_binary());

    column = create_column();
    data_column = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(column.get());
    ASSERT_TRUE(data_column->is_binary());

    column = create_const_column();
    data_column = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(column.get());
    ASSERT_TRUE(data_column->is_binary());
}

TEST_F(ColumnHelperTest, get_null_column) {
    auto column = create_nullable_column();
    const auto* null_column = ColumnHelper::get_null_column(column.get());
    ASSERT_EQ(null_column->get_name(), "integral-1");

    column = create_column();
    null_column = ColumnHelper::get_null_column(column.get());
    ASSERT_TRUE(null_column == nullptr);

    column = create_const_column();
    null_column = ColumnHelper::get_null_column(column.get());
    ASSERT_EQ(null_column->get_name(), "integral-1");
}

// Verify update_nested_has_null recurses into STRUCT subfield columns.
// Mirrors the post-write step a STRUCT-returning Java UDF goes through:
// the Java side memcpys raw null bytes into subfield bitmaps without touching
// each NullableColumn's `_has_null` cache, and update_nested_has_null is
// expected to refresh the cache field-by-field.
TEST_F(ColumnHelperTest, update_nested_has_null_struct) {
    TypeDescriptor td(TYPE_STRUCT);
    td.children.emplace_back(TYPE_INT);
    td.children.emplace_back(TYPE_VARCHAR);
    td.field_names = {"a", "b"};
    auto col = ColumnHelper::create_column(td, true);
    auto* outer = down_cast<NullableColumn*>(col.get());
    outer->resize(3);

    auto* sc = down_cast<StructColumn*>(outer->data_column_raw_ptr());
    ASSERT_EQ(sc->fields_size(), 2);
    auto* a_col = down_cast<NullableColumn*>(sc->field_column_raw_ptr(0));
    auto* b_col = down_cast<NullableColumn*>(sc->field_column_raw_ptr(1));

    // Drop a null straight into subfield a's bitmap, bypassing the bookkeeping
    // that maintains _has_null. Pre-condition is the stale `false`.
    a_col->null_column_data()[1] = 1;
    ASSERT_FALSE(a_col->has_null());

    ASSERT_OK(ColumnHelper::update_nested_has_null(col.get()));

    EXPECT_TRUE(a_col->has_null());
    EXPECT_FALSE(b_col->has_null());
}

} // namespace starrocks
