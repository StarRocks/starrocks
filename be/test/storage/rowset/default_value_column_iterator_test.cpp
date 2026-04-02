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

#include "storage/rowset/default_value_column_iterator.h"

#include "column/column_helper.h"
#include "gtest/gtest.h"
#include "storage/column_predicate.h"
#include "storage/rowset/column_iterator.h"
#include "storage/tablet_schema.h"
#include "storage/types.h"

namespace starrocks {
class DefaultValueColumnIteratorTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// NOLINTNEXTLINE
TEST_F(DefaultValueColumnIteratorTest, delete_after_column) {
    TypeInfoPtr type_info = get_type_info(TYPE_INT);
    DefaultValueColumnIterator iter(false, "", true, type_info, 0, 10);

    ColumnIteratorOptions opts;
    Status st = iter.init(opts);
    ASSERT_TRUE(st.ok());

    std::vector<const ColumnPredicate*> preds;
    std::unique_ptr<ColumnPredicate> del_pred(new_column_null_predicate(type_info, 1, true));
    SparseRange<> row_ranges;
    st = iter.get_row_ranges_by_zone_map(preds, del_pred.get(), &row_ranges, CompoundNodeType::AND);
    ASSERT_TRUE(st.ok());

    TypeDescriptor type_desc(LogicalType::TYPE_INT);
    MutableColumnPtr column = ColumnHelper::create_column(type_desc, true);

    size_t num_rows = 10;
    st = iter.next_batch(&num_rows, column.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(column->delete_state(), DEL_PARTIAL_SATISFIED);
    ASSERT_EQ(num_rows, 10);
    ASSERT_EQ(column->size(), 10);
    for (size_t i = 0; i < 10; i++) {
        ASSERT_TRUE(column->is_null(i));
    }
}

// Test that DefaultValueColumnIterator properly destroys placement-new'd Datum
// for complex types (ARRAY/MAP/STRUCT), preventing memory leaks.
// This test is designed to be run under ASAN to detect leaks.
TEST_F(DefaultValueColumnIteratorTest, no_leak_for_array_default_value) {
    // Build an ARRAY<INT> TypeInfo via TabletColumn
    TabletColumn array_col;
    array_col.set_unique_id(0);
    array_col.set_name("c_array");
    array_col.set_type(TYPE_ARRAY);
    array_col.set_is_nullable(true);
    array_col.set_length(24);

    TabletColumn int_col;
    int_col.set_unique_id(1);
    int_col.set_name("element");
    int_col.set_type(TYPE_INT);
    int_col.set_is_nullable(false);
    int_col.set_length(4);
    array_col.add_sub_column(int_col);

    TypeInfoPtr type_info = get_type_info(array_col);
    ASSERT_NE(type_info, nullptr);

    // JSON representation of [1, 2, 3]
    std::string default_value = "[1, 2, 3]";

    {
        // Scope the iterator so its destructor runs before test ends.
        // Under ASAN, a missing Datum destructor call would be reported as a leak.
        DefaultValueColumnIterator iter(true, default_value, true, type_info, 24, 10);

        ColumnIteratorOptions opts;
        ASSERT_TRUE(iter.init(opts).ok());

        // Read a batch to ensure the default value is actually used
        TypeDescriptor type_desc(LogicalType::TYPE_ARRAY);
        type_desc.children.emplace_back(LogicalType::TYPE_INT);
        MutableColumnPtr column = ColumnHelper::create_column(type_desc, true);

        size_t num_rows = 5;
        ASSERT_TRUE(iter.next_batch(&num_rows, column.get()).ok());
        ASSERT_EQ(column->size(), 5);
    }
    // If the destructor doesn't call Datum::~Datum(), ASAN will report a leak here.
}

TEST_F(DefaultValueColumnIteratorTest, no_leak_for_map_default_value) {
    TabletColumn map_col;
    map_col.set_unique_id(0);
    map_col.set_name("c_map");
    map_col.set_type(TYPE_MAP);
    map_col.set_is_nullable(true);
    map_col.set_length(24);

    TabletColumn key_col;
    key_col.set_unique_id(1);
    key_col.set_name("key");
    key_col.set_type(TYPE_VARCHAR);
    key_col.set_is_nullable(false);
    key_col.set_length(128);
    map_col.add_sub_column(key_col);

    TabletColumn val_col;
    val_col.set_unique_id(2);
    val_col.set_name("value");
    val_col.set_type(TYPE_INT);
    val_col.set_is_nullable(true);
    val_col.set_length(4);
    map_col.add_sub_column(val_col);

    TypeInfoPtr type_info = get_type_info(map_col);
    ASSERT_NE(type_info, nullptr);

    std::string default_value = R"({"a": 1, "b": 2})";

    {
        DefaultValueColumnIterator iter(true, default_value, true, type_info, 24, 10);
        ColumnIteratorOptions opts;
        ASSERT_TRUE(iter.init(opts).ok());

        TypeDescriptor type_desc(LogicalType::TYPE_MAP);
        type_desc.children.emplace_back(LogicalType::TYPE_VARCHAR);
        type_desc.children.back().len = 128;
        type_desc.children.emplace_back(LogicalType::TYPE_INT);
        MutableColumnPtr column = ColumnHelper::create_column(type_desc, true);

        size_t num_rows = 3;
        ASSERT_TRUE(iter.next_batch(&num_rows, column.get()).ok());
        ASSERT_EQ(column->size(), 3);
    }
}

TEST_F(DefaultValueColumnIteratorTest, no_leak_for_struct_default_value) {
    TabletColumn struct_col;
    struct_col.set_unique_id(0);
    struct_col.set_name("c_struct");
    struct_col.set_type(TYPE_STRUCT);
    struct_col.set_is_nullable(true);
    struct_col.set_length(24);

    TabletColumn field1;
    field1.set_unique_id(1);
    field1.set_name("f1");
    field1.set_type(TYPE_INT);
    field1.set_is_nullable(true);
    field1.set_length(4);
    struct_col.add_sub_column(field1);

    TabletColumn field2;
    field2.set_unique_id(2);
    field2.set_name("f2");
    field2.set_type(TYPE_VARCHAR);
    field2.set_is_nullable(true);
    field2.set_length(128);
    struct_col.add_sub_column(field2);

    TypeInfoPtr type_info = get_type_info(struct_col);
    ASSERT_NE(type_info, nullptr);

    std::string default_value = R"([42, "hello"])";

    {
        DefaultValueColumnIterator iter(true, default_value, true, type_info, 24, 10);
        ColumnIteratorOptions opts;
        ASSERT_TRUE(iter.init(opts).ok());

        TypeDescriptor type_desc(LogicalType::TYPE_STRUCT);
        type_desc.children.emplace_back(LogicalType::TYPE_INT);
        type_desc.children.emplace_back(LogicalType::TYPE_VARCHAR);
        type_desc.children.back().len = 128;
        type_desc.field_names.emplace_back("f1");
        type_desc.field_names.emplace_back("f2");
        MutableColumnPtr column = ColumnHelper::create_column(type_desc, true);

        size_t num_rows = 3;
        ASSERT_TRUE(iter.next_batch(&num_rows, column.get()).ok());
        ASSERT_EQ(column->size(), 3);
    }
}

} // namespace starrocks