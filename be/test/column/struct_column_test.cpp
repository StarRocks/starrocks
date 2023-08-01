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

#include "column/struct_column.h"

#include <gtest/gtest.h>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"

namespace starrocks {

std::shared_ptr<StructColumn> create_test_column() {
    std::vector<std::string> field_name{"id", "name"};
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    Columns fields{id, name};
    auto column = StructColumn::create(fields, field_name);

    DatumStruct struct1{uint64_t(1), Slice("smith")};
    DatumStruct struct2{uint64_t(2), Slice("cruise")};
    column->append_datum(struct1);
    column->append_datum(struct2);

    return column;
}

TEST(StructColumnTest, test_create) {
    auto col = create_test_column();

    ASSERT_EQ(col->size(), 2);
    ASSERT_EQ("{id:1,name:'smith'}", col->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", col->debug_item(1));

    ASSERT_EQ("{id:1,name:'smith'}, {id:2,name:'cruise'}", col->debug_string());
}

TEST(StructColumnTest, test_update_if_overflow) {
    auto col = create_test_column();

    // it does not upgrade because of not overflow
    auto ret = col->upgrade_if_overflow();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);
}

TEST(StructColumnTest, test_column_downgrade) {
    {
        auto col = create_test_column();

        ASSERT_FALSE(col->has_large_column());
        auto ret = col->downgrade();
        ASSERT_TRUE(ret.ok());
        ASSERT_TRUE(ret.value() == nullptr);
    }

    {
        std::vector<std::string> field_name{"id", "name"};
        auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
        auto name = NullableColumn::create(LargeBinaryColumn::create(), NullColumn::create());
        Columns fields{id, name};
        auto column = StructColumn::create(fields, field_name);

        for (size_t i = 0; i < 10; i++) {
            column->append_datum(DatumStruct{i, Slice(std::to_string(i))});
        }

        ASSERT_TRUE(column->has_large_column());
        auto ret = column->downgrade();
        ASSERT_TRUE(ret.ok());
        ASSERT_TRUE(ret.value() == nullptr);
        ASSERT_FALSE(column->has_large_column());
        ASSERT_EQ(column->size(), 10);
        for (size_t i = 0; i < 10; i++) {
            DatumStruct datum = column->get(i).get_struct();
            ASSERT_EQ(i, datum[0].get_uint64());
            ASSERT_EQ(std::to_string(i), datum[1].get_slice());
        }
    }
}

TEST(StructColumnTest, test_append_null) {
    {
        std::vector<std::string> field_name{"id", "name"};
        auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
        auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        Columns fields{id, name};
        auto column = StructColumn::create(fields, field_name);

        DatumStruct struct1{uint64_t(1), Slice("smith")};
        DatumStruct struct3{uint64_t(3), Slice("cruise")};
        column->append_datum(struct1);
        ASSERT_TRUE(column->append_nulls(1));
        column->append_datum(struct3);

        ASSERT_EQ(column->size(), 3);
        ASSERT_EQ("{id:1,name:'smith'}", column->debug_item(0));
        ASSERT_EQ("{id:NULL,name:NULL}", column->debug_item(1));
        ASSERT_EQ("{id:3,name:'cruise'}", column->debug_item(2));
    }

    {
        std::vector<std::string> field_name{"id", "name"};
        auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
        // one subfield is not nullable
        auto name = BinaryColumn::create();
        Columns fields{id, name};
        auto column = StructColumn::create(fields, field_name);

        DatumStruct struct1{uint64_t(1), Slice("smith")};
        DatumStruct struct3{uint64_t(3), Slice("cruise")};
        column->append_datum(struct1);
        ASSERT_FALSE(column->append_nulls(1));
        column->append_datum(struct3);

        ASSERT_EQ(column->size(), 2);
        ASSERT_EQ("{id:1,name:'smith'}", column->debug_item(0));
        ASSERT_EQ("{id:3,name:'cruise'}", column->debug_item(1));
    }
}

TEST(StructColumnTest, test_append_defaults) {
    std::vector<std::string> field_name{"id", "name"};
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    Columns fields{id, name};
    auto column = StructColumn::create(fields, field_name);

    column->append_default();
    ASSERT_EQ(1, column->size());
    ASSERT_EQ("{id:NULL,name:NULL}", column->debug_item(0));

    column->append_default(2);
    ASSERT_EQ(3, column->size());
    ASSERT_EQ("{id:NULL,name:NULL}", column->debug_item(1));
    ASSERT_EQ("{id:NULL,name:NULL}", column->debug_item(2));
}

TEST(StructColumnTest, equals) {
    // lhs: {1, 2}, {1, null}, {4, 5}, {2, 1}
    // rhs: {1, 2}, {1, 2}, {6, 7}, {2, 1}
    StructColumn::Ptr lhs;
    {
        auto field1 = NullableColumn::create(Int32Column::create(), NullColumn::create());
        auto field2 = NullableColumn::create(Int32Column::create(), NullColumn::create());
        Columns fields{field1, field2};
        lhs = StructColumn::create(fields);
    }
    lhs->_fields[0]->append_datum(Datum(1));
    lhs->_fields[0]->append_datum(Datum(1));
    lhs->_fields[0]->append_datum(Datum(4));
    lhs->_fields[0]->append_datum(Datum(2));

    lhs->_fields[1]->append_datum(Datum(2));
    lhs->_fields[1]->append_nulls(1);
    lhs->_fields[1]->append_datum(Datum(5));
    lhs->_fields[1]->append_datum(Datum(1));

    StructColumn::Ptr rhs;
    {
        auto field1 = Int32Column::create();
        auto field2 = Int32Column::create();
        Columns fields{field1, field2};
        rhs = StructColumn::create(fields);
    }
    rhs->_fields[0]->append_datum(Datum(1));
    rhs->_fields[0]->append_datum(Datum(1));
    rhs->_fields[0]->append_datum(Datum(6));
    rhs->_fields[0]->append_datum(Datum(2));

    rhs->_fields[1]->append_datum(Datum(2));
    rhs->_fields[1]->append_datum(Datum(2));
    rhs->_fields[1]->append_datum(Datum(7));
    rhs->_fields[1]->append_datum(Datum(1));

    ASSERT_TRUE(lhs->equals(0, *rhs, 0));
    ASSERT_TRUE(lhs->equals(0, *rhs, 1));
    ASSERT_FALSE(lhs->equals(1, *rhs, 1));
    ASSERT_FALSE(lhs->equals(2, *rhs, 2));
    ASSERT_TRUE(lhs->equals(3, *rhs, 3));
}

TEST(StructColumnTest, test_byte_size) {
    auto col = create_test_column();

    ASSERT_EQ(sizeof(uint64_t) * 2 + sizeof(BinaryColumn::Offset) * 2 + 11 + 2 * 2, col->byte_size());
    ASSERT_EQ(sizeof(uint64_t) + sizeof(BinaryColumn::Offset) + 6 + 2, col->byte_size(1, 1));
    ASSERT_EQ(sizeof(uint64_t) + sizeof(BinaryColumn::Offset) + 5 + 2, col->byte_size(0));
}

TEST(StructColumnTest, test_resize) {
    auto col = create_test_column();

    ASSERT_EQ(2, col->size());

    col->resize(1);

    ASSERT_EQ(1, col->size());
    ASSERT_EQ("{id:1,name:'smith'}", col->debug_item(0));
}

TEST(StructColumnTest, test_reset_column) {
    auto col = create_test_column();

    col->reset_column();

    ASSERT_EQ(0, col->size());
    for (const auto& subfield : col->fields()) {
        ASSERT_EQ(0, subfield->size());
    }
}

TEST(StructColumnTest, test_swap_column) {
    ColumnPtr column1;
    ColumnPtr column2;
    {
        std::vector<std::string> field_name{"id", "name"};
        auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
        auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        Columns fields{id, name};
        column1 = StructColumn::create(fields, field_name);

        DatumStruct struct1{uint64_t(1), Slice("smith")};
        column1->append_datum(struct1);
    }
    {
        std::vector<std::string> field_name{"id", "name"};
        auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
        auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        Columns fields{id, name};
        column2 = StructColumn::create(fields, field_name);

        DatumStruct struct1{uint64_t(2), Slice("smith cruise")};
        DatumStruct struct2{uint64_t(3), Slice("cruise smith")};
        column2->append_datum(struct1);
        column2->append_datum(struct2);
    }
    ASSERT_EQ(1, column1->size());
    ASSERT_EQ(2, column2->size());
    ASSERT_EQ("{id:1,name:'smith'}", column1->debug_item(0));
    ASSERT_EQ("{id:2,name:'smith cruise'}", column2->debug_item(0));
    ASSERT_EQ("{id:3,name:'cruise smith'}", column2->debug_item(1));

    column1->swap_column(*column2);
    ASSERT_EQ(2, column1->size());
    ASSERT_EQ(1, column2->size());
    ASSERT_EQ("{id:2,name:'smith cruise'}", column1->debug_item(0));
    ASSERT_EQ("{id:3,name:'cruise smith'}", column1->debug_item(1));
    ASSERT_EQ("{id:1,name:'smith'}", column2->debug_item(0));
}

TEST(StructColumnTest, test_copy_construtor) {
    auto col = create_test_column();

    ASSERT_EQ(col->size(), 2);
    ASSERT_EQ("{id:1,name:'smith'}", col->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", col->debug_item(1));

    StructColumn copy(*col);
    col->reset_column();
    ASSERT_EQ(0, col->size());
    ASSERT_EQ(2, copy.size());
    ASSERT_EQ("{id:1,name:'smith'}", copy.debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", copy.debug_item(1));

    ASSERT_TRUE(copy.fields().at(0).unique());
    ASSERT_TRUE(copy.fields().at(1).unique());
}

TEST(StructColumnTest, test_move_construtor) {
    auto col = create_test_column();

    ASSERT_EQ(col->size(), 2);
    ASSERT_EQ("{id:1,name:'smith'}", col->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", col->debug_item(1));

    StructColumn copy(std::move(*col));
    ASSERT_EQ(2, copy.size());
    ASSERT_EQ("{id:1,name:'smith'}", copy.debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", copy.debug_item(1));

    ASSERT_TRUE(copy.fields().at(0).unique());
    ASSERT_TRUE(copy.fields().at(1).unique());
}

TEST(StructColumnTest, test_clone) {
    auto col = create_test_column();

    ASSERT_EQ(col->size(), 2);
    ASSERT_EQ("{id:1,name:'smith'}", col->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", col->debug_item(1));

    auto copy = col->clone();
    col->reset_column();
    ASSERT_EQ(2, copy->size());
    ASSERT_EQ("{id:1,name:'smith'}", copy->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", copy->debug_item(1));

    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->fields().at(0).unique());
    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->fields().at(1).unique());
}

TEST(StructColumnTest, test_clone_shared) {
    auto col = create_test_column();

    ASSERT_EQ(col->size(), 2);
    ASSERT_EQ("{id:1,name:'smith'}", col->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", col->debug_item(1));

    auto copy = col->clone_shared();
    col->reset_column();
    ASSERT_EQ(2, copy->size());
    ASSERT_EQ("{id:1,name:'smith'}", copy->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", copy->debug_item(1));

    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->fields().at(0).unique());
    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->fields().at(1).unique());
}

TEST(StructColumnTest, test_clone_empty) {
    auto col = create_test_column();

    ASSERT_EQ(2, col->size());
    ASSERT_EQ("{id:1,name:'smith'}", col->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", col->debug_item(1));

    auto copy = col->clone_empty();
    col->reset_column();
    ASSERT_EQ(0, copy->size());

    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->fields().at(0).unique());
    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->fields().at(1).unique());
}

TEST(StructColumnTest, test_update_rows) {
    std::vector<std::string> field_name{"id", "name"};
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto column = StructColumn::create(Columns{id, name}, field_name);

    ASSERT_TRUE(column->is_struct());
    ASSERT_FALSE(column->is_nullable());
    ASSERT_EQ(0, column->size());

    DatumStruct struct1{uint64_t(1), Slice("smith")};
    DatumStruct struct2{uint64_t(2), Slice("cruise")};
    DatumStruct struct3{uint64_t(3), Slice("hello")};
    column->append_datum(struct1);
    column->append_datum(struct2);
    column->append_datum(struct3);

    auto copy = column->clone_empty();
    copy->append_datum(struct2);
    copy->append_datum(DatumStruct{uint64_t(4), Slice("world")});
    std::vector<uint32_t> replace_indexes = {0, 2};
    ASSERT_TRUE(column->update_rows(*copy.get(), replace_indexes.data()).ok());

    ASSERT_EQ(3, column->size());
    ASSERT_EQ("{id:2,name:'cruise'}", column->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", column->debug_item(1));
    ASSERT_EQ("{id:4,name:'world'}", column->debug_item(2));
}

TEST(StructColumnTest, test_assign) {
    std::vector<std::string> field_name{"id", "name"};
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto column = StructColumn::create(Columns{id, name}, field_name);

    ASSERT_TRUE(column->is_struct());
    ASSERT_FALSE(column->is_nullable());
    ASSERT_EQ(0, column->size());

    DatumStruct struct1{uint64_t(1), Slice("smith")};
    column->append_datum(struct1);

    ASSERT_EQ(1, column->size());
    column->assign(2, 0);

    ASSERT_EQ(2, column->size());
    ASSERT_EQ("{id:1,name:'smith'}", column->debug_item(0));
    ASSERT_EQ("{id:1,name:'smith'}", column->debug_item(1));
}

TEST(StructColumnTest, test_unnamed) {
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    Columns fields{id, name};
    auto column = StructColumn::create(fields);

    ASSERT_TRUE(column->is_struct());
    ASSERT_FALSE(column->is_nullable());
    ASSERT_EQ(0, column->size());

    DatumStruct struct1{uint64_t(1), Slice("smith")};
    DatumStruct struct2{uint64_t(2), Slice("cruise")};
    column->append_datum(struct1);
    column->append_datum(struct2);

    ASSERT_EQ(column->size(), 2);
    ASSERT_EQ("{1,'smith'}", column->debug_item(0));
    ASSERT_EQ("{2,'cruise'}", column->debug_item(1));

    ASSERT_EQ("{1,'smith'}, {2,'cruise'}", column->debug_string());
}

TEST(StructColumnTest, test_element_memory_usage) {
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    Columns fields{id, name};
    auto column = StructColumn::create(fields);

    column->append_datum(DatumStruct{uint64_t(1), Slice("2")});
    column->append_datum(DatumStruct{uint64_t(1), Slice("4")});
    column->append_datum(DatumStruct{uint64_t(1), Slice("6")});

    ASSERT_EQ(45, column->Column::element_memory_usage());

    std::vector<size_t> element_mem_usages = {15, 15, 15};
    size_t element_num = element_mem_usages.size();
    for (size_t start = 0; start < element_num; start++) {
        size_t expected_usage = 0;
        ASSERT_EQ(0, column->element_memory_usage(start, 0));
        for (size_t size = 1; start + size <= element_num; size++) {
            expected_usage += element_mem_usages[start + size - 1];
            ASSERT_EQ(expected_usage, column->element_memory_usage(start, size));
        }
    }
}

} // namespace starrocks