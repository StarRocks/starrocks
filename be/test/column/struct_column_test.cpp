// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/struct_column.h"

#include <gtest/gtest.h>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"

namespace starrocks::vectorized {

TEST(StructColumnTest, test_create) {
    auto field_name = BinaryColumn::create();
    field_name->append_string("id");
    field_name->append_string("name");
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    Columns fields{id, name};
    auto column = StructColumn::create(fields, field_name);

    ASSERT_TRUE(column->is_struct());
    ASSERT_FALSE(column->is_nullable());
    ASSERT_EQ(0, column->size());

    DatumStruct struct1{uint64_t(1), Slice("smith")};
    DatumStruct struct2{uint64_t(2), Slice("cruise")};
    column->append_datum(struct1);
    column->append_datum(struct2);

    ASSERT_EQ(column->size(), 2);
    ASSERT_EQ("{id:1,name:'smith'}", column->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", column->debug_item(1));

    ASSERT_EQ("{id:1,name:'smith'}, {id:2,name:'cruise'}", column->debug_string());
}

TEST(StructColumnTest, test_update_if_overflow) {
    {
        auto field_name = BinaryColumn::create();
        field_name->append_string("id");
        field_name->append_string("name");
        auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
        auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        Columns fields{id, name};
        auto column = StructColumn::create(fields, field_name);

        DatumStruct struct1{uint64_t(1), Slice("smith")};
        DatumStruct struct2{uint64_t(2), Slice("cruise")};
        column->append_datum(struct1);
        column->append_datum(struct2);

        // it does not upgrade because of not overflow
        auto ret = column->upgrade_if_overflow();
        ASSERT_TRUE(ret.ok());
        ASSERT_TRUE(ret.value() == nullptr);
    }

    {
        /*
         * require too much of time, comment it.
        auto field_name = BinaryColumn::create();
        field_name->append_string("id");
        field_name->append_string("name");
        auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
        auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        Columns fields{id, name};
        auto column = StructColumn::create(fields, field_name);

        size_t item_count = 1 << 30;
        for (size_t i = 0; i < item_count; i++) {
            column->append_datum(DatumStruct{i, Slice("smith")});
        }

        auto ret = column->upgrade_if_overflow();
        ASSERT_TRUE(ret.ok());
        ASSERT_TRUE(ret.value() == nullptr);
        ASSERT_TRUE(column->has_large_column());
         */
    }
}

TEST(StructColumnTest, test_column_downgrade) {
    {
        auto field_name = BinaryColumn::create();
        field_name->append_string("id");
        field_name->append_string("name");
        auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
        auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        Columns fields{id, name};
        auto column = StructColumn::create(fields, field_name);

        DatumStruct struct1{uint64_t(1), Slice("smith")};
        DatumStruct struct2{uint64_t(2), Slice("cruise")};
        column->append_datum(struct1);
        column->append_datum(struct2);

        ASSERT_FALSE(column->has_large_column());
        auto ret = column->downgrade();
        ASSERT_TRUE(ret.ok());
        ASSERT_TRUE(ret.value() == nullptr);
    }

    {
        auto field_name = BinaryColumn::create();
        field_name->append_string("id");
        field_name->append_string("name");
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
        auto field_name = BinaryColumn::create();
        field_name->append_string("id");
        field_name->append_string("name");
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
        auto field_name = BinaryColumn::create();
        field_name->append_string("id");
        field_name->append_string("name");
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
    auto field_name = BinaryColumn::create();
    field_name->append_string("id");
    field_name->append_string("name");
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

TEST(StructColumnTest, test_resize) {
    auto field_name = BinaryColumn::create();
    field_name->append_string("id");
    field_name->append_string("name");
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    Columns fields{id, name};
    auto column = StructColumn::create(fields, field_name);

    DatumStruct struct1{uint64_t(1), Slice("smith")};
    DatumStruct struct2{uint64_t(2), Slice("cruise")};
    column->append_datum(struct1);
    column->append_datum(struct2);

    ASSERT_EQ(2, column->size());

    column->resize(1);

    ASSERT_EQ(1, column->size());
    ASSERT_EQ("{id:1,name:'smith'}", column->debug_item(0));
}

TEST(StructColumnTest, test_reset_column) {
    auto field_name = BinaryColumn::create();
    field_name->append_string("id");
    field_name->append_string("name");
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    Columns fields{id, name};
    auto column = StructColumn::create(fields, field_name);

    DatumStruct struct1{uint64_t(1), Slice("smith")};
    DatumStruct struct2{uint64_t(2), Slice("cruise")};
    column->append_datum(struct1);
    column->append_datum(struct2);

    ASSERT_EQ(2, column->size());

    column->reset_column();

    ASSERT_EQ(0, column->size());
    for (const auto& subfield : column->fields()) {
        ASSERT_EQ(0, subfield->size());
    }
}

TEST(StructColumnTest, test_swap_column) {
    ColumnPtr column1;
    ColumnPtr column2;
    {
        auto field_name = BinaryColumn::create();
        field_name->append_string("id");
        field_name->append_string("name");
        auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
        auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        Columns fields{id, name};
        column1 = StructColumn::create(fields, field_name);

        DatumStruct struct1{uint64_t(1), Slice("smith")};
        column1->append_datum(struct1);
    }
    {
        auto field_name = BinaryColumn::create();
        field_name->append_string("id");
        field_name->append_string("name");
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
    auto field_name = BinaryColumn::create();
    field_name->append_string("id");
    field_name->append_string("name");
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto column = StructColumn::create(Columns{id, name}, field_name);

    // delete reference
    field_name = nullptr;
    id = nullptr;
    name = nullptr;

    ASSERT_TRUE(column->is_struct());
    ASSERT_FALSE(column->is_nullable());
    ASSERT_EQ(0, column->size());

    DatumStruct struct1{uint64_t(1), Slice("smith")};
    DatumStruct struct2{uint64_t(2), Slice("cruise")};
    column->append_datum(struct1);
    column->append_datum(struct2);

    ASSERT_EQ(column->size(), 2);
    ASSERT_EQ("{id:1,name:'smith'}", column->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", column->debug_item(1));

    StructColumn copy(*column);
    column->reset_column();
    ASSERT_EQ(0, column->size());
    ASSERT_EQ(2, copy.size());
    ASSERT_EQ("{id:1,name:'smith'}", copy.debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", copy.debug_item(1));

    ASSERT_TRUE(copy.field_names_column().unique());
    ASSERT_TRUE(copy.fields().at(0).unique());
    ASSERT_TRUE(copy.fields().at(1).unique());
}

TEST(StructColumnTest, test_move_construtor) {
    auto field_name = BinaryColumn::create();
    field_name->append_string("id");
    field_name->append_string("name");
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto column = StructColumn::create(Columns{id, name}, field_name);

    // delete reference
    field_name = nullptr;
    id = nullptr;
    name = nullptr;

    ASSERT_TRUE(column->is_struct());
    ASSERT_FALSE(column->is_nullable());
    ASSERT_EQ(0, column->size());

    DatumStruct struct1{uint64_t(1), Slice("smith")};
    DatumStruct struct2{uint64_t(2), Slice("cruise")};
    column->append_datum(struct1);
    column->append_datum(struct2);

    ASSERT_EQ(column->size(), 2);
    ASSERT_EQ("{id:1,name:'smith'}", column->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", column->debug_item(1));

    StructColumn copy(std::move(*column));
    ASSERT_EQ(2, copy.size());
    ASSERT_EQ("{id:1,name:'smith'}", copy.debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", copy.debug_item(1));

    ASSERT_TRUE(copy.field_names_column().unique());
    ASSERT_TRUE(copy.fields().at(0).unique());
    ASSERT_TRUE(copy.fields().at(1).unique());
}

TEST(StructColumnTest, test_clone) {
    auto field_name = BinaryColumn::create();
    field_name->append_string("id");
    field_name->append_string("name");
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto column = StructColumn::create(Columns{id, name}, field_name);

    // delete reference
    field_name = nullptr;
    id = nullptr;
    name = nullptr;

    ASSERT_TRUE(column->is_struct());
    ASSERT_FALSE(column->is_nullable());
    ASSERT_EQ(0, column->size());

    DatumStruct struct1{uint64_t(1), Slice("smith")};
    DatumStruct struct2{uint64_t(2), Slice("cruise")};
    column->append_datum(struct1);
    column->append_datum(struct2);

    ASSERT_EQ(column->size(), 2);
    ASSERT_EQ("{id:1,name:'smith'}", column->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", column->debug_item(1));

    auto copy = column->clone();
    column->reset_column();
    ASSERT_EQ(2, copy->size());
    ASSERT_EQ("{id:1,name:'smith'}", copy->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", copy->debug_item(1));

    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->field_names_column().unique());
    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->fields().at(0).unique());
    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->fields().at(1).unique());
}

TEST(StructColumnTest, test_clone_shared) {
    auto field_name = BinaryColumn::create();
    field_name->append_string("id");
    field_name->append_string("name");
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto column = StructColumn::create(Columns{id, name}, field_name);

    // delete reference
    field_name = nullptr;
    id = nullptr;
    name = nullptr;

    ASSERT_TRUE(column->is_struct());
    ASSERT_FALSE(column->is_nullable());
    ASSERT_EQ(0, column->size());

    DatumStruct struct1{uint64_t(1), Slice("smith")};
    DatumStruct struct2{uint64_t(2), Slice("cruise")};
    column->append_datum(struct1);
    column->append_datum(struct2);

    ASSERT_EQ(column->size(), 2);
    ASSERT_EQ("{id:1,name:'smith'}", column->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", column->debug_item(1));

    auto copy = column->clone_shared();
    column->reset_column();
    ASSERT_EQ(2, copy->size());
    ASSERT_EQ("{id:1,name:'smith'}", copy->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", copy->debug_item(1));

    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->field_names_column().unique());
    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->fields().at(0).unique());
    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->fields().at(1).unique());
}

TEST(StructColumnTest, test_clone_empty) {
    auto field_name = BinaryColumn::create();
    field_name->append_string("id");
    field_name->append_string("name");
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto column = StructColumn::create(Columns{id, name}, field_name);

    // delete reference
    field_name = nullptr;
    id = nullptr;
    name = nullptr;

    ASSERT_TRUE(column->is_struct());
    ASSERT_FALSE(column->is_nullable());
    ASSERT_EQ(0, column->size());

    DatumStruct struct1{uint64_t(1), Slice("smith")};
    DatumStruct struct2{uint64_t(2), Slice("cruise")};
    column->append_datum(struct1);
    column->append_datum(struct2);

    ASSERT_EQ(2, column->size());
    ASSERT_EQ("{id:1,name:'smith'}", column->debug_item(0));
    ASSERT_EQ("{id:2,name:'cruise'}", column->debug_item(1));

    auto copy = column->clone_empty();
    column->reset_column();
    ASSERT_EQ(0, copy->size());

    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->field_names_column().unique());
    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->fields().at(0).unique());
    ASSERT_TRUE(down_cast<StructColumn*>(copy.get())->fields().at(1).unique());
}

TEST(StructColumnTest, test_update_rows) {
    auto field_name = BinaryColumn::create();
    field_name->append_string("id");
    field_name->append_string("name");
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
    auto field_name = BinaryColumn::create();
    field_name->append_string("id");
    field_name->append_string("name");
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

TEST(StructColumnTest, test_reference_memory_usage) {
    auto field_name = BinaryColumn::create();
    field_name->append_string("id");
    field_name->append_string("name");
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    Columns fields{id, name};
    auto column = StructColumn::create(fields, field_name);

    column->append_datum(DatumStruct{uint64_t(1), Slice("2")});
    column->append_datum(DatumStruct{uint64_t(1), Slice("4")});
    column->append_datum(DatumStruct{uint64_t(1), Slice("6")});

    ASSERT_EQ(0, column->Column::reference_memory_usage());
}

} // namespace starrocks::vectorized
