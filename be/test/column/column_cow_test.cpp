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

#include <cstdint>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "runtime/types.h"
#include "testutil/parallel_test.h"

namespace starrocks {

class ColumnCOWTest : public testing::Test {
public:
    template <typename Type>
    static void test_column_cow(ColumnPtr&& col) {
        // COW: mut1 is as shadow copy of col, because col's use count is 1
        ColumnPtr mut1 = Type::static_pointer_cast(Column::mutate(std::move(col)));
        EXPECT_EQ(nullptr, col);
        EXPECT_EQ(1, mut1->use_count());

        // mut2 is shadow copy of mut1 which is not mutable
        auto mut2 = mut1;
        EXPECT_EQ(mut1.get(), mut2.get());
        EXPECT_EQ(2, mut1->use_count());
        EXPECT_EQ(2, mut2->use_count());

        // COW: mut3 is a deep copy of mut1
        auto mut3 = Type::static_pointer_cast(Column::mutate(std::move(mut1)));
        EXPECT_EQ(nullptr, mut1);
        EXPECT_NE(mut2.get(), mut3.get());
        EXPECT_EQ(1, mut3->use_count());
    }
};

TEST_F(ColumnCOWTest, test_int_column) {
    UInt32Column::Ptr col = UInt32Column::create();
    col->append(1);
    EXPECT_EQ(1, col->size());
    EXPECT_EQ(1, col->use_count());

    {
        UInt32Column::Ptr col1 = UInt32Column::static_pointer_cast(col);
        // col1 is deep copy of col
        EXPECT_EQ(1, col1->size());
        EXPECT_EQ(2, col1->use_count());
        EXPECT_EQ(1, col1->get(0).get_int32());
        EXPECT_EQ(col.get(), col1.get());
    }

    // mut is as shadow copy of col, because col's use count is 1
    UInt32Column::MutablePtr mut1 = UInt32Column::static_pointer_cast(Column::mutate(std::move(col)));
    EXPECT_EQ(nullptr, col);
    EXPECT_EQ(1, mut1->size());
    EXPECT_EQ(1, mut1->use_count());
    mut1->append(2);
    EXPECT_EQ(2, mut1->size());
    EXPECT_EQ(1, mut1->get(0).get_int32());
    EXPECT_EQ(2, mut1->get(1).get_int32());

    UInt32Column::MutablePtr mut2 = UInt32Column::static_pointer_cast(Column::mutate(std::move(mut1)));
    EXPECT_EQ(nullptr, mut1);
    EXPECT_EQ(2, mut2->size());
    EXPECT_EQ(1, mut2->use_count());
    mut2->append(3);
    EXPECT_EQ(3, mut2->size());
    EXPECT_EQ(1, mut2->get(0).get_int32());
    EXPECT_EQ(2, mut2->get(1).get_int32());
    EXPECT_EQ(3, mut2->get(2).get_int32());
}

TEST_F(ColumnCOWTest, test_nullable_column) {
    auto col = NullableColumn::create(Int32Column::create(), NullColumn::create());
    col->append_datum(1);
    col->append_datum(2);

    // COW: mut1 is as shadow copy of col, because col's use count is 1
    NullableColumn::Ptr mut1 = NullableColumn::static_pointer_cast(Column::mutate(std::move(col)));
    EXPECT_EQ(nullptr, col);
    EXPECT_EQ(2, mut1->size());
    EXPECT_EQ(1, mut1->use_count());
    auto& mut1_data = mut1->data_column();
    auto& mut1_null = mut1->null_column();
    EXPECT_EQ(1, mut1_data->use_count());
    EXPECT_EQ(1, mut1_null->use_count());

    // mut2 is shadow copy of mut1 which is not mutable
    NullableColumn::Ptr mut2 = mut1;
    EXPECT_EQ(mut1.get(), mut2.get());
    EXPECT_EQ(2, mut2->size());
    EXPECT_EQ(2, mut1->use_count());
    EXPECT_EQ(2, mut2->use_count());

    // COW: mut3 is a deep copy of mut1
    NullableColumn::Ptr mut3 = NullableColumn::static_pointer_cast(Column::mutate(std::move(mut1)));
    EXPECT_EQ(nullptr, mut1);
    EXPECT_NE(mut2.get(), mut3.get());
    EXPECT_EQ(2, mut3->size());
    EXPECT_EQ(1, mut3->use_count());
    auto& mut3_data = mut3->data_column();
    auto& mut3_null = mut3->null_column();
    EXPECT_EQ(1, mut3_data->use_count());
    EXPECT_EQ(1, mut3_null->use_count());
}

TEST_F(ColumnCOWTest, test_array_column) {
    auto offsets = UInt32Column::create();
    offsets->append(0);

    auto elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
    elements->append_datum(1);
    elements->append_datum(2);
    offsets->append(2);

    elements->append_datum(3);
    elements->append_datum(4);
    elements->append_datum(5);
    offsets->append(5);
    auto col = ArrayColumn::create(std::move(elements), std::move(offsets));

    ASSERT_TRUE(col->is_array());
    ASSERT_FALSE(col->is_nullable());
    ASSERT_EQ(2, col->size());

    // mut is as shadow copy of col, because col's use count is 1
    ArrayColumn::Ptr mut1 = ArrayColumn::static_pointer_cast(Column::mutate(std::move(col)));
    EXPECT_EQ(nullptr, col);
    EXPECT_EQ(2, mut1->size());
    EXPECT_EQ(1, mut1->use_count());
    auto& mut_elements = mut1->elements_column();
    auto& mut_offsets = mut1->offsets_column();
    EXPECT_EQ(1, mut_elements->use_count());
    EXPECT_EQ(1, mut_offsets->use_count());

    // ref count +1
    ArrayColumn::Ptr mut2 = mut1;
    EXPECT_EQ(mut1.get(), mut2.get());
    EXPECT_EQ(2, mut2->size());
    EXPECT_EQ(2, mut1->use_count());
    EXPECT_EQ(2, mut2->use_count());

    // mut3 is a deep copy of mut1
    ArrayColumn::Ptr mut3 = ArrayColumn::static_pointer_cast(Column::mutate(std::move(mut1)));
    EXPECT_EQ(nullptr, mut1);
    EXPECT_NE(mut2.get(), mut3.get());
    EXPECT_EQ(2, mut3->size());
    EXPECT_EQ(1, mut3->use_count());
    auto& mut3_elements = mut3->elements_column();
    auto& mut3_offsets = mut3->offsets_column();
    EXPECT_EQ(1, mut3_elements->use_count());
    EXPECT_EQ(1, mut3_offsets->use_count());
}

TEST_F(ColumnCOWTest, test_binary_column) {
    BinaryColumn::Ptr col = BinaryColumn::create();
    for (size_t i = 0; i < 10; i++) {
        col->append(std::to_string(i));
    }
    test_column_cow<BinaryColumn>(std::move(col));
}

TEST_F(ColumnCOWTest, test_json_column) {
    JsonColumn::Ptr col = JsonColumn::create();
    col->append(JsonValue::parse("1").value());
    test_column_cow<JsonColumn>(std::move(col));
}

TEST_F(ColumnCOWTest, test_struct_column) {
    std::vector<std::string> field_name{"id", "name"};
    auto id = NullableColumn::create(UInt64Column::create(), NullColumn::create());
    auto name = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    Columns fields{std::move(id), std::move(name)};
    auto col = StructColumn::create(std::move(fields), std::move(field_name));
    DatumStruct struct1{uint64_t(1), Slice("smith")};
    DatumStruct struct2{uint64_t(2), Slice("cruise")};
    col->append_datum(struct1);
    col->append_datum(struct2);

    test_column_cow<StructColumn>(std::move(col));
}

TEST_F(ColumnCOWTest, test_map_column) {
    MapColumn::Ptr column = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                              NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                              UInt32Column::create());
    for (int32_t i = 0; i < 10; i++) {
        column->append_datum(DatumMap{{i, i + 1}});
    }
    test_column_cow<MapColumn>(std::move(column));
}

TEST_F(ColumnCOWTest, test_object_column) {
    BitmapColumn::Ptr src_col = BitmapColumn::create();
    BitmapValue bitmap;
    for (size_t i = 0; i < 64; i++) {
        bitmap.add(i);
    }
    src_col->append(&bitmap);
    test_column_cow<BitmapColumn>(std::move(src_col));
}

TEST_F(ColumnCOWTest, test_const_column) {
    Int32Column::Ptr data_column = Int32Column::create();
    data_column->append(1);

    ConstColumn::Ptr column = ConstColumn::create(std::move(data_column), 1024);
    test_column_cow<ConstColumn>(std::move(column));
}

} // namespace starrocks
