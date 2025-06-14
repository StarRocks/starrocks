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

#include "exprs/map_functions.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/map_column.h"
#include "exprs/mock_vectorized_expr.h"
#include "testutil/parallel_test.h"

namespace starrocks {

TypeDescriptor map_type(LogicalType key, LogicalType value) {
    TypeDescriptor type_creator;
    type_creator.type = LogicalType::TYPE_MAP;
    type_creator.children.emplace_back(TypeDescriptor(key));
    type_creator.children.emplace_back(TypeDescriptor(value));
    return type_creator;
}

PARALLEL_TEST(MapFunctionsTest, test_map) {
    TypeDescriptor input_keys_type;
    input_keys_type.type = LogicalType::TYPE_ARRAY;
    input_keys_type.children.emplace_back(LogicalType::TYPE_INT);

    TypeDescriptor input_values_type;
    input_values_type.type = LogicalType::TYPE_ARRAY;
    input_values_type.children.emplace_back(LogicalType::TYPE_VARCHAR);

    ColumnPtr input_keys = ColumnHelper::create_column(input_keys_type, true);
    // keys:   [[1,2,3], NULL, [4,5], [6,7], NULL, [9]]
    // values: [[a,b,c], [d],   NULL, [f,g], NULL, [h]]
    {
        // [1,2,3]
        {
            DatumArray datum{1, 2, 2};
            input_keys->append_datum(datum);
        }
        // NULL
        input_keys->append_nulls(1);
        // [4, 5]
        {
            DatumArray datum{4, 5};
            input_keys->append_datum(datum);
        }
        // [6, 7]
        {
            DatumArray datum{6, 7};
            input_keys->append_datum(datum);
        }
        // NULL
        input_keys->append_nulls(1);
        // [9]
        {
            DatumArray datum{9};
            input_keys->append_datum(datum);
        }
    }
    ColumnPtr input_values = ColumnHelper::create_column(input_values_type, true);
    {
        // [a,b,c]
        {
            DatumArray datum{"a", "b", "c"};
            input_values->append_datum(datum);
        }
        // [d]
        {
            DatumArray datum{"d"};
            input_values->append_datum(datum);
        }
        // NULL
        input_values->append_nulls(1);
        // [f,g]
        {
            DatumArray datum{"f", "g"};
            input_values->append_datum(datum);
        }
        // NULL
        input_values->append_nulls(1);
        // [h]
        {
            DatumArray datum{"h"};
            input_values->append_datum(datum);
        }
    }
    auto ret = MapFunctions::map_from_arrays(nullptr, {input_keys, input_values});
    EXPECT_TRUE(ret.ok());
    auto result = std::move(ret.value());
    EXPECT_EQ(6, result->size());

    EXPECT_STREQ("{1:'a',2:'c'}", result->debug_item(0).c_str());
    EXPECT_TRUE(result->is_null(1));
    EXPECT_TRUE(result->is_null(2));
    EXPECT_STREQ("{6:'f',7:'g'}", result->debug_item(3).c_str());
    EXPECT_TRUE(result->is_null(4));
    EXPECT_STREQ("{9:'h'}", result->debug_item(5).c_str());
}

PARALLEL_TEST(MapFunctionsTest, test_map_mismatch1) {
    TypeDescriptor input_keys_type;
    input_keys_type.type = LogicalType::TYPE_ARRAY;
    input_keys_type.children.emplace_back(LogicalType::TYPE_INT);

    TypeDescriptor input_values_type;
    input_values_type.type = LogicalType::TYPE_ARRAY;
    input_values_type.children.emplace_back(LogicalType::TYPE_VARCHAR);

    ColumnPtr input_keys = ColumnHelper::create_column(input_keys_type, true);
    // keys:   [[1,2,3], NULL, [4,5]]
    // values: [[a,b,c], [d],  [h]]
    {
        // [1,2,3]
        {
            DatumArray datum{1, 2, 3};
            input_keys->append_datum(datum);
        }
        // NULL
        input_keys->append_nulls(1);
        // [4, 5]
        {
            DatumArray datum{4, 5};
            input_keys->append_datum(datum);
        }
    }
    ColumnPtr input_values = ColumnHelper::create_column(input_values_type, true);
    {
        // [a,b,c]
        {
            DatumArray datum{"a", "b", "c"};
            input_values->append_datum(datum);
        }
        // [d]
        {
            DatumArray datum{"d"};
            input_values->append_datum(datum);
        }
        // [h]
        {
            DatumArray datum{"h"};
            input_values->append_datum(datum);
        }
    }
    auto ret = MapFunctions::map_from_arrays(nullptr, {input_keys, input_values});
    EXPECT_FALSE(ret.ok());
}

PARALLEL_TEST(MapFunctionsTest, test_map_mismatch2) {
    TypeDescriptor input_keys_type;
    input_keys_type.type = LogicalType::TYPE_ARRAY;
    input_keys_type.children.emplace_back(LogicalType::TYPE_INT);

    TypeDescriptor input_values_type;
    input_values_type.type = LogicalType::TYPE_ARRAY;
    input_values_type.children.emplace_back(LogicalType::TYPE_VARCHAR);

    ColumnPtr input_keys = ColumnHelper::create_column(input_keys_type, true);
    // keys:   [[1,2,3]]
    // values: [[a]]
    {
        // [1,2,3]
        {
            DatumArray datum{1, 2, 3};
            input_keys->append_datum(datum);
        }
    }
    ColumnPtr input_values = ColumnHelper::create_column(input_values_type, true);
    {
        // [a]
        {
            DatumArray datum{"a"};
            input_values->append_datum(datum);
        }
    }
    auto ret = MapFunctions::map_from_arrays(nullptr, {input_keys, input_values});
    EXPECT_FALSE(ret.ok());
}

PARALLEL_TEST(MapFunctionsTest, test_map_function) {
    TypeDescriptor type_map_int_int = map_type(TYPE_INT, TYPE_INT);

    ColumnPtr column = ColumnHelper::create_column(type_map_int_int, true);

    DatumMap map;
    map[(int32_t)1] = (int32_t)11;
    map[(int32_t)2] = (int32_t)22;
    map[(int32_t)3] = (int32_t)33;
    column->append_datum(map);

    DatumMap map1;
    map1[(int32_t)1] = (int32_t)44;
    map1[(int32_t)2] = (int32_t)55;
    map1[(int32_t)4] = (int32_t)66;
    column->append_datum(map1);

    DatumMap map2;
    map2[(int32_t)2] = (int32_t)77;
    map2[(int32_t)3] = (int32_t)88;
    column->append_datum(map2);

    DatumMap map3;
    map3[(int32_t)2] = (int32_t)99;
    column->append_datum(map3);

    column->append_datum(DatumMap());

    column->append_default();

    // Inputs:
    //   c0
    // --------
    //   [1->11, 2->22, 3->33]
    //   [1->44, 2->55, 4->66]
    //   [2->77, 3->88]
    //   [2->99]
    //   [NULL]
    //   NULL
    //
    // Query:
    //   map_size(c0)
    //
    // Outputs:
    //   3
    //   3
    //   2
    //   1
    //   0
    //   NULL

    auto result = MapFunctions::map_size(nullptr, {column}).value();
    EXPECT_EQ(6, result->size());

    ASSERT_FALSE(result->get(0).is_null());
    ASSERT_FALSE(result->get(1).is_null());
    ASSERT_FALSE(result->get(2).is_null());
    ASSERT_FALSE(result->get(3).is_null());
    ASSERT_FALSE(result->get(4).is_null());
    ASSERT_TRUE(result->get(5).is_null());

    EXPECT_EQ(3, result->get(0).get_int32());
    EXPECT_EQ(3, result->get(1).get_int32());
    EXPECT_EQ(2, result->get(2).get_int32());
    EXPECT_EQ(1, result->get(3).get_int32());
    EXPECT_EQ(0, result->get(4).get_int32());

    // Inputs:
    //   c0
    // --------
    //   [1->11, 2->22, 3->33]
    //   [1->44, 2->55, 4->66]
    //   [2->77, 3->88]
    //   [2->99]
    //   [NULL]
    //   NULL
    //
    // Query:
    //   map_keys(c0)
    //
    // Outputs:
    //   [1,2,3]
    //   [1,2,4]
    //   [2,3]
    //   [2]
    //   []
    //   NULL
    auto result_keys = MapFunctions::map_keys(nullptr, {column}).value();
    EXPECT_EQ(6, result->size());

    EXPECT_EQ(3, result_keys->get(0).get_array().size());
    EXPECT_EQ(3, result_keys->get(1).get_array().size());
    EXPECT_EQ(2, result_keys->get(2).get_array().size());
    EXPECT_EQ(1, result_keys->get(3).get_array().size());
    EXPECT_EQ(0, result_keys->get(4).get_array().size());
    ASSERT_TRUE(result_keys->get(5).is_null());

    // result_keys[2] = [2, 3]
    EXPECT_EQ(2, result_keys->get(2).get_array()[0].get_int32());
    EXPECT_EQ(3, result_keys->get(2).get_array()[1].get_int32());

    // Inputs:
    //   c0
    // --------
    //   [1->11, 2->22, 3->33]
    //   [1->44, 2->55, 4->66]
    //   [2->77, 3->88]
    //   [2->99]
    //   [NULL]
    //   NULL
    //
    // Query:
    //   map_values(c0)
    //
    // Outputs:
    //   [11,22,33]
    //   [44,55,66]
    //   [77,88]
    //   [99]
    //   []
    //   NULL
    auto result_values = MapFunctions::map_values(nullptr, {column}).value();
    EXPECT_EQ(6, result->size());

    EXPECT_EQ(3, result_values->get(0).get_array().size());
    EXPECT_EQ(3, result_values->get(1).get_array().size());
    EXPECT_EQ(2, result_values->get(2).get_array().size());
    EXPECT_EQ(1, result_values->get(3).get_array().size());
    EXPECT_EQ(0, result_values->get(4).get_array().size());
    ASSERT_TRUE(result_keys->get(5).is_null());

    // result_values[1] = [44, 55, 66]
    EXPECT_EQ(44, result_values->get(1).get_array()[0].get_int32());
    EXPECT_EQ(55, result_values->get(1).get_array()[1].get_int32());
    EXPECT_EQ(66, result_values->get(1).get_array()[2].get_int32());
}

PARALLEL_TEST(MapFunctionsTest, test_map_filter_int_nullable) {
    TypeDescriptor type_map_int_int = map_type(TYPE_INT, TYPE_INT);

    ColumnPtr map_column_nullable = ColumnHelper::create_column(type_map_int_int, true);
    {
        //   [1->44, 2->55, 4->66]
        //   [2->77, 3->88]
        //   [3 -> NULL]
        //   []
        //   NULL
        DatumMap map1;
        map1[(int32_t)1] = (int32_t)44;
        map1[(int32_t)2] = (int32_t)55;
        map1[(int32_t)4] = (int32_t)66;
        map_column_nullable->append_datum(map1);

        DatumMap map2;
        map2[(int32_t)2] = (int32_t)77;
        map2[(int32_t)3] = (int32_t)88;
        map_column_nullable->append_datum(map2);

        DatumMap map3;
        map3[(int32_t)3] = Datum();
        map_column_nullable->append_datum(map3);

        // {} empty
        map_column_nullable->append_datum(DatumMap());
        // NULL
        map_column_nullable->append_datum(Datum{});
    }

    ColumnPtr map_column_not_nullable = ColumnHelper::create_column(type_map_int_int, false);
    {
        //   [1->44, 2->55, 4->66]
        //   [2->77, 3->88]
        //   [3 -> NULL]
        //   []
        //   []
        DatumMap map1;
        map1[(int32_t)1] = (int32_t)44;
        map1[(int32_t)2] = (int32_t)55;
        map1[(int32_t)4] = (int32_t)66;
        map_column_not_nullable->append_datum(map1);

        DatumMap map2;
        map2[(int32_t)2] = (int32_t)77;
        map2[(int32_t)3] = (int32_t)88;
        map_column_not_nullable->append_datum(map2);

        DatumMap map3;
        map3[(int32_t)3] = Datum();
        map_column_not_nullable->append_datum(map3);

        // {} empty
        map_column_not_nullable->append_datum(DatumMap());
        map_column_not_nullable->append_datum(DatumMap());
    }

    TypeDescriptor TYPE_ARRAY_BOOLEAN = array_type(TYPE_BOOLEAN);

    // [null, true, false]
    // []
    // [false]
    // [NULL]
    // [true]
    ColumnPtr bool_array_not_nullable = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
    bool_array_not_nullable->append_datum(DatumArray{Datum(), true, false});
    bool_array_not_nullable->append_datum(DatumArray{});
    bool_array_not_nullable->append_datum(DatumArray{false});
    bool_array_not_nullable->append_datum(DatumArray{Datum()});
    bool_array_not_nullable->append_datum(DatumArray{true});

    // [null, true, false]
    // NULL
    // [false]
    // [null]
    // [false, null]
    ColumnPtr bool_array_nullable = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    bool_array_nullable->append_datum(DatumArray{Datum(), true, false});
    bool_array_nullable->append_datum(Datum{});
    bool_array_nullable->append_datum(DatumArray{false});
    bool_array_nullable->append_datum(DatumArray{Datum()});
    bool_array_nullable->append_datum(DatumArray{false, Datum()});

    {
        auto result = MapFunctions::map_filter(nullptr, {map_column_nullable, bool_array_not_nullable}).value();
        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "[{2:55}, {}, {}, {}, NULL]");
    }
    {
        auto result = MapFunctions::map_filter(nullptr, {map_column_nullable, bool_array_nullable}).value();
        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "[{2:55}, {}, {}, {}, NULL]");
    }
    {
        auto result = MapFunctions::map_filter(nullptr, {map_column_not_nullable, bool_array_not_nullable}).value();
        EXPECT_FALSE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "{2:55}, {}, {}, {}, {}");
    }
    {
        auto result = MapFunctions::map_filter(nullptr, {map_column_not_nullable, bool_array_nullable}).value();
        EXPECT_FALSE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "{2:55}, {}, {}, {}, {}");
    }
    ColumnPtr only_null_column = ColumnHelper::create_const_null_column(1);
    {
        auto result = MapFunctions::map_filter(nullptr, {map_column_nullable, only_null_column}).value();
        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "[{}, {}, {}, {}, NULL]");
    }
    {
        auto result = MapFunctions::map_filter(nullptr, {map_column_not_nullable, only_null_column}).value();
        EXPECT_FALSE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "{}, {}, {}, {}, {}");
    }
    {
        auto result = MapFunctions::map_filter(nullptr, {only_null_column, only_null_column}).value();
        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "CONST: NULL Size : 1");
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(MapFunctionsTest, test_distinct_map_keys) {
    {
        UInt32Column::Ptr offsets = UInt32Column::create();
        Int32Column::Ptr keys_data = Int32Column::create();
        NullColumn::Ptr keys_null = NullColumn::create();
        NullableColumn::Ptr keys = NullableColumn::create(keys_data, keys_null);
        Int32Column::Ptr values_data = Int32Column::create();
        NullColumn::Ptr values_null = NullColumn::create();
        NullableColumn::Ptr values = NullableColumn::create(values_data, values_null);
        MapColumn::Ptr column = MapColumn::create(keys, values, offsets);

        DatumMap map;
        map[(int32_t)1] = (int32_t)11;
        map[(int32_t)22] = (int32_t)22;
        map[(int32_t)22] = (int32_t)33;
        column->append_datum(map);

        DatumMap map1;
        map1[(int32_t)4] = (int32_t)44;
        map1[(int32_t)4] = (int32_t)55;
        map1[(int32_t)4] = (int32_t)66;
        column->append_datum(map1);

        DatumMap map3;
        map3[(int32_t)3] = Datum();
        column->append_datum(map3);

        // {} empty
        column->append_datum(DatumMap());

        auto res = MapFunctions::distinct_map_keys(nullptr, {column}).value();

        ASSERT_EQ("{1:11,22:33}", res->debug_item(0));
        ASSERT_EQ("{4:66}", res->debug_item(1));
        ASSERT_EQ("{3:NULL}", res->debug_item(2));
        ASSERT_EQ("{}", res->debug_item(3));
    }
    {
        UInt32Column::Ptr offsets = UInt32Column::create();
        BinaryColumn::Ptr keys_data = BinaryColumn::create();
        NullColumn::Ptr keys_null = NullColumn::create();
        NullableColumn::Ptr keys = NullableColumn::create(keys_data, keys_null);
        BinaryColumn::Ptr values_data = BinaryColumn::create();
        NullColumn::Ptr null_column = NullColumn::create();
        NullableColumn::Ptr values = NullableColumn::create(values_data, null_column);
        MapColumn::Ptr column = MapColumn::create(keys, values, offsets);

        DatumMap map;
        map[(Slice) "a"] = (Slice) "hello";
        map[(Slice) "b"] = (Slice) " ";
        map[(Slice) "a"] = (Slice) "world";
        column->append_datum(map);

        DatumMap map1;
        map1[(Slice) "def"] = (Slice) "haha";
        map1[(Slice) "g h"] = (Slice) "let's dance";
        column->append_datum(map1);

        auto res = MapFunctions::distinct_map_keys(nullptr, {column}).value();

        ASSERT_EQ("{'a':'world','b':' '}", res->debug_item(0));
        ASSERT_EQ("{'def':'haha','g h':'let's dance'}", res->debug_item(1));
    }
    { // nested map
        UInt32Column::Ptr offsets = UInt32Column::create();
        Int32Column::Ptr keys_data = Int32Column::create();
        NullColumn::Ptr keys_null = NullColumn::create();
        NullableColumn::Ptr keys = NullableColumn::create(keys_data, keys_null);
        Int32Column::Ptr values_data = Int32Column::create();
        NullColumn::Ptr values_null = NullColumn::create();
        NullableColumn::Ptr values = NullableColumn::create(values_data, values_null);
        MapColumn::Ptr column = MapColumn::create(keys, values, offsets);

        DatumMap map;
        map[(int32_t)1] = (int32_t)11;
        map[(int32_t)22] = (int32_t)22;
        map[(int32_t)22] = (int32_t)33;
        column->append_datum(map);

        DatumMap map1;
        map1[(int32_t)4] = (int32_t)44;
        map1[(int32_t)4] = (int32_t)55;
        map1[(int32_t)4] = (int32_t)66;
        column->append_datum(map1);

        DatumMap map3;
        map3[(int32_t)3] = Datum();
        column->append_datum(map3);

        // {} empty
        column->append_datum(DatumMap());

        UInt32Column::Ptr nest_offsets = UInt32Column::create();
        auto nest_keys = keys->clone_empty();
        nest_keys->append_datum(1);
        nest_keys->append_datum(1);
        nest_keys->append_datum(1);
        nest_keys->append_datum(1);
        nest_offsets->get_data().push_back(0);
        nest_offsets->get_data().push_back(2);
        nest_offsets->get_data().push_back(4);

        auto nest_map =
                MapColumn::create(std::move(nest_keys), ColumnHelper::cast_to_nullable_column(column), nest_offsets);
        auto res = MapFunctions::distinct_map_keys(nullptr, {nest_map}).value();

        ASSERT_EQ("{1:{4:66}}", res->debug_item(0));
        ASSERT_EQ("{1:{}}", res->debug_item(1));
    }
}

PARALLEL_TEST(MapFunctionsTest, test_map_concat) {
    TypeDescriptor type_map_int_int = map_type(TYPE_INT, TYPE_INT);

    ColumnPtr map_column_nullable = ColumnHelper::create_column(type_map_int_int, true);
    {
        //   [11->44, 2->55, 4->66]
        //   [2->77, 3->88]
        //   [3 -> NULL]
        //   []
        //   NULL
        DatumMap map1;
        map1[(int32_t)11] = (int32_t)44;
        map1[(int32_t)2] = (int32_t)55;
        map1[(int32_t)4] = (int32_t)66;
        map_column_nullable->append_datum(map1);

        DatumMap map2;
        map2[(int32_t)2] = (int32_t)77;
        map2[(int32_t)3] = (int32_t)88;
        map_column_nullable->append_datum(map2);

        DatumMap map3;
        map3[(int32_t)3] = Datum();
        map_column_nullable->append_datum(map3);

        // {} empty
        map_column_nullable->append_datum(DatumMap());
        // NULL
        map_column_nullable->append_datum(Datum{});
    }

    ColumnPtr map_column_not_nullable = ColumnHelper::create_column(type_map_int_int, false);
    {
        //   [1->44, 2->55, 4->66]
        //   [2->77, 3->88]
        //   [3 -> NULL]
        //   []
        //   []
        DatumMap map1;
        map1[(int32_t)1] = (int32_t)44;
        map1[(int32_t)2] = (int32_t)55;
        map1[(int32_t)4] = (int32_t)66;
        map_column_not_nullable->append_datum(map1);

        DatumMap map2;
        map2[(int32_t)2] = (int32_t)77;
        map2[(int32_t)3] = (int32_t)88;
        map_column_not_nullable->append_datum(map2);

        DatumMap map3;
        map3[(int32_t)3] = Datum();
        map_column_not_nullable->append_datum(map3);

        // {} empty
        map_column_not_nullable->append_datum(DatumMap());
        map_column_not_nullable->append_datum(DatumMap());
    }

    ColumnPtr only_null_column = ColumnHelper::create_const_null_column(5);

    auto mapn = down_cast<NullableColumn*>(map_column_nullable->clone().get())->data_column();

    ConstColumn::Ptr const_column = ConstColumn::create(mapn, 5);

    {
        auto result = MapFunctions::map_concat(nullptr, {map_column_nullable, map_column_not_nullable}).value();
        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "[{1:44,2:55,4:66,11:44}, {2:77,3:88}, {3:NULL}, {}, {}]");
    }

    {
        auto result = MapFunctions::map_concat(nullptr, {map_column_nullable, map_column_nullable}).value();
        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "[{2:55,4:66,11:44}, {2:77,3:88}, {3:NULL}, {}, NULL]");
    }
    {
        auto result = MapFunctions::map_concat(nullptr, {map_column_not_nullable, map_column_not_nullable}).value();
        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "[{1:44,2:55,4:66}, {2:77,3:88}, {3:NULL}, {}, {}]");
    }
    {
        auto result = MapFunctions::map_concat(nullptr,
                                               {map_column_not_nullable, map_column_not_nullable, map_column_nullable})
                              .value();
        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "[{2:55,4:66,11:44,1:44}, {2:77,3:88}, {3:NULL}, {}, {}]");
    }
    {
        auto result = MapFunctions::map_concat(nullptr, {map_column_not_nullable}).value();
        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "[{1:44,2:55,4:66}, {2:77,3:88}, {3:NULL}, {}, {}]");
    }
    {
        auto result = MapFunctions::map_concat(nullptr, {map_column_nullable}).value();
        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "[{2:55,4:66,11:44}, {2:77,3:88}, {3:NULL}, {}, NULL]");
    }
    {
        auto result = MapFunctions::map_concat(nullptr, {map_column_nullable, only_null_column}).value();
        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "[{2:55,4:66,11:44}, {2:77,3:88}, {3:NULL}, {}, NULL]");
    }
    {
        auto result = MapFunctions::map_concat(nullptr, {only_null_column}).value();
        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "CONST: NULL Size : 5");
    }
    {
        auto result = MapFunctions::map_concat(nullptr, {const_column}).value();
        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(),
                     "[{2:55,4:66,11:44}, {2:55,4:66,11:44}, {2:55,4:66,11:44}, {2:55,4:66,11:44}, {2:55,4:66,11:44}]");
    }
    {
        auto result =
                MapFunctions::map_concat(nullptr, {const_column, only_null_column, map_column_not_nullable}).value();
        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(),
                     "[{1:44,2:55,4:66,11:44}, {2:77,3:88,4:66,11:44}, {3:NULL,2:55,4:66,11:44}, {2:55,4:66,11:44}, "
                     "{2:55,4:66,11:44}]");
    }
}

} // namespace starrocks
