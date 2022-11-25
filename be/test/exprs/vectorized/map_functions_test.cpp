// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/map_functions.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/map_column.h"
#include "testutil/parallel_test.h"

namespace starrocks::vectorized {

PARALLEL_TEST(MapFunctionsTest, test_map_function) {
    TypeDescriptor type_map_int_int;
    type_map_int_int.type = PrimitiveType::TYPE_MAP;
    type_map_int_int.children.emplace_back(TypeDescriptor(PrimitiveType::TYPE_INT));
    type_map_int_int.children.emplace_back(TypeDescriptor(PrimitiveType::TYPE_INT));

    auto column = ColumnHelper::create_column(type_map_int_int, true);

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

} // namespace starrocks::vectorized
