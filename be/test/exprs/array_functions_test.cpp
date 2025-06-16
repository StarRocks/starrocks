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

#include "exprs/array_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <unordered_set>

#include "column/const_column.h"
#include "column/map_column.h"
#include "exprs/mock_vectorized_expr.h"

namespace starrocks {

TypeDescriptor array_type(const TypeDescriptor& child_type) {
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.children.emplace_back(child_type);
    return t;
}

TypeDescriptor array_type(const LogicalType& child_type) {
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.children.resize(1);
    t.children[0].type = child_type;
    t.children[0].len = child_type == TYPE_VARCHAR ? 10 : child_type == TYPE_CHAR ? 10 : -1;
    return t;
}

class ArrayFunctionsTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}

    TypeDescriptor TYPE_ARRAY_BOOLEAN = array_type(TYPE_BOOLEAN);
    TypeDescriptor TYPE_ARRAY_TINYINT = array_type(TYPE_TINYINT);
    TypeDescriptor TYPE_ARRAY_SMALLINT = array_type(TYPE_SMALLINT);
    TypeDescriptor TYPE_ARRAY_INT = array_type(TYPE_INT);
    TypeDescriptor TYPE_ARRAY_LARGEINT = array_type(TYPE_LARGEINT);
    TypeDescriptor TYPE_ARRAY_FLOAT = array_type(TYPE_FLOAT);
    TypeDescriptor TYPE_ARRAY_DOUBLE = array_type(TYPE_DOUBLE);
    TypeDescriptor TYPE_ARRAY_VARCHAR = array_type(TYPE_VARCHAR);
    TypeDescriptor TYPE_ARRAY_ARRAY_INT = array_type(array_type(TYPE_INT));
    TypeDescriptor TYPE_ARRAY_ARRAY_VARCHAR = array_type(array_type(TYPE_VARCHAR));

    TypeDescriptor TYPE_ARRAY_BIGINT = array_type(TYPE_BIGINT);
    TypeDescriptor TYPE_ARRAY_DATE = array_type(TYPE_DATE);
    TypeDescriptor TYPE_ARRAY_DATETIME = array_type(TYPE_DATETIME);

protected:
    template <typename CppType>
    void _check_array(const Buffer<CppType>& check_values, const DatumArray& value);

    template <typename CppType>
    void _check_array_nullable(const Buffer<CppType>& check_values, const Buffer<uint8_t>& nulls,
                               const DatumArray& value);

    FunctionContext _ctx;
};

template <typename CppType>
void ArrayFunctionsTest::_check_array(const Buffer<CppType>& check_values, const DatumArray& value) {
    ASSERT_EQ(check_values.size(), value.size());
    if constexpr (std::is_same_v<CppType, uint8_t>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_uint8());
        }
    } else if constexpr (std::is_same_v<CppType, int8_t>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_int8());
        }
    } else if constexpr (std::is_same_v<CppType, int16_t>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_int16());
        }
    } else if constexpr (std::is_same_v<CppType, int32_t>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_int32());
        }
    } else if constexpr (std::is_same_v<CppType, int64_t>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_int64());
        }
    } else if constexpr (std::is_same_v<CppType, int128_t>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_int128());
        }
    } else if constexpr (std::is_same_v<CppType, float>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_float());
        }
    } else if constexpr (std::is_same_v<CppType, double>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_double());
        }
    } else if constexpr (std::is_same_v<CppType, Slice>) {
        for (size_t i = 0; i < value.size(); i++) {
            ASSERT_EQ(check_values[i], value[i].get_slice());
        }
    } else {
        ASSERT_TRUE(false);
    }
}

template <typename CppType>
void ArrayFunctionsTest::_check_array_nullable(const Buffer<CppType>& check_values, const Buffer<uint8_t>& nulls,
                                               const DatumArray& value) {
    ASSERT_EQ(check_values.size(), value.size());
    if constexpr (std::is_same_v<CppType, int32_t>) {
        for (size_t i = 0; i < value.size(); i++) {
            if (nulls[i]) {
                ASSERT_TRUE(value[i].is_null());
            } else {
                ASSERT_FALSE(value[i].is_null());
                ASSERT_EQ(check_values[i], value[i].get_int32());
            }
        }
    } else if constexpr (std::is_same_v<CppType, Slice>) {
        for (size_t i = 0; i < value.size(); i++) {
            if (nulls[i]) {
                ASSERT_TRUE(value[i].is_null());
            } else {
                ASSERT_FALSE(value[i].is_null());
                ASSERT_EQ(check_values[i], value[i].get_slice());
            }
        }
    } else {
        ASSERT_TRUE(false);
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_length) {
    // []
    // NULL
    // [NULL]
    // [1]
    // [1, 2]
    {
        ColumnPtr c = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        c->append_datum(Datum(DatumArray{}));
        c->append_datum(Datum());
        c->append_datum(Datum(DatumArray{Datum()}));
        c->append_datum(Datum(DatumArray{Datum((int32_t)1)}));
        c->append_datum(Datum(DatumArray{Datum((int32_t)1), Datum((int32_t)2)}));

        auto result = ArrayFunctions::array_length(nullptr, {c}).value();
        EXPECT_EQ(5, result->size());

        EXPECT_EQ(result->get(0), Datum(0));
        EXPECT_EQ(result->get(1), kNullDatum);
        EXPECT_EQ(result->get(2), Datum(1));
        EXPECT_EQ(result->get(3), Datum(1));
        EXPECT_EQ(result->get(4), Datum(2));
    }

    // []
    // NULL
    // [NULL]
    // ["a"]
    // ["a", "b"]
    {
        ColumnPtr c = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        c->append_datum(Datum(DatumArray{}));
        c->append_datum(Datum());
        c->append_datum(Datum(DatumArray{Datum()}));
        c->append_datum(Datum(DatumArray{Datum("a")}));
        c->append_datum(Datum(DatumArray{Datum("a"), Datum("b")}));

        auto result = ArrayFunctions::array_length(nullptr, {c}).value();
        EXPECT_EQ(5, result->size());

        ASSERT_FALSE(result->get(0).is_null());
        ASSERT_TRUE(result->get(1).is_null());
        ASSERT_FALSE(result->get(2).is_null());
        ASSERT_FALSE(result->get(3).is_null());
        ASSERT_FALSE(result->get(4).is_null());

        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(1, result->get(2).get_int32());
        EXPECT_EQ(1, result->get(3).get_int32());
        EXPECT_EQ(2, result->get(4).get_int32());
    }

    // []
    // NULL
    // [NULL]
    // [[NULL]]
    // [[]]
    // [[],[]]
    // [[1], [2], [3]]
    {
        ColumnPtr c = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, true);
        c->append_datum(Datum(DatumArray{}));
        c->append_datum(Datum());
        c->append_datum(Datum(DatumArray{Datum()}));
        c->append_datum(Datum(DatumArray{Datum(DatumArray{Datum()})}));
        c->append_datum(Datum(DatumArray{Datum(DatumArray{})}));
        c->append_datum(Datum(DatumArray{Datum(DatumArray{}), Datum(DatumArray{})}));
        c->append_datum(Datum(DatumArray{Datum(DatumArray{Datum((int32_t)1)}), Datum(DatumArray{Datum((int32_t)2)}),
                                         Datum(DatumArray{Datum((int32_t)3)})}));

        auto result = ArrayFunctions::array_length(nullptr, {c}).value();
        EXPECT_EQ(7, result->size());

        ASSERT_FALSE(result->get(0).is_null());
        ASSERT_TRUE(result->get(1).is_null());
        ASSERT_FALSE(result->get(2).is_null());
        ASSERT_FALSE(result->get(3).is_null());
        ASSERT_FALSE(result->get(4).is_null());
        ASSERT_FALSE(result->get(5).is_null());
        ASSERT_FALSE(result->get(6).is_null());

        auto datum = Datum(DatumArray{DatumArray{Datum()}});
        LOG(INFO) << "datum size=" << datum.get_array().size();

        LOG(INFO) << c->debug_string();
        LOG(INFO) << result->debug_string();
        EXPECT_EQ(0, result->get(0).get_int32());
        ASSERT_TRUE(result->get(1).is_null());
        EXPECT_EQ(1, result->get(2).get_int32());
        EXPECT_EQ(1, result->get(3).get_int32());
        EXPECT_EQ(1, result->get(4).get_int32());
        EXPECT_EQ(2, result->get(5).get_int32());
        EXPECT_EQ(3, result->get(6).get_int32());
    }

    // [] only null
    {
        ColumnPtr c = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, true, true, 10);

        auto result = ArrayFunctions::array_length(nullptr, {c}).value();
        EXPECT_EQ(10, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    // [] only const
    {
        ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        src_column->append_datum(DatumArray{"5", "5", "33", "666"});
        src_column = ConstColumn::create(src_column, 3);

        auto result = ArrayFunctions::array_length(nullptr, {src_column}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(4, result->get(1).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_cum_sum) {
    // []
    // NULL
    // [NULL]
    // [1]
    // [1,2,3,4,5]
    // [null,null,1, null]
    {
        ColumnPtr c = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
        c->append_datum(Datum(DatumArray{}));
        c->append_datum(Datum());
        c->append_datum(Datum(DatumArray{Datum()}));
        c->append_datum(Datum(DatumArray{Datum((int64_t)1)}));
        c->append_datum(Datum(DatumArray{Datum((int64_t)1), Datum((int64_t)2), Datum((int64_t)3), Datum((int64_t)4),
                                         Datum((int64_t)5)}));
        c->append_datum(Datum(DatumArray{Datum(), Datum(), Datum((int64_t)1), Datum()}));

        auto result = ArrayFunctions::array_cum_sum_bigint(nullptr, {c}).value();
        EXPECT_EQ(6, result->size());

        ASSERT_FALSE(result->get(0).is_null());
        ASSERT_TRUE(result->get(1).is_null());
        ASSERT_FALSE(result->get(2).is_null());
        ASSERT_FALSE(result->get(3).is_null());
        ASSERT_FALSE(result->get(4).is_null());
        ASSERT_FALSE(result->get(5).is_null());

        EXPECT_EQ(0, result->get(0).get_array().size());
        EXPECT_EQ(1, result->get(2).get_array().size());
        EXPECT_EQ(1, result->get(3).get_array().size());
        EXPECT_EQ(5, result->get(4).get_array().size());
        EXPECT_EQ(4, result->get(5).get_array().size());
    }

    // [] only null
    {
        ColumnPtr c = ColumnHelper::create_const_null_column(3);

        auto result = ArrayFunctions::array_cum_sum_bigint(nullptr, {c}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
    }

    // [] only const
    {
        ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
        src_column->append_datum(Datum(DatumArray{Datum((int64_t)1), Datum((int64_t)2), Datum((int64_t)3),
                                                  Datum((int64_t)4), Datum((int64_t)5)}));
        auto c = ConstColumn::create(src_column, 3);
        auto result = ArrayFunctions::array_cum_sum_bigint(nullptr, {std::move(c)}).value();
        EXPECT_EQ(3, result->size());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_empty_array) {
    // array_contains([], 1)
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        target->append_datum(Datum{(int32_t)1});

        auto result = ArrayFunctions::array_contains_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // array_contains([], "abc")
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum{"abc"});

        auto result = ArrayFunctions::array_contains_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // array_contains(ARRAY<ARRAY<int>>[], [1])
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), false);
        target->append_datum(Datum(DatumArray{Datum{(int32_t)1}}));

        auto result = ArrayFunctions::array_contains_generic(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // array_contains(ARRAY<ARRAY<int>>[], ARRAY<int>[])
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), false);
        target->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_contains_generic(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // multiple lines with const target:
    //  array_contains([], 1);
    //  array_contains([], 1);
    //  array_contains([], 1);
    //  array_contains([], 1);
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        DCHECK(target->is_constant());
        target->append_datum(Datum((int32_t)1));
        target->resize(4);

        auto result = ArrayFunctions::array_contains_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());
    }
    // multiple lines with different target:
    //  array_contains([], 1);
    //  array_contains([], 2);
    //  array_contains([], NULL);
    //  array_contains([], 3);
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        target->append_datum(Datum((int32_t)1));
        target->append_datum(Datum((int32_t)2));
        target->append_datum(Datum{});
        target->append_datum(Datum((int32_t)3));

        auto result = ArrayFunctions::array_contains_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());
    }
    // multiple lines with Only-NULL target:
    //  array_contains([], NULL);
    //  array_contains([], NULL);
    //  array_contains([], NULL);
    //  array_contains([], NULL);
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_const_null_column(1);
        target->resize(4);

        auto result = ArrayFunctions::array_contains_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());

        array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        result = ArrayFunctions::array_contains_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());

        array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        result = ArrayFunctions::array_contains_generic(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_no_null) {
    /// Test class:
    ///  - Both the array elements and targets has NO NULL.

    // array_contains(array<boolean>[], 0) : 0
    // array_contains(array<boolean>[], 1) : 0
    // array_contains(array<boolean>[0], 0) : 1
    // array_contains(array<boolean>[0], 1) : 0
    // array_contains(array<boolean>[1], 0) : 0
    // array_contains(array<boolean>[1], 1) : 1
    // array_contains(array<boolean>[1,0], 0) : 1
    // array_contains(array<boolean>[1,0], 1) : 1
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), false);
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});

        auto result = ArrayFunctions::array_contains_specific<TYPE_BOOLEAN>(nullptr, {array, target}).value();
        EXPECT_EQ(8, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(1, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());
        EXPECT_EQ(0, result->get(4).get_int8());
        EXPECT_EQ(1, result->get(5).get_int8());
        EXPECT_EQ(1, result->get(6).get_int8());
        EXPECT_EQ(1, result->get(7).get_int8());
    }
    // array_contains([], 3) : 0
    // array_contains([2], 3) : 0
    // array_contains([1, 2, 3], 3) : 1
    // array_contains([3, 2, 1], 3) : 1
    // array_contains([2, 1, 3], 3) : 1
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        target->append_datum(Datum{3});
        target->resize(5);

        auto result = ArrayFunctions::array_contains_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(5, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(1, result->get(2).get_int8());
        EXPECT_EQ(1, result->get(3).get_int8());
        EXPECT_EQ(1, result->get(4).get_int8());
    }
    // array_contains([], []) : 0
    // array_contains([[]], []) : 1
    // array_contains([["d", "o"], ["r"], ["i", "s"]], []) : 0
    // array_contains([["d", "o"], ["r"], ["i", "s"]], ["d"]) : 0
    // array_contains([["d", "o"], ["r"], ["i", "s"]], ["d", "o"]) : 1
    // array_contains([["d", "o"], ["r"], ["i", "s"]], ["o", "d"]) : 0
    // array_contains([["d", "o"], ["r"], ["i", "s"]], ["r"]) : 1
    // array_contains([["d", "o"], ["r"], ["i", "s"]], ["ri"]) : 0
    // array_contains([["d", "o"], ["r"], ["i", "s"]], ["r", "i"]) : 0
    // array_contains([["d", "o"], ["r"], ["i", "s"]], ["i", "s"]) : 1
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum(DatumArray{})});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});

        ColumnPtr target = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(DatumArray{"d"});
        target->append_datum(DatumArray{"d", "o"});
        target->append_datum(DatumArray{"o", "d"});
        target->append_datum(DatumArray{"r"});
        target->append_datum(DatumArray{"ri"});
        target->append_datum(DatumArray{"r", "i"});
        target->append_datum(DatumArray{"i", "s"});

        auto result = ArrayFunctions::array_contains_generic(nullptr, {array, target}).value();
        EXPECT_EQ(10, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(1, result->get(1).get_int8());
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());
        EXPECT_EQ(1, result->get(4).get_int8());
        EXPECT_EQ(0, result->get(5).get_int8());
        EXPECT_EQ(1, result->get(6).get_int8());
        EXPECT_EQ(0, result->get(7).get_int8());
        EXPECT_EQ(0, result->get(8).get_int8());
        EXPECT_EQ(1, result->get(9).get_int8());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_has_null_element) {
    // array_contains([NULL], "abc")
    // array_contains(["abc", NULL], "abc")
    // array_contains([NULL, "abc"], "abc")
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum{}});
        array->append_datum(DatumArray{"abc", Datum{}});
        array->append_datum(DatumArray{Datum{}, "abc"});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum{"abc"});
        target->append_datum(Datum{"abc"});
        target->append_datum(Datum{"abc"});

        auto result = ArrayFunctions::array_contains_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(1, result->get(1).get_int8());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_has_null_target) {
    // array_contains(["abc", "def"], NULL)
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{"abc", "def"});

        // const-null column.
        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 0);

        auto result = ArrayFunctions::array_contains_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // array_contains(ARRAY<TINYINT>[1, 2, 3], 2)
    // array_contains(ARRAY<TINYINT>[1, 2, 3], 4)
    // array_contains(ARRAY<TINYINT>[1, 2, 3], NULL)
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
        target->append_datum(Datum((int8_t)2));
        target->append_datum(Datum((int8_t)4));
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_contains_specific<TYPE_TINYINT>(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(1, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(0, result->get(2).get_int8());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_has_null_element_and_target) {
    // array_contains([NULL], NULL)
    // array_contains([NULL, "abc"], NULL)
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), "abc"});

        // const-null column.
        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 0);

        auto result = ArrayFunctions::array_contains_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(2, result->size());
        EXPECT_EQ(1, result->get(0).get_int8());
        EXPECT_EQ(1, result->get(1).get_int8());
    }
    // array_contains([NULL], NULL)
    // array_contains([NULL, [1,2]], NULL)
    // array_contains([NULL, [1,2]], [1,2])
    // array_contains([[1,2], NULL], [1,2])
    // array_contains([[1,2], NULL], NULL)
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), true);
        target->append_datum(Datum());
        target->append_datum(Datum());
        target->append_datum(DatumArray{1, 2});
        target->append_datum(DatumArray{1, 2});
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_contains_generic(nullptr, {array, target}).value();
        EXPECT_EQ(5, result->size());
        EXPECT_EQ(1, result->get(0).get_int8());
        EXPECT_EQ(1, result->get(1).get_int8());
        EXPECT_EQ(1, result->get(2).get_int8());
        EXPECT_EQ(1, result->get(3).get_int8());
        EXPECT_EQ(1, result->get(4).get_int8());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_nullable_array) {
    // array_contains(["a", "b"], "c")
    // array_contains(NULL, "c")
    // array_contains(["a", "b", "c"], "c")
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{"a", "b"});
        array->append_datum(Datum());
        array->append_datum(DatumArray{"a", "b", "c"});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum("c"));
        target->append_datum(Datum("c"));
        target->append_datum(Datum("c"));

        auto result = ArrayFunctions::array_contains_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
    // array_contains([["a"], ["b"]], ["c"])
    // array_contains(NULL, ["c"])
    // array_contains([["a", "b"], ["c"]], ["c"])
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{DatumArray{"a"}, DatumArray{"b"}});
        array->append_datum(Datum());
        array->append_datum(DatumArray{DatumArray{"a", "b"}, DatumArray{"c"}});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_VARCHAR), false);
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});

        auto result = ArrayFunctions::array_contains_generic(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
    // array_contains(NULL, NULL)
    // array_contains(NULL, ["a"])
    // array_contains(NULL, [NULL])
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(Datum());
        array->append_datum(Datum());
        array->append_datum(Datum());

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_VARCHAR), true);
        target->append_datum(Datum());
        target->append_datum(DatumArray{"a"});
        target->append_datum(DatumArray{Datum()});

        auto result = ArrayFunctions::array_contains_generic(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->get(0).is_null());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_all) {
    // array_contains_all(["a", "b", "c"], ["c"])         -> 1
    // array_contains_all(NULL, ["c"])                    -> NULL
    // array_contains_all(["a", "b", "c"], NULL)          -> NULL
    // array_contains_all(["a", "b", NULL], NULL)         -> NULL
    // array_contains_all(["a", "b", NULL], ["a", NULL])  -> 1
    // array_contains_all(NULL, ["a", NULL])              -> NULL
    // array_contains_all(["a", "b", NULL], [NULL])       -> 1
    // array_contains_all(["a", "b", "c"], ["d"])         -> 0
    // array_contains_all(["a", "b", "c"], ["a", "d"])    -> 0
    // array_contains_all(["a", "b", "c"], ["a", "c"])    -> 1
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{"a", "b", "c"});
        array->append_datum(Datum());
        array->append_datum(DatumArray{"a", "b", "c"});
        array->append_datum(DatumArray{"a", "b", Datum()});
        array->append_datum(DatumArray{"a", "b", Datum()});
        array->append_datum(Datum());
        array->append_datum(DatumArray{"a", "b", Datum()});
        array->append_datum(DatumArray{"a", "b", "c"});
        array->append_datum(DatumArray{"a", "b", "c"});
        array->append_datum(DatumArray{"a", "b", "c"});

        ColumnPtr target = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});
        target->append_datum(Datum());
        target->append_datum(Datum());
        target->append_datum(DatumArray{"a", Datum()});
        target->append_datum(DatumArray{"a", Datum()});
        target->append_datum(DatumArray{Datum()});
        target->append_datum(DatumArray{"d"});
        target->append_datum(DatumArray{"a", "d"});
        target->append_datum(DatumArray{"a", "c"});
        FunctionContext ctx;
        auto result = ArrayFunctions::array_contains_all_specific<TYPE_VARCHAR>(&ctx, {array, target}).value();
        EXPECT_EQ(10, result->size());
        EXPECT_EQ(1, result->get(0).get_int8());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
        EXPECT_TRUE(result->get(3).is_null());
        EXPECT_EQ(1, result->get(4).get_int8());
        EXPECT_TRUE(result->get(5).is_null());
        EXPECT_EQ(1, result->get(6).get_int8());
        EXPECT_EQ(0, result->get(7).get_int8());
        EXPECT_EQ(0, result->get(8).get_int8());
        EXPECT_EQ(1, result->get(9).get_int8());
    }
    // array_contains_all([["a"], ["b"]], [["c"]])
    // array_contains_all(NULL, [["c"]])
    // array_contains_all([["a", "b"], ["c"], NULL], [["a", "b"], NULL])
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{Datum(DatumArray{"a"}), Datum(DatumArray{"b"})});
        array->append_datum(Datum());
        array->append_datum(DatumArray{Datum(DatumArray{"a", "b"}), Datum(DatumArray{"c"}), Datum()});

        ColumnPtr target = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        target->append_datum(DatumArray{Datum(DatumArray{"c"})});
        target->append_datum(DatumArray{Datum(DatumArray{"c"})});
        target->append_datum(DatumArray{Datum(DatumArray{"a", "b"}), Datum()});

        auto result = ArrayFunctions::array_contains_all(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_position_empty_array) {
    // array_position([], 1) : 0
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        target->append_datum(Datum{(int32_t)1});

        auto result = ArrayFunctions::array_position_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
    }
    // array_position([], "abc"): 0
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum{"abc"});

        auto result = ArrayFunctions::array_position_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
    }
    // array_position(ARRAY<ARRAY<int>>[], [1]): 0
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), false);
        target->append_datum(Datum(DatumArray{Datum{(int32_t)1}}));

        auto result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
    }
    // array_position(ARRAY<ARRAY<int>>[], ARRAY<int>[]): 0
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), false);
        target->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
    }
    // multiple lines with const target:
    //  array_position([], 1): 0;
    //  array_position([], 1): 0;
    //  array_position([], 1): 0;
    //  array_position([], 1): 0;
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        DCHECK(target->is_constant());
        target->append_datum(Datum((int32_t)1));
        target->resize(4);

        auto result = ArrayFunctions::array_position_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(0, result->get(2).get_int32());
        EXPECT_EQ(0, result->get(3).get_int32());
    }
    // multiple lines with different target:
    //  array_position([], 1): 0;
    //  array_position([], 2): 0;
    //  array_position([], NULL): 0;
    //  array_position([], 3): 0;
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        target->append_datum(Datum((int32_t)1));
        target->append_datum(Datum((int32_t)2));
        target->append_datum(Datum{});
        target->append_datum(Datum((int32_t)3));

        auto result = ArrayFunctions::array_position_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(0, result->get(2).get_int32());
        EXPECT_EQ(0, result->get(3).get_int32());
    }
    // multiple lines with Only-NULL target:
    //  array_position([], NULL): 0;
    //  array_position([], NULL): 0;
    //  array_position([], NULL): 0;
    //  array_position([], NULL): 0;
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_const_null_column(1);
        target->resize(4);

        auto result = ArrayFunctions::array_position_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(0, result->get(2).get_int32());
        EXPECT_EQ(0, result->get(3).get_int32());

        array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(0, result->get(2).get_int32());
        EXPECT_EQ(0, result->get(3).get_int32());

        array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(0, result->get(2).get_int32());
        EXPECT_EQ(0, result->get(3).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_position_no_null) {
    /// Test class:
    ///  - Both the array elements and targets has NO NULL.

    // array_position(array<boolean>[], 0) : 0
    // array_position(array<boolean>[], 1) : 0
    // array_position(array<boolean>[0], 0) : 1
    // array_position(array<boolean>[0], 1) : 0
    // array_position(array<boolean>[1], 0) : 0
    // array_position(array<boolean>[1], 1) : 1
    // array_position(array<boolean>[1,0], 0) : 2
    // array_position(array<boolean>[1,0], 1) : 1
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), false);
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});

        auto result = ArrayFunctions::array_position_specific<TYPE_BOOLEAN>(nullptr, {array, target}).value();
        EXPECT_EQ(8, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(1, result->get(2).get_int32());
        EXPECT_EQ(0, result->get(3).get_int32());
        EXPECT_EQ(0, result->get(4).get_int32());
        EXPECT_EQ(1, result->get(5).get_int32());
        EXPECT_EQ(2, result->get(6).get_int32());
        EXPECT_EQ(1, result->get(7).get_int32());
    }
    // array_position([], 3) : 0
    // array_position([2], 3) : 0
    // array_position([1, 2, 3], 3) : 3
    // array_position([3, 2, 1], 3) : 1
    // array_position([2, 1, 3], 3) : 3
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        target->append_datum(Datum{3});
        target->resize(5);

        auto result = ArrayFunctions::array_position_specific<TYPE_INT>(nullptr, {array, target}).value();
        EXPECT_EQ(5, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(3, result->get(2).get_int32());
        EXPECT_EQ(1, result->get(3).get_int32());
        EXPECT_EQ(3, result->get(4).get_int32());
    }
    // array_position([], []) : 0
    // array_position([[]], []) : 1
    // array_position([["d", "o"], ["r"], ["i", "s"]], []) : 0
    // array_position([["d", "o"], ["r"], ["i", "s"]], ["d"]) : 0
    // array_position([["d", "o"], ["r"], ["i", "s"]], ["d", "o"]) : 1
    // array_position([["d", "o"], ["r"], ["i", "s"]], ["o", "d"]) : 0
    // array_position([["d", "o"], ["r"], ["i", "s"]], ["r"]) : 2
    // array_position([["d", "o"], ["r"], ["i", "s"]], ["ri"]) : 0
    // array_position([["d", "o"], ["r"], ["i", "s"]], ["r", "i"]) : 0
    // array_position([["d", "o"], ["r"], ["i", "s"]], ["i", "s"]) : 3
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum(DatumArray{})});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});

        ColumnPtr target = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(DatumArray{"d"});
        target->append_datum(DatumArray{"d", "o"});
        target->append_datum(DatumArray{"o", "d"});
        target->append_datum(DatumArray{"r"});
        target->append_datum(DatumArray{"ri"});
        target->append_datum(DatumArray{"r", "i"});
        target->append_datum(DatumArray{"i", "s"});

        auto result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(10, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(1, result->get(1).get_int32());
        EXPECT_EQ(0, result->get(2).get_int32());
        EXPECT_EQ(0, result->get(3).get_int32());
        EXPECT_EQ(1, result->get(4).get_int32());
        EXPECT_EQ(0, result->get(5).get_int32());
        EXPECT_EQ(2, result->get(6).get_int32());
        EXPECT_EQ(0, result->get(7).get_int32());
        EXPECT_EQ(0, result->get(8).get_int32());
        EXPECT_EQ(3, result->get(9).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_position_has_null_element) {
    // array_position([NULL], "abc"): 0
    // array_position(["abc", NULL], "abc"): 1
    // array_position([NULL, "abc"], "abc"): 2
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum{}});
        array->append_datum(DatumArray{"abc", Datum{}});
        array->append_datum(DatumArray{Datum{}, "abc"});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum{"abc"});
        target->append_datum(Datum{"abc"});
        target->append_datum(Datum{"abc"});

        auto result = ArrayFunctions::array_position_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_EQ(1, result->get(1).get_int32());
        EXPECT_EQ(2, result->get(2).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_position_has_null_target) {
    // array_position(["abc", "def"], NULL): 0
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{"abc", "def"});

        // const-null column.
        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 0);

        auto result = ArrayFunctions::array_position_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
    }
    // array_position(ARRAY<TINYINT>[1, 2, 3], 2): 2
    // array_position(ARRAY<TINYINT>[1, 2, 3], 4): 0
    // array_position(ARRAY<TINYINT>[1, 2, 3], NULL): 0
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
        target->append_datum(Datum((int8_t)2));
        target->append_datum(Datum((int8_t)4));
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_position_specific<TYPE_TINYINT>(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(2, result->get(0).get_int32());
        EXPECT_EQ(0, result->get(1).get_int32());
        EXPECT_EQ(0, result->get(2).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_position_has_null_element_and_target) {
    // array_position([NULL], NULL): 1
    // array_position([NULL, "abc"], NULL): 1
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), "abc"});

        // const-null column.
        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 1);

        auto result = ArrayFunctions::array_position_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(2, result->size());
        EXPECT_EQ(1, result->get(0).get_int32());
        EXPECT_EQ(1, result->get(1).get_int32());
    }
    // array_position([NULL], NULL): 1
    // array_position([NULL, [1,2]], NULL): 1
    // array_position([NULL, [1,2]], [1,2]): 2
    // array_position([[1,2], NULL], [1,2]): 1
    // array_position([[1,2], NULL], NULL): 2
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), true);
        target->append_datum(Datum());
        target->append_datum(Datum());
        target->append_datum(DatumArray{1, 2});
        target->append_datum(DatumArray{1, 2});
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(5, result->size());
        EXPECT_EQ(1, result->get(0).get_int32());
        EXPECT_EQ(1, result->get(1).get_int32());
        EXPECT_EQ(2, result->get(2).get_int32());
        EXPECT_EQ(1, result->get(3).get_int32());
        EXPECT_EQ(2, result->get(4).get_int32());
    }
}

TEST_F(ArrayFunctionsTest, array_position_has_null_element_and_target_and_check_return_column_type) {
    // array_position([NULL], NULL): 1
    // array_position([NULL, "abc"], NULL): 1
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), "abc"});

        // const-null column.
        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 0);

        auto result = ArrayFunctions::array_position_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(2, result->size());
        EXPECT_EQ(1, result->get(0).get_int32());
        EXPECT_EQ(1, result->get(1).get_int32());
    }
    // array_position([NULL], NULL): 1
    // array_position([NULL, [1,2]], NULL): 1
    // array_position([NULL, [1,2]], [1,2]): 2
    // array_position([[1,2], NULL], [1,2]): 1
    // array_position([[1,2], NULL], NULL): 2
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), true);
        target->append_datum(Datum());
        target->append_datum(Datum());
        target->append_datum(DatumArray{1, 2});
        target->append_datum(DatumArray{1, 2});
        target->append_datum(Datum());

        auto result = ColumnHelper::cast_to<TYPE_INT>(
                ArrayFunctions::array_position_generic(nullptr, {array, target}).value());
        EXPECT_EQ(5, result->size());
        EXPECT_EQ(1, result->get(0).get_int32());
        EXPECT_EQ(1, result->get(1).get_int32());
        EXPECT_EQ(2, result->get(2).get_int32());
        EXPECT_EQ(1, result->get(3).get_int32());
        EXPECT_EQ(2, result->get(4).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_position_nullable_array) {
    // array_position(["a", "b"], "c"): 0
    // array_position(NULL, "c"): null
    // array_position(["a", "b", "c"], "c"): 3
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{"a", "b"});
        array->append_datum(Datum());
        array->append_datum(DatumArray{"a", "b", "c"});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum("c"));
        target->append_datum(Datum("c"));
        target->append_datum(Datum("c"));

        auto result = ArrayFunctions::array_position_specific<TYPE_VARCHAR>(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_EQ(3, result->get(2).get_int32());
    }
    // array_position([["a"], ["b"]], ["c"]): 0
    // array_position(NULL, ["c"]): null
    // array_position([["a", "b"], ["c"]], ["c"]): 2
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{DatumArray{"a"}, DatumArray{"b"}});
        array->append_datum(Datum());
        array->append_datum(DatumArray{DatumArray{"a", "b"}, DatumArray{"c"}});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_VARCHAR), false);
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});

        auto result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int32());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_EQ(2, result->get(2).get_int32());
    }
    // array_position(NULL, NULL): null
    // array_position(NULL, ["a"]): null
    // array_position(NULL, [NULL]): null
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(Datum());
        array->append_datum(Datum());
        array->append_datum(Datum());

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_VARCHAR), true);
        target->append_datum(Datum());
        target->append_datum(DatumArray{"a"});
        target->append_datum(DatumArray{Datum()});

        auto result = ArrayFunctions::array_position_generic(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->get(0).is_null());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_remove_empty_array) {
    // array_remove([], 1) -> []
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        target->append_datum(Datum{(int32_t)1});

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_array().size());
    }

    // array_remove([], "abc") -> []
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum{"abc"});

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_array().size());
    }

    // array_remove([[]], [1]) -> [[]]
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), false);
        target->append_datum(Datum(DatumArray{Datum{(int32_t)1}}));

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());

        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());
    }

    // array_remove([[]], []) -> []
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), false);
        target->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());

        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());
    }

    // array_remove([], 1) -> []
    // array_remove([], 1) -> []
    // array_remove([], 1) -> []
    // array_remove([], 1) -> []
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        DCHECK(target->is_constant());
        target->append_datum(Datum((int32_t)1));
        target->resize(4);

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());

        // 1st row: array_remove([], 1) -> []
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());

        // 2nd row: array_remove([], 1) -> []
        row = result->get(1).get_array();
        EXPECT_EQ(0, row.size());

        // 3rd row: array_remove([], 1) -> []
        row = result->get(2).get_array();
        EXPECT_EQ(0, row.size());

        // 4th row: array_remove([], 1) -> []
        row = result->get(3).get_array();
        EXPECT_EQ(0, row.size());
    }

    // array_remove([], 1) -> []
    // array_remove([], 2) -> []
    // array_remove([], NULL) -> []
    // array_remove([], 3) -> []
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        target->append_datum(Datum((int32_t)1));
        target->append_datum(Datum((int32_t)2));
        target->append_datum(Datum{});
        target->append_datum(Datum((int32_t)3));

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());

        // 1st row: array_remove([], 1) -> []
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());

        // 2nd row: array_remove([], 2) -> []
        row = result->get(1).get_array();
        EXPECT_EQ(0, row.size());

        // 3rd row: array_remove([], NULL) -> []
        row = result->get(2).get_array();
        EXPECT_EQ(0, row.size());

        // 4th row: array_remove([], 3) -> []
        row = result->get(3).get_array();
        EXPECT_EQ(0, row.size());
    }

    // array_remove([], NULL) -> []
    // array_remove([], NULL) -> []
    // array_remove([], NULL) -> []
    // array_remove([], NULL) -> []
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr target = ColumnHelper::create_const_null_column(1);
        target->resize(4);

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_array().size());
        EXPECT_EQ(0, result->get(1).get_array().size());
        EXPECT_EQ(0, result->get(2).get_array().size());
        EXPECT_EQ(0, result->get(3).get_array().size());

        array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_array().size());
        EXPECT_EQ(0, result->get(1).get_array().size());
        EXPECT_EQ(0, result->get(2).get_array().size());
        EXPECT_EQ(0, result->get(3).get_array().size());

        array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_EQ(0, result->get(0).get_array().size());
        EXPECT_EQ(0, result->get(1).get_array().size());
        EXPECT_EQ(0, result->get(2).get_array().size());
        EXPECT_EQ(0, result->get(3).get_array().size());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_remove_no_null) {
    // array_remove([], false)            -> []
    // array_remove([], true)             -> []
    // array_remove([false], false)       -> []
    // array_remove([false], true)        -> [false]
    // array_remove([true], false)        -> [true]
    // array_remove([true], true)         -> []
    // array_remove([true, false], false) -> [true]
    // array_remove([true, false], true)  -> [false]
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), false);
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(8, result->size());

        // 1st row: array_remove([], false) -> []
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());

        // 2nd row: array_remove([], true) -> []
        row = result->get(1).get_array();
        EXPECT_EQ(0, row.size());

        // 3rd row: array_remove([false], false) -> []
        row = result->get(2).get_array();
        EXPECT_EQ(0, row.size());

        // 4th row: array_remove([false], true) -> [false]
        row = result->get(3).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(0, row[0].get_int8());

        // 5th row: array_remove([true], false) -> [true]
        row = result->get(4).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(1, row[0].get_int8());

        // 6th row: array_remove([true], true) -> []
        row = result->get(5).get_array();
        EXPECT_EQ(0, row.size());

        // 7th row: array_remove([true, false], false) -> [true]
        row = result->get(6).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(1, row[0].get_int8());

        // 8th row: array_remove([true, false], true) -> [false]
        row = result->get(7).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(0, row[0].get_int8());
    }

    // array_remove([], 3) -> []
    // array_remove([2], 3) -> [2]
    // array_remove([1, 2, 3], 3) -> [1, 2]
    // array_remove([3, 2, 1], 3) -> [2, 1]
    // array_remove([2, 1, 3], 3) -> [2, 1]
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 0);
        target->append_datum(Datum{3});
        target->resize(5);

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(5, result->size());

        // 1st row: array_remove([], 3) -> []
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());

        // 2nd row: array_remove([2], 3) -> [2]
        row = result->get(1).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(2, row[0].get_int32());

        // 3rd row: array_remove([1, 2, 3], 3) -> [1, 2]
        row = result->get(2).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ(1, row[0].get_int32());
        EXPECT_EQ(2, row[1].get_int32());

        // 4th row: array_remove([3, 2, 1], 3) -> [2, 1]
        row = result->get(3).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ(2, row[0].get_int32());
        EXPECT_EQ(1, row[1].get_int32());

        // 5th row: array_remove([2, 1, 3], 3) -> [2, 1]
        row = result->get(4).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ(2, row[0].get_int32());
        EXPECT_EQ(1, row[1].get_int32());
    }

    // array_remove([], [])                                      -> []
    // array_remove([[]], [])                                    -> []
    // array_remove([["d", "o"], ["r"], ["i", "s"]], [])         -> [["d", "o"], ["r"], ["i", "s"]]
    // array_remove([["d", "o"], ["r"], ["i", "s"]], ["d])       -> [["d", "o"], ["r"], ["i", "s"]]
    // array_remove([["d", "o"], ["r"], ["i", "s"]], ["d", "o"]) -> [["r"], ["i", "s"]]
    // array_remove([["d", "o"], ["r"], ["i", "s"]], ["o", "d"]) -> [["d", "o"], ["r"], ["i", "s"]]
    // array_remove([["d", "o"], ["r"], ["i", "s"]], ["r"])      -> [["d", "o"], ["i", "s"]]
    // array_remove([["d", "o"], ["r"], ["i", "s"]], ["ri"])     -> [["d", "o"], ["r"], ["i", "s"]]
    // array_remove([["d", "o"], ["r"], ["i", "s"]], ["r", "i"]) -> [["d", "o"], ["r"], ["i", "s"]]
    // array_remove([["d", "o"], ["r"], ["i", "s"]], ["i", "s"]) -> [["d", "o"], ["r"]]
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum(DatumArray{})});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});

        ColumnPtr target = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(Datum(DatumArray{}));
        target->append_datum(DatumArray{"d"});
        target->append_datum(DatumArray{"d", "o"});
        target->append_datum(DatumArray{"o", "d"});
        target->append_datum(DatumArray{"r"});
        target->append_datum(DatumArray{"ri"});
        target->append_datum(DatumArray{"r", "i"});
        target->append_datum(DatumArray{"i", "s"});

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(10, result->size());

        // 1st row: array_remove([], []) -> []
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());

        // 2nd row: array_remove([[]], []) -> []
        row = result->get(1).get_array();
        EXPECT_EQ(0, row.size());

        // 3rd row: array_remove([["d", "o"], ["r"], ["i", "s"]], []) -> [["d", "o"], ["r"], ["i", "s"]]
        row = result->get(2).get_array();
        EXPECT_EQ(3, row.size());
        EXPECT_EQ(2, row[0].get_array().size()); // ["d", "o"]
        EXPECT_EQ("d", row[0].get_array()[0].get_slice());
        EXPECT_EQ("o", row[0].get_array()[1].get_slice());
        EXPECT_EQ(1, row[1].get_array().size()); //["r"]
        EXPECT_EQ("r", row[1].get_array()[0].get_slice());
        EXPECT_EQ(2, row[2].get_array().size()); // ["i", "s"]
        EXPECT_EQ("i", row[2].get_array()[0].get_slice());
        EXPECT_EQ("s", row[2].get_array()[1].get_slice());

        // 4th row: array_remove([["d", "o"], ["r"], ["i", "s"]], ["d]) -> [["d", "o"], ["r"], ["i", "s"]]
        row = result->get(3).get_array();
        EXPECT_EQ(3, row.size());
        EXPECT_EQ(2, row[0].get_array().size()); // ["d", "o"]
        EXPECT_EQ("d", row[0].get_array()[0].get_slice());
        EXPECT_EQ("o", row[0].get_array()[1].get_slice());
        EXPECT_EQ(1, row[1].get_array().size()); //["r"]
        EXPECT_EQ("r", row[1].get_array()[0].get_slice());
        EXPECT_EQ(2, row[2].get_array().size()); // ["i", "s"]
        EXPECT_EQ("i", row[2].get_array()[0].get_slice());
        EXPECT_EQ("s", row[2].get_array()[1].get_slice());

        // 5th row: array_remove([["d", "o"], ["r"], ["i", "s"]], ["d", "o"]) -> [["r"], ["i", "s"]]
        row = result->get(4).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ(1, row[0].get_array().size()); //["r"]
        EXPECT_EQ("r", row[0].get_array()[0].get_slice());
        EXPECT_EQ(2, row[1].get_array().size()); // ["i", "s"]
        EXPECT_EQ("i", row[1].get_array()[0].get_slice());
        EXPECT_EQ("s", row[1].get_array()[1].get_slice());

        // 6th row: array_remove([["d", "o"], ["r"], ["i", "s"]], ["o", "d"]) -> [["d", "o"], ["r"], ["i", "s"]]
        row = result->get(5).get_array();
        EXPECT_EQ(3, row.size());
        EXPECT_EQ(2, row[0].get_array().size()); // ["d", "o"]
        EXPECT_EQ("d", row[0].get_array()[0].get_slice());
        EXPECT_EQ("o", row[0].get_array()[1].get_slice());
        EXPECT_EQ(1, row[1].get_array().size()); // ["r"]
        EXPECT_EQ("r", row[1].get_array()[0].get_slice());
        EXPECT_EQ(2, row[2].get_array().size()); // ["i", "s"]
        EXPECT_EQ("i", row[2].get_array()[0].get_slice());
        EXPECT_EQ("s", row[2].get_array()[1].get_slice());

        // 7th row: array_remove([["d", "o"], ["r"], ["i", "s"]], ["r"]) -> [["d", "o"], ["i", "s"]]
        row = result->get(6).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ(2, row[0].get_array().size()); // ["d", "o"]
        EXPECT_EQ("d", row[0].get_array()[0].get_slice());
        EXPECT_EQ("o", row[0].get_array()[1].get_slice());
        EXPECT_EQ(2, row[1].get_array().size()); // ["i", "s"]
        EXPECT_EQ("i", row[1].get_array()[0].get_slice());
        EXPECT_EQ("s", row[1].get_array()[1].get_slice());

        // 8th row: array_remove([["d", "o"], ["r"], ["i", "s"]], ["ri"]) -> [["d", "o"], ["r"], ["i", "s"]]
        row = result->get(7).get_array();
        EXPECT_EQ(3, row.size());
        EXPECT_EQ(2, row[0].get_array().size()); // ["d", "o"]
        EXPECT_EQ("d", row[0].get_array()[0].get_slice());
        EXPECT_EQ("o", row[0].get_array()[1].get_slice());
        EXPECT_EQ(1, row[1].get_array().size()); //["r"]
        EXPECT_EQ("r", row[1].get_array()[0].get_slice());
        EXPECT_EQ(2, row[2].get_array().size()); // ["i", "s"]
        EXPECT_EQ("i", row[2].get_array()[0].get_slice());
        EXPECT_EQ("s", row[2].get_array()[1].get_slice());

        // 9th row: array_remove([["d", "o"], ["r"], ["i", "s"]], ["r", "i"]) -> [["d", "o"], ["r"], ["i", "s"]]
        row = result->get(8).get_array();
        EXPECT_EQ(3, row.size());
        EXPECT_EQ(2, row[0].get_array().size()); // ["d", "o"]
        EXPECT_EQ("d", row[0].get_array()[0].get_slice());
        EXPECT_EQ("o", row[0].get_array()[1].get_slice());
        EXPECT_EQ(1, row[1].get_array().size()); //["r"]
        EXPECT_EQ("r", row[1].get_array()[0].get_slice());
        EXPECT_EQ(2, row[2].get_array().size()); // ["i", "s"]
        EXPECT_EQ("i", row[2].get_array()[0].get_slice());
        EXPECT_EQ("s", row[2].get_array()[1].get_slice());

        // 10th row: array_remove([["d", "o"], ["r"], ["i", "s"]], ["i", "s"]) -> [["d", "o"], ["r"]]
        row = result->get(9).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ(2, row[0].get_array().size()); // ["d", "o"]
        EXPECT_EQ("d", row[0].get_array()[0].get_slice());
        EXPECT_EQ("o", row[0].get_array()[1].get_slice());
        EXPECT_EQ(1, row[1].get_array().size()); //["r"]
        EXPECT_EQ("r", row[1].get_array()[0].get_slice());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_remove_has_null_element) {
    // array_remove([NULL], "abc")        -> [NULL]
    // array_remove(["abc", NULL], "abc") -> [NULL]
    // array_remove([NULL, "abc"], "abc") -> [NULL]
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum{}});
        array->append_datum(DatumArray{"abc", Datum{}});
        array->append_datum(DatumArray{Datum{}, "abc"});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum{"abc"});
        target->append_datum(Datum{"abc"});
        target->append_datum(Datum{"abc"});

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());

        // 1st row: array_remove([NULL], "abc") -> [NULL]
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(1, row.size());

        // 2nd row: array_remove(["abc", NULL], "abc") -> [NULL]
        row = result->get(1).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_TRUE(row[0].is_null());

        // 3rd row: array_remove([NULL, "abc"], "abc") -> [NULL]
        row = result->get(2).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_TRUE(row[0].is_null());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_remove_has_null_target) {
    {
        // array_remove(["abc", "def"], NULL) -> ["abc", "def"]
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{"abc", "def"});

        // const-null column.
        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 0);
        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(1, result->size());

        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ("abc", row[0].get_slice());
        EXPECT_EQ("def", row[1].get_slice());
    }

    // array_remove([1, 2, 3], 2) -> [1, 3]
    // array_remove([1, 2, 3], 4) -> [1, 2, 3]
    // array_remove([1, 2, 3], NULL) -> [1, 2, 3]
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
        target->append_datum(Datum((int8_t)2));
        target->append_datum(Datum((int8_t)4));
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());

        // 1st row: array_remove([1, 2, 3], 2) -> [1, 3]
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ(1, row[0].get_int8());
        EXPECT_EQ(3, row[1].get_int8());

        // 2nd row: array_remove([1, 2, 3], 4) -> [1, 2, 3]
        row = result->get(1).get_array();
        EXPECT_EQ(3, row.size());
        EXPECT_EQ(1, row[0].get_int8());
        EXPECT_EQ(2, row[1].get_int8());
        EXPECT_EQ(3, row[2].get_int8());

        // 3rd row: array_remove([1, 2, 3], NULL) -> [1, 2, 3]
        row = result->get(2).get_array();
        EXPECT_EQ(3, row.size());
        EXPECT_EQ(1, row[0].get_int8());
        EXPECT_EQ(2, row[1].get_int8());
        EXPECT_EQ(3, row[2].get_int8());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_remove_has_null_element_and_target) {
    // array_remove([NULL], NULL)  -> []
    // array_remove([NULL, "abc"], NULL) -> ["abc"]
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), "abc"});

        // const-null column.
        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 0);

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(2, result->size());

        // 1st row: array_remove([NULL], NULL) -> []
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());

        // 2nd row: array_remove([NULL, "abc"], NULL) -> ["abc"]
        row = result->get(1).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ("abc", row[0].get_slice());
    }

    // array_remove([NULL], NULL)           -> []
    // array_remove([NULL, [1, 2]], NULL)   -> [[1 ,2]]
    // array_remove([NULL, [1, 2]], [1, 2]) -> [NULL]
    // array_remove([[1, 2], NULL], [1, 2]) -> [NULL]
    // array_remove([NULL, [1, 2]], NULL)   -> [[1, 2]]
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), true);
        target->append_datum(Datum());
        target->append_datum(Datum());
        target->append_datum(DatumArray{1, 2});
        target->append_datum(DatumArray{1, 2});
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(5, result->size());

        // 1st row: array_remove([NULL], NULL) -> []
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(0, row.size());

        // 2nd row: array_remove([NULL, [1, 2]], NULL)   -> [[1 ,2]]
        row = result->get(1).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(2, row[0].get_array().size());
        EXPECT_EQ(1, row[0].get_array()[0].get_int32());
        EXPECT_EQ(2, row[0].get_array()[1].get_int32());

        // 3rd row: array_remove([NULL, [1, 2]], [1, 2]) -> [NULL]
        row = result->get(2).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_TRUE(row[0].is_null());

        // 4th row: array_remove([[1, 2], NULL], [1, 2]) -> [NULL]
        row = result->get(3).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_TRUE(row[0].is_null());

        // 5th row: array_remove([NULL, [1, 2]], NULL)   -> [[1, 2]]
        row = result->get(4).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(2, row[0].get_array().size());
        EXPECT_EQ(1, row[0].get_array()[0].get_int32());
        EXPECT_EQ(2, row[0].get_array()[1].get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_remove_nullable_array) {
    {
        // array_remove(["a", "b"], "c")      -> ["a", "b"]
        // array_remove(NULL, "c")            -> NULL
        // array_remove(["a", "b", "c"], "c") -> ["a", "b"]
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{"a", "b"});
        array->append_datum(Datum());
        array->append_datum(DatumArray{"a", "b", "c"});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        target->append_datum(Datum("c"));
        target->append_datum(Datum("c"));
        target->append_datum(Datum("c"));

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());

        // 1st row: array_remove(["a", "b"], "c")      -> ["a", "b"]
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ("a", row[0].get_slice());
        EXPECT_EQ("b", row[1].get_slice());

        // 2nd row: array_remove(NULL, "c") -> NULL
        EXPECT_TRUE(result->get(1).is_null());

        // 3rd row: array_remove(["a", "b", "c"], "c") -> ["a", "b"]
        row = result->get(2).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ("a", row[0].get_slice());
        EXPECT_EQ("b", row[1].get_slice());
    }

    // array_remove([["a"], ["b"]], ["c"]) -> [["a"], ["b"]]
    // array_remove(NULL, ["c"])           -> NULL
    // array_remove([["a", "b"], ["c"]])   -> [["a", "b"]]
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{DatumArray{"a"}, DatumArray{"b"}});
        array->append_datum(Datum());
        array->append_datum(DatumArray{DatumArray{"a", "b"}, DatumArray{"c"}});

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_VARCHAR), false);
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());

        // 1st row: array_remove([["a"], ["b"]], ["c"]) -> [["a"], ["b"]]
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ("a", row[0].get_array()[0].get_slice());
        EXPECT_EQ("b", row[1].get_array()[0].get_slice());

        // 2nd row: array_remove(NULL, ["c"]) -> NULL
        EXPECT_TRUE(result->get(1).is_null());

        // 3rd row: array_remove([["a", "b"], ["c"]])   -> [["a", "b"]]
        row = result->get(0).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_EQ("a", row[0].get_array()[0].get_slice());
        EXPECT_EQ("b", row[1].get_array()[0].get_slice());
    }

    // array_remove(NULL, NULL)  -> NULL
    // array_remove(NULL, ["a"]) -> NULL
    // array_remove(NULL, [])    -> NULL
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(Datum());
        array->append_datum(Datum());
        array->append_datum(Datum());

        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_VARCHAR), true);
        target->append_datum(Datum());
        target->append_datum(DatumArray{"a"});
        target->append_datum(DatumArray{Datum()});

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());

        EXPECT_TRUE(result->get(0).is_null());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
    }
    // array_remove(NULL,1)
    {
        ColumnPtr array = ColumnHelper::create_const_null_column(3);
        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
        target->append_datum(Datum((int8_t)2));
        target->append_datum(Datum((int8_t)4));
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_remove(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());

        EXPECT_TRUE(result->get(0).is_null());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_append) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));

        ColumnPtr null = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 0);

        auto result = ArrayFunctions::array_append(nullptr, {array, null}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(1, result->get(0).get_array().size());
        EXPECT_TRUE(result->get(0).get_array()[0].is_null());
    }
    // array_append(['abc'], 'def')
    // array_append(['xyz', 'xxx'], 'def')
    // array_append([], 'def')
    // array_append(NULL, 'def')
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{"abc"});
        array->append_datum(DatumArray{"xyz", "xxx"});
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum());

        ColumnPtr data = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 0);
        data->append_datum("def");

        auto result = ArrayFunctions::array_append(nullptr, {array, data}).value();
        EXPECT_EQ(4, result->size());
        // First row.
        EXPECT_EQ(2, result->get(0).get_array().size());
        EXPECT_EQ("abc", result->get(0).get_array()[0].get_slice());
        EXPECT_EQ("def", result->get(0).get_array()[1].get_slice());
        // Second row.
        EXPECT_EQ(3, result->get(1).get_array().size());
        EXPECT_EQ("xyz", result->get(1).get_array()[0].get_slice());
        EXPECT_EQ("xxx", result->get(1).get_array()[1].get_slice());
        EXPECT_EQ("def", result->get(1).get_array()[2].get_slice());
        // Third row.
        EXPECT_EQ(1, result->get(2).get_array().size());
        EXPECT_EQ("def", result->get(2).get_array()[0].get_slice());
        // Last row.
        EXPECT_TRUE(result->get(3).is_null());
    }
    // array_append([], [])                       -> [[]]
    // array_append([[0,1], [2])                  -> [[0,1], [2]]
    // array_append([NULL], [3,4])                -> [NULL, [3,4]]
    // array_append(NULL, NULL)                   -> NULL
    // array_append([[10, 11],[12,13]], [14,15])  -> [[10,11],[12,13],[14,15]]
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, true);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum(DatumArray{0, 1})});
        array->append_datum(DatumArray{Datum()});
        array->append_datum(Datum());
        array->append_datum(DatumArray{Datum(DatumArray{10, 11}), Datum(DatumArray{12, 13})});

        ColumnPtr data = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        data->append_datum(Datum(DatumArray{}));
        data->append_datum(DatumArray{2});
        data->append_datum(DatumArray{3, 4});
        data->append_datum(Datum());
        data->append_datum(DatumArray{14, 15});

        auto result = ArrayFunctions::array_append(nullptr, {array, data}).value();
        EXPECT_EQ(5, result->size());
        // 1st row.
        DatumArray row = result->get(0).get_array();
        EXPECT_EQ(1, row.size());
        EXPECT_EQ(0, row[0].get_array().size());
        // 2nd row
        row = result->get(1).get_array();
        EXPECT_EQ(2, row.size());

        EXPECT_EQ(2, row[0].get_array().size());
        EXPECT_EQ(1, row[1].get_array().size());

        EXPECT_EQ(0, row[0].get_array()[0].get_int32());
        EXPECT_EQ(1, row[0].get_array()[1].get_int32());
        EXPECT_EQ(2, row[1].get_array()[0].get_int32());
        // 3rd row
        row = result->get(2).get_array();
        EXPECT_EQ(2, row.size());
        EXPECT_TRUE(row[0].is_null());
        EXPECT_EQ(2, row[1].get_array().size());
        EXPECT_EQ(3, row[1].get_array()[0].get_int32());
        EXPECT_EQ(4, row[1].get_array()[1].get_int32());
        // 4th row
        EXPECT_TRUE(result->get(3).is_null());
        // 5th row
        row = result->get(4).get_array();
        EXPECT_EQ(3, row.size());

        EXPECT_EQ(2, row[0].get_array().size());
        EXPECT_EQ(2, row[1].get_array().size());
        EXPECT_EQ(2, row[2].get_array().size());

        EXPECT_EQ(10, row[0].get_array()[0].get_int32());
        EXPECT_EQ(11, row[0].get_array()[1].get_int32());
        EXPECT_EQ(12, row[1].get_array()[0].get_int32());
        EXPECT_EQ(13, row[1].get_array()[1].get_int32());
        EXPECT_EQ(14, row[2].get_array()[0].get_int32());
        EXPECT_EQ(15, row[2].get_array()[1].get_int32());
    }

    // array_append(NULL,1)
    {
        ColumnPtr array = ColumnHelper::create_const_null_column(3);
        ColumnPtr target = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
        target->append_datum(Datum((int8_t)2));
        target->append_datum(Datum((int8_t)4));
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_append(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());

        EXPECT_TRUE(result->get(0).is_null());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
    }
}

TEST_F(ArrayFunctionsTest, array_sum_empty_array) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum<TYPE_TINYINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));

        array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        result = ArrayFunctions::array_sum<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

TEST_F(ArrayFunctionsTest, array_avg_empty_array) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg<TYPE_TINYINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));

        array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        result = ArrayFunctions::array_avg<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

TEST_F(ArrayFunctionsTest, array_min_empty_array) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min<TYPE_TINYINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));

        array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        result = ArrayFunctions::array_min<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

TEST_F(ArrayFunctionsTest, array_max_empty_array) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max<TYPE_TINYINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));

        array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        result = ArrayFunctions::array_max<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

TEST_F(ArrayFunctionsTest, array_sum_no_null) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});

        auto result = ArrayFunctions::array_sum<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(8, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_EQ(0, result->get(2).get_int64());
        EXPECT_EQ(0, result->get(3).get_int64());
        EXPECT_EQ(1, result->get(4).get_int64());
        EXPECT_EQ(1, result->get(5).get_int64());
        EXPECT_EQ(1, result->get(6).get_int64());
        EXPECT_EQ(1, result->get(7).get_int64());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});
        array->append_datum(DatumArray{1, 2, 3, Datum()});

        auto result = ArrayFunctions::array_sum<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2, result->get(1).get_int64());
        EXPECT_EQ(6, result->get(2).get_int64());
        EXPECT_EQ(6, result->get(3).get_int64());
        EXPECT_EQ(6, result->get(4).get_int64());
        EXPECT_EQ(6, result->get(5).get_int64());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t)127, (int8_t)100, (int8_t)-1});
        array->append_datum(DatumArray{(int8_t)-128, (int8_t)-1, (int8_t)10});

        auto result = ArrayFunctions::array_sum<TYPE_TINYINT>(nullptr, {array}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(226, result->get(1).get_int64());
        EXPECT_EQ(-119, result->get(2).get_int64());
    }
}

TEST_F(ArrayFunctionsTest, array_avg_no_null) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});

        auto result = ArrayFunctions::array_avg<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(8, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_EQ(0, result->get(2).get_double());
        EXPECT_EQ(0, result->get(3).get_double());
        EXPECT_EQ(1, result->get(4).get_double());
        EXPECT_EQ(1, result->get(5).get_double());
        EXPECT_EQ(0.5, result->get(6).get_double());
        EXPECT_EQ(0.5, result->get(7).get_double());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});
        array->append_datum(DatumArray{1, 2, 3, Datum()});

        auto result = ArrayFunctions::array_avg<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2, result->get(1).get_double());
        EXPECT_EQ(2, result->get(2).get_double());
        EXPECT_EQ(2, result->get(3).get_double());
        EXPECT_EQ(2, result->get(4).get_double());
        EXPECT_EQ(1.5, result->get(5).get_double());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) true, Datum(), Datum(), (int8_t) false});
        array->append_datum(DatumArray{(int8_t) false, Datum()});
        array->append_datum(DatumArray{(int8_t) true, Datum()});

        auto result = ArrayFunctions::array_avg<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(0.25, result->get(1).get_double());
        EXPECT_EQ(0, result->get(2).get_double());
        EXPECT_EQ(0.5, result->get(3).get_double());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t)-128, (int8_t)127, (int8_t)0, Datum()});
        array->append_datum(DatumArray{(int8_t)127, (int8_t)10, (int8_t)100});

        auto result = ArrayFunctions::array_avg<TYPE_TINYINT>(nullptr, {array}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(-0.25, result->get(1).get_double());
        EXPECT_EQ(79, result->get(2).get_double());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_SMALLINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int16_t)30000, (int16_t)30000, Datum()});
        array->append_datum(DatumArray{(int16_t)-32768, (int16_t)32767, Datum(), (int16_t)0, (int16_t)1});

        auto result = ArrayFunctions::array_avg<TYPE_SMALLINT>(nullptr, {array}).value();
        EXPECT_EQ(3, result->size());

        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(20000, result->get(1).get_double());
        EXPECT_EQ(0, result->get(2).get_double());
    }
}

TEST_F(ArrayFunctionsTest, array_min_no_null) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});

        auto result = ArrayFunctions::array_min<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(8, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());
        EXPECT_EQ(1, result->get(4).get_int8());
        EXPECT_EQ(1, result->get(5).get_int8());
        EXPECT_EQ(0, result->get(6).get_int8());
        EXPECT_EQ(0, result->get(7).get_int8());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});

        auto result = ArrayFunctions::array_min<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(5, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2, result->get(1).get_int32());
        EXPECT_EQ(1, result->get(2).get_int32());
        EXPECT_EQ(1, result->get(3).get_int32());
        EXPECT_EQ(1, result->get(4).get_int32());
    }
}

TEST_F(ArrayFunctionsTest, array_max_no_null) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});

        auto result = ArrayFunctions::array_max<TYPE_BOOLEAN>(nullptr, {array}).value();
        EXPECT_EQ(8, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_EQ(0, result->get(2).get_int8());
        EXPECT_EQ(0, result->get(3).get_int8());
        EXPECT_EQ(1, result->get(4).get_int8());
        EXPECT_EQ(1, result->get(5).get_int8());
        EXPECT_EQ(1, result->get(6).get_int8());
        EXPECT_EQ(1, result->get(7).get_int8());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});

        auto result = ArrayFunctions::array_max<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(5, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2, result->get(1).get_int32());
        EXPECT_EQ(3, result->get(2).get_int32());
        EXPECT_EQ(3, result->get(3).get_int32());
        EXPECT_EQ(3, result->get(4).get_int32());
    }
}

TEST_F(ArrayFunctionsTest, array_sum_has_null_element) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int64_t)2000});
        array->append_datum(DatumArray{(int64_t)1000, (int64_t)2, (int64_t)3});
        array->append_datum(DatumArray{(int64_t)3, (int64_t)2, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)200000000, (int64_t)121, (int64_t)300});
        array->append_datum(DatumArray{(int64_t)33, Datum(), (int64_t)300});

        auto result = ArrayFunctions::array_sum<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int64());
        EXPECT_EQ(1005, result->get(2).get_int64());
        EXPECT_EQ(6, result->get(3).get_int64());
        EXPECT_EQ(200000421, result->get(4).get_int64());
        EXPECT_EQ(333, result->get(5).get_int64());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_LARGEINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int128_t)2000});
        array->append_datum(DatumArray{(int128_t)1000, (int128_t)2, (int128_t)3});
        array->append_datum(DatumArray{(int128_t)3, (int128_t)2, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)200000000, (int128_t)121, (int128_t)300});
        array->append_datum(DatumArray{(int128_t)33, Datum(), (int128_t)300});

        auto result = ArrayFunctions::array_sum<TYPE_LARGEINT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int128());
        EXPECT_EQ(1005, result->get(2).get_int128());
        EXPECT_EQ(6, result->get(3).get_int128());
        EXPECT_EQ(200000421, result->get(4).get_int128());
        EXPECT_EQ(333, result->get(5).get_int128());
    }
}

TEST_F(ArrayFunctionsTest, array_avg_has_null_element) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int64_t)2000});
        array->append_datum(DatumArray{(int64_t)1000, (int64_t)2, (int64_t)3});
        array->append_datum(DatumArray{(int64_t)3, (int64_t)2, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)1, (int64_t)1, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)2, Datum(), (int64_t)1});

        auto result = ArrayFunctions::array_avg<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_double());
        EXPECT_EQ(335, result->get(2).get_double());
        EXPECT_EQ(2, result->get(3).get_double());
        EXPECT_EQ(1, result->get(4).get_double());
        EXPECT_EQ(1, result->get(5).get_double());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_LARGEINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int128_t)2000});
        array->append_datum(DatumArray{(int128_t)1000, (int128_t)2, (int128_t)3});
        array->append_datum(DatumArray{(int128_t)3, (int128_t)2, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)1, (int128_t)1, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)2, Datum(), (int128_t)1});

        auto result = ArrayFunctions::array_avg<TYPE_LARGEINT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_double());
        EXPECT_EQ(335, result->get(2).get_double());
        EXPECT_EQ(2, result->get(3).get_double());
        EXPECT_EQ(1, result->get(4).get_double());
        EXPECT_EQ(1, result->get(5).get_double());
    }
}

TEST_F(ArrayFunctionsTest, array_min_has_null_element) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int64_t)2000});
        array->append_datum(DatumArray{(int64_t)1000, (int64_t)2, (int64_t)3});
        array->append_datum(DatumArray{(int64_t)3, (int64_t)2, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)1, (int64_t)1, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)1, Datum(), (int64_t)1});

        auto result = ArrayFunctions::array_min<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int64());
        EXPECT_EQ(2, result->get(2).get_int64());
        EXPECT_EQ(1, result->get(3).get_int64());
        EXPECT_EQ(1, result->get(4).get_int64());
        EXPECT_EQ(1, result->get(5).get_int64());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_LARGEINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int128_t)2000});
        array->append_datum(DatumArray{(int128_t)1000, (int128_t)2, (int128_t)3});
        array->append_datum(DatumArray{(int128_t)3, (int128_t)2, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)1, (int128_t)1, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)1, Datum(), (int128_t)1});

        auto result = ArrayFunctions::array_min<TYPE_LARGEINT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int128());
        EXPECT_EQ(2, result->get(2).get_int128());
        EXPECT_EQ(1, result->get(3).get_int128());
        EXPECT_EQ(1, result->get(4).get_int128());
        EXPECT_EQ(1, result->get(5).get_int128());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_DATE, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 24)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 26)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 28)});
        array->append_datum(DatumArray{Datum(), DateValue::create(1990, 3, 28)});

        auto result = ArrayFunctions::array_min<TYPE_DATE>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(1).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(2).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(3).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(4).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 28), result->get(5).get_date());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_DATETIME, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 34)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 36)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 38)});
        array->append_datum(DatumArray{Datum(), TimestampValue::create(1990, 3, 22, 5, 32, 38)});

        auto result = ArrayFunctions::array_min<TYPE_DATETIME>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(1).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(2).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(3).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(4).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 38), result->get(5).get_timestamp());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{"varchar"});
        array->append_datum(DatumArray{"varchar1", "varchar2"});
        array->append_datum(DatumArray{"varchar1", "varchar3"});
        array->append_datum(DatumArray{"varchar1", "varchar4"});
        array->append_datum(DatumArray{Datum(), "varchar4"});

        auto result = ArrayFunctions::array_min<TYPE_VARCHAR>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ("varchar", result->get(1).get_slice());
        EXPECT_EQ("varchar1", result->get(2).get_slice());
        EXPECT_EQ("varchar1", result->get(3).get_slice());
        EXPECT_EQ("varchar1", result->get(4).get_slice());
        EXPECT_EQ("varchar4", result->get(5).get_slice());
    }
}

TEST_F(ArrayFunctionsTest, array_max_has_null_element) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int64_t)2000});
        array->append_datum(DatumArray{(int64_t)1000, (int64_t)2, (int64_t)3});
        array->append_datum(DatumArray{(int64_t)3, (int64_t)2, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)1, (int64_t)1, (int64_t)1});

        auto result = ArrayFunctions::array_max<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(5, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int64());
        EXPECT_EQ(1000, result->get(2).get_int64());
        EXPECT_EQ(3, result->get(3).get_int64());
        EXPECT_EQ(1, result->get(4).get_int64());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_LARGEINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int128_t)2000});
        array->append_datum(DatumArray{(int128_t)1000, (int128_t)2, (int128_t)3});
        array->append_datum(DatumArray{(int128_t)3, (int128_t)2, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)1, (int128_t)1, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)2, (int128_t)1, Datum()});

        auto result = ArrayFunctions::array_max<TYPE_LARGEINT>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int128());
        EXPECT_EQ(1000, result->get(2).get_int128());
        EXPECT_EQ(3, result->get(3).get_int128());
        EXPECT_EQ(1, result->get(4).get_int128());
        EXPECT_EQ(2, result->get(5).get_int128());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_DATE, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 24)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 26)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 28)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), Datum()});

        auto result = ArrayFunctions::array_max<TYPE_DATE>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(1).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 24), result->get(2).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 26), result->get(3).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 28), result->get(4).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(5).get_date());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_DATETIME, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 34)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 36)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 38)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32), Datum()});

        auto result = ArrayFunctions::array_max<TYPE_DATETIME>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(1).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 34), result->get(2).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 36), result->get(3).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 38), result->get(4).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(5).get_timestamp());
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{"varchar"});
        array->append_datum(DatumArray{"varchar1", "varchar2"});
        array->append_datum(DatumArray{"varchar1", "varchar3"});
        array->append_datum(DatumArray{"varchar1", "varchar4"});
        array->append_datum(DatumArray{"varchar1", Datum()});

        auto result = ArrayFunctions::array_max<TYPE_VARCHAR>(nullptr, {array}).value();
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ("varchar", result->get(1).get_slice());
        EXPECT_EQ("varchar2", result->get(2).get_slice());
        EXPECT_EQ("varchar3", result->get(3).get_slice());
        EXPECT_EQ("varchar4", result->get(4).get_slice());
        EXPECT_EQ("varchar1", result->get(5).get_slice());
    }
}

TEST_F(ArrayFunctionsTest, array_sum_nullable_array) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        array->append_datum(DatumArray{3, 5});
        array->append_datum(DatumArray{Datum(), 54});
        array->append_datum(DatumArray{5352, 121, 30});

        auto result = ArrayFunctions::array_sum<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(8, result->get(0).get_int64());
        EXPECT_EQ(54, result->get(1).get_int64());
        EXPECT_EQ(5503, result->get(2).get_int64());
    }
}

TEST_F(ArrayFunctionsTest, array_avg_nullable_array) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        array->append_datum(DatumArray{3, 5});
        array->append_datum(DatumArray{Datum(), 54});
        array->append_datum(DatumArray{5352, 121, 32});

        auto result = ArrayFunctions::array_avg<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(4, result->get(0).get_double());
        EXPECT_EQ(27, result->get(1).get_double());
        EXPECT_EQ(1835, result->get(2).get_double());
    }
}

TEST_F(ArrayFunctionsTest, array_min_nullable_array) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        array->append_datum(DatumArray{3, 5});
        array->append_datum(DatumArray{Datum(), 54});
        array->append_datum(DatumArray{5352, 121, 32});

        auto result = ArrayFunctions::array_min<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(3, result->get(0).get_int32());
        EXPECT_EQ(54, result->get(1).get_int32());
        EXPECT_EQ(32, result->get(2).get_int32());
    }
}

TEST_F(ArrayFunctionsTest, array_max_nullable_array) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        array->append_datum(DatumArray{3, 5});
        array->append_datum(DatumArray{Datum(), 54});
        array->append_datum(DatumArray{5352, 121, 32});

        auto result = ArrayFunctions::array_max<TYPE_INT>(nullptr, {array}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(5, result->get(0).get_int32());
        EXPECT_EQ(54, result->get(1).get_int32());
        EXPECT_EQ(5352, result->get(2).get_int32());
    }
}

TEST_F(ArrayFunctionsTest, array_all_null) {
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_sum<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_avg<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_min<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_min<TYPE_VARCHAR>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_max<TYPE_BIGINT>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_max<TYPE_VARCHAR>(nullptr, {array}).value();
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

TEST_F(ArrayFunctionsTest, array_reverse_int) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, 3, 6});
    src_column->append_datum(DatumArray{2, 3, 7, 8});
    src_column->append_datum(DatumArray{4, 3, 2, 1});

    ArrayReverse<LogicalType::TYPE_INT> reverse;
    auto dest_column = reverse.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<int32_t>({6, 3, 5}, dest_column->get(0).get_array());
    _check_array<int32_t>({8, 7, 3, 2}, dest_column->get(1).get_array());
    _check_array<int32_t>({1, 2, 3, 4}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_string) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"352", "66", "4325"});
    src_column->append_datum(DatumArray{"235", "99", "8", "43251"});
    src_column->append_datum(DatumArray{"44", "33", "22", "112"});

    ArrayReverse<LogicalType::TYPE_VARCHAR> reverse;
    auto dest_column = reverse.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<Slice>({"4325", "66", "352"}, dest_column->get(0).get_array());
    _check_array<Slice>({"43251", "8", "99", "235"}, dest_column->get(1).get_array());
    _check_array<Slice>({"112", "22", "33", "44"}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_nullable_elements) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, Datum(), 3, 6});
    src_column->append_datum(DatumArray{2, 3, Datum(), Datum()});
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    ArrayReverse<LogicalType::TYPE_INT> reverse;
    auto dest_column = reverse.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array_nullable<int32_t>({6, 3, 0, 5}, {0, 0, 1, 0}, dest_column->get(0).get_array());
    _check_array_nullable<int32_t>({0, 0, 3, 2}, {1, 1, 0, 0}, dest_column->get(1).get_array());
    _check_array_nullable<int32_t>({0, 0, 0, 0}, {1, 1, 1, 1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_nullable_array) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, Datum(), 3, 6});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    ArrayReverse<LogicalType::TYPE_INT> reverse;
    auto dest_column = reverse.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array_nullable<int32_t>({6, 3, 0, 5}, {0, 0, 1, 0}, dest_column->get(0).get_array());
    ASSERT_TRUE(dest_column->get(1).is_null());
    _check_array_nullable<int32_t>({0, 0, 0, 0}, {1, 1, 1, 1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_only_null) {
    ColumnPtr src_column = ColumnHelper::create_const_null_column(3);

    ArrayReverse<LogicalType::TYPE_INT> reverse;
    auto dest_column = reverse.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_TRUE(dest_column->get(0).is_null());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_TRUE(dest_column->get(2).is_null());
}

TEST_F(ArrayFunctionsTest, array_difference_boolean) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column->append_datum(DatumArray{(uint8_t)5, (uint8_t)3, (uint8_t)6});
    src_column->append_datum(DatumArray{(uint8_t)2, (uint8_t)3, (uint8_t)7, (uint8_t)8});
    src_column->append_datum(DatumArray{(uint8_t)4, (uint8_t)3, (uint8_t)2, (uint8_t)1});

    ArrayDifference<LogicalType::TYPE_BOOLEAN> difference;
    auto dest_column = difference.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<int64_t>({0, -2, 3}, dest_column->get(0).get_array());
    _check_array<int64_t>({0, 1, 4, 1}, dest_column->get(1).get_array());
    _check_array<int64_t>({0, -1, -1, -1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_difference_boolean_with_entry_null) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column->append_datum(DatumArray{(uint8_t)5, Datum(), (uint8_t)6});
    src_column->append_datum(DatumArray{Datum(), (uint8_t)3, (uint8_t)7, (uint8_t)8});
    src_column->append_datum(DatumArray{(uint8_t)4, (uint8_t)3, (uint8_t)2, Datum()});
    src_column->append_datum(Datum());

    ArrayDifference<LogicalType::TYPE_BOOLEAN> difference;
    auto dest_column = difference.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 4);

    ASSERT_EQ(0, dest_column->get(0).get_array()[0].get_int64());
    ASSERT_TRUE(dest_column->get(0).get_array()[1].is_null());
    ASSERT_TRUE(dest_column->get(0).get_array()[2].is_null());

    ASSERT_TRUE(dest_column->get(1).get_array()[0].is_null());
    ASSERT_TRUE(dest_column->get(1).get_array()[1].is_null());
    ASSERT_EQ(4, dest_column->get(1).get_array()[2].get_int64());
    ASSERT_EQ(1, dest_column->get(1).get_array()[3].get_int64());

    ASSERT_EQ(0, dest_column->get(2).get_array()[0].get_int64());
    ASSERT_EQ(-1, dest_column->get(2).get_array()[1].get_int64());
    ASSERT_EQ(-1, dest_column->get(2).get_array()[2].get_int64());
    ASSERT_TRUE(dest_column->get(2).get_array()[3].is_null());
    ASSERT_TRUE(dest_column->get(3).is_null());
}

TEST_F(ArrayFunctionsTest, array_difference_int) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, 3, 6});
    src_column->append_datum(DatumArray{2, 3, 7, 8});
    src_column->append_datum(DatumArray{4, 3, 2, 1});

    ArrayDifference<LogicalType::TYPE_INT> difference;
    auto dest_column = difference.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<int64_t>({0, -2, 3}, dest_column->get(0).get_array());
    _check_array<int64_t>({0, 1, 4, 1}, dest_column->get(1).get_array());
    _check_array<int64_t>({0, -1, -1, -1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_difference_int_with_entry_null) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, Datum(), 6});
    src_column->append_datum(DatumArray{Datum(), 3, 7, 8});
    src_column->append_datum(DatumArray{4, 3, 2, Datum()});
    src_column->append_datum(Datum());

    ArrayDifference<LogicalType::TYPE_INT> difference;
    auto dest_column = difference.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 4);

    ASSERT_EQ(0, dest_column->get(0).get_array()[0].get_int64());
    ASSERT_TRUE(dest_column->get(0).get_array()[1].is_null());
    ASSERT_TRUE(dest_column->get(0).get_array()[2].is_null());

    ASSERT_TRUE(dest_column->get(1).get_array()[0].is_null());
    ASSERT_TRUE(dest_column->get(1).get_array()[1].is_null());
    ASSERT_EQ(4, dest_column->get(1).get_array()[2].get_int64());
    ASSERT_EQ(1, dest_column->get(1).get_array()[3].get_int64());

    ASSERT_EQ(0, dest_column->get(2).get_array()[0].get_int64());
    ASSERT_EQ(-1, dest_column->get(2).get_array()[1].get_int64());
    ASSERT_EQ(-1, dest_column->get(2).get_array()[2].get_int64());
    ASSERT_TRUE(dest_column->get(2).get_array()[3].is_null());
    ASSERT_TRUE(dest_column->get(3).is_null());
}

TEST_F(ArrayFunctionsTest, array_difference_bigint) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)2, (int64_t)3, (int64_t)7, (int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1});

    ArrayDifference<LogicalType::TYPE_BIGINT> difference;
    auto dest_column = difference.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<int64_t>({(int64_t)0, (int64_t)-2, (int64_t)3}, dest_column->get(0).get_array());
    _check_array<int64_t>({(int64_t)0, (int64_t)1, (int64_t)4, (int64_t)1}, dest_column->get(1).get_array());
    _check_array<int64_t>({(int64_t)0, (int64_t)-1, (int64_t)-1, (int64_t)-1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_difference_bigint_with_entry_null) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column->append_datum(DatumArray{(int64_t)5, Datum(), (int64_t)6});
    src_column->append_datum(DatumArray{Datum(), (int64_t)3, (int64_t)7, (int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, Datum()});
    src_column->append_datum(Datum());

    ArrayDifference<LogicalType::TYPE_BIGINT> difference;
    auto dest_column = difference.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 4);

    ASSERT_EQ((int64_t)0, dest_column->get(0).get_array()[0].get_int64());
    ASSERT_TRUE(dest_column->get(0).get_array()[1].is_null());
    ASSERT_TRUE(dest_column->get(0).get_array()[2].is_null());

    ASSERT_TRUE(dest_column->get(1).get_array()[0].is_null());
    ASSERT_TRUE(dest_column->get(1).get_array()[1].is_null());
    ASSERT_EQ((int64_t)4, dest_column->get(1).get_array()[2].get_int64());
    ASSERT_EQ((int64_t)1, dest_column->get(1).get_array()[3].get_int64());

    ASSERT_EQ((int64_t)0, dest_column->get(2).get_array()[0].get_int64());
    ASSERT_EQ((int64_t)-1, dest_column->get(2).get_array()[1].get_int64());
    ASSERT_EQ((int64_t)-1, dest_column->get(2).get_array()[2].get_int64());
    ASSERT_TRUE(dest_column->get(2).get_array()[3].is_null());
    ASSERT_TRUE(dest_column->get(3).is_null());
}

TEST_F(ArrayFunctionsTest, array_difference_double) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)2, (double)3, (double)7, (double)8});
    src_column->append_datum(DatumArray{(double)4, (double)3, (double)2, (double)1});

    ArrayDifference<LogicalType::TYPE_DOUBLE> difference;
    auto dest_column = difference.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<double>({(double)0, (double)-2, (double)3}, dest_column->get(0).get_array());
    _check_array<double>({(double)0, (double)1, (double)4, (double)1}, dest_column->get(1).get_array());
    _check_array<double>({(double)0, (double)-1, (double)-1, (double)-1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_slice_int) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{(int32_t)5, (int32_t)3, (int32_t)6});
    src_column->append_datum(DatumArray{(int32_t)2, (int32_t)3, (int32_t)7, (int32_t)8});
    src_column->append_datum(DatumArray{(int32_t)4, (int32_t)3, (int32_t)2, (int32_t)1});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int32_t)4, Datum(), (int32_t)2, (int32_t)1});
    src_column->append_datum(DatumArray{(int32_t)1, (int32_t)2, Datum(), (int32_t)4, (int32_t)5});
    src_column->append_datum(DatumArray{(int32_t)1, (int32_t)2, Datum(), (int32_t)4, (int32_t)5});
    src_column->append_datum(DatumArray{(int32_t)1, (int32_t)2, Datum(), (int32_t)4, (int32_t)5});

    Int64Column::Ptr offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(-2);
    offset_column->append(-7);
    offset_column->append(-8);

    Int64Column::Ptr length_column = Int64Column::create();
    length_column->append(1);
    length_column->append(3);
    length_column->append(2);
    length_column->append(1);
    length_column->append(2);
    length_column->append(3);
    length_column->append(3);
    length_column->append(3);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column, length_column}).value();

    ASSERT_EQ(dest_column->size(), 8);
    _check_array<int32_t>({(int32_t)5}, dest_column->get(0).get_array());
    _check_array<int32_t>({(int32_t)3, (int32_t)7, (int32_t)8}, dest_column->get(1).get_array());
    _check_array<int32_t>({(int32_t)2, (int32_t)1}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[1].get_int32());
    _check_array<int32_t>({(int32_t)4, (int32_t)5}, dest_column->get(5).get_array());
    _check_array<int32_t>({(int32_t)1}, dest_column->get(6).get_array());
    _check_array<int32_t>({}, dest_column->get(7).get_array());
}

TEST_F(ArrayFunctionsTest, array_slice_bigint) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)2, (int64_t)3, (int64_t)7, (int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int64_t)4, Datum(), (int64_t)2, (int64_t)1});

    Int64Column::Ptr offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);

    Int64Column::Ptr length_column = Int64Column::create();
    length_column->append(1);
    length_column->append(3);
    length_column->append(2);
    length_column->append(1);
    length_column->append(2);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column, length_column}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<int64_t>({(int64_t)5}, dest_column->get(0).get_array());
    _check_array<int64_t>({(int64_t)3, (int64_t)7, (int64_t)8}, dest_column->get(1).get_array());
    _check_array<int64_t>({(int64_t)2, (int64_t)1}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[1].get_int64());
}

TEST_F(ArrayFunctionsTest, array_slice_float) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_FLOAT, true);
    src_column->append_datum(DatumArray{(float)5, (float)3, (float)6});
    src_column->append_datum(DatumArray{(float)2, (float)3, (float)7, (float)8});
    src_column->append_datum(DatumArray{(float)4, (float)3, (float)2, (float)1});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(float)4, Datum(), (float)2, (float)1});

    Int64Column::Ptr offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);

    Int64Column::Ptr length_column = Int64Column::create();
    length_column->append(1);
    length_column->append(3);
    length_column->append(2);
    length_column->append(1);
    length_column->append(2);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column, length_column}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<float>({(float)5}, dest_column->get(0).get_array());
    _check_array<float>({(float)3, (float)7, (float)8}, dest_column->get(1).get_array());
    _check_array<float>({(float)2, (float)1}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[1].get_float());
}

TEST_F(ArrayFunctionsTest, array_slice_double) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)2, (double)3, (double)7, (double)8});
    src_column->append_datum(DatumArray{(double)4, (double)3, (double)2, (double)1});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(double)4, Datum(), (double)2, (double)1});

    Int64Column::Ptr offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);

    Int64Column::Ptr length_column = Int64Column::create();
    length_column->append(1);
    length_column->append(3);
    length_column->append(2);
    length_column->append(1);
    length_column->append(2);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column, length_column}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<double>({(double)5}, dest_column->get(0).get_array());
    _check_array<double>({(double)3, (double)7, (double)8}, dest_column->get(1).get_array());
    _check_array<double>({(double)2, (double)1}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[1].get_double());
}

TEST_F(ArrayFunctionsTest, array_slice_varchar) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("2"), Slice("3"), Slice("7"), Slice("8")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Slice("4"), Datum(), Slice("2"), Slice("1")});

    Int64Column::Ptr offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);

    Int64Column::Ptr length_column = Int64Column::create();
    length_column->append(1);
    length_column->append(3);
    length_column->append(2);
    length_column->append(1);
    length_column->append(2);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column, length_column}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<Slice>({Slice("5")}, dest_column->get(0).get_array());
    _check_array<Slice>({Slice("3"), Slice("7"), Slice("8")}, dest_column->get(1).get_array());
    _check_array<Slice>({Slice("2"), Slice("1")}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(Slice("2"), dest_column->get(4).get_array()[1].get_slice());
}

TEST_F(ArrayFunctionsTest, array_slice_bigint_only_offset) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)2, (int64_t)3, (int64_t)7, (int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int64_t)4, Datum(), (int64_t)2, (int64_t)1});

    Int64Column::Ptr offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<int64_t>({(int64_t)5, (int64_t)3, (int64_t)6}, dest_column->get(0).get_array());
    _check_array<int64_t>({(int64_t)3, (int64_t)7, (int64_t)8}, dest_column->get(1).get_array());
    _check_array<int64_t>({(int64_t)2, (int64_t)1}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[1].get_int64());
    ASSERT_EQ(1, dest_column->get(4).get_array()[2].get_int64());
}

TEST_F(ArrayFunctionsTest, array_slice_double_only_offset) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)2, (double)3, (double)7, (double)8});
    src_column->append_datum(DatumArray{(double)4, (double)3, (double)2, (double)1});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(double)4, Datum(), (double)2, (double)1});

    Int64Column::Ptr offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<double>({(double)5, (double)3, (double)6}, dest_column->get(0).get_array());
    _check_array<double>({(double)3, (double)7, (double)8}, dest_column->get(1).get_array());
    _check_array<double>({(double)2, (double)1}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[1].get_double());
    ASSERT_EQ(1, dest_column->get(4).get_array()[2].get_double());
}

TEST_F(ArrayFunctionsTest, array_slice_varchar_only_offset) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("2"), Slice("3"), Slice("7"), Slice("8")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Slice("4"), Datum(), Slice("2"), Slice("1")});

    Int64Column::Ptr offset_column = Int64Column::create();
    offset_column->append(1);
    offset_column->append(2);
    offset_column->append(3);
    offset_column->append(1);
    offset_column->append(2);

    auto dest_column = ArrayFunctions::array_slice(nullptr, {src_column, offset_column}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<Slice>({Slice("5"), Slice("3"), Slice("6")}, dest_column->get(0).get_array());
    _check_array<Slice>({Slice("3"), Slice("7"), Slice("8")}, dest_column->get(1).get_array());
    _check_array<Slice>({Slice("2"), Slice("1")}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
    ASSERT_EQ(Slice("2"), dest_column->get(4).get_array()[1].get_slice());
    ASSERT_EQ(Slice("1"), dest_column->get(4).get_array()[2].get_slice());
}

TEST_F(ArrayFunctionsTest, array_concat_tinyint) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column2->append_datum(DatumArray{(int8_t)5, (int8_t)6});
    src_column2->append_datum(DatumArray{(int8_t)2});
    src_column2->append_datum(DatumArray{(int8_t)4, (int8_t)3, (int8_t)2, (int8_t)1});
    src_column2->append_datum(DatumArray{(int8_t)4, (int8_t)9});
    src_column2->append_datum(DatumArray{(int8_t)4, Datum()});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column3->append_datum(DatumArray{(int8_t)5});
    src_column3->append_datum(DatumArray{(int8_t)2, (int8_t)8});
    src_column3->append_datum(DatumArray{(int8_t)2, (int8_t)1});
    src_column3->append_datum(DatumArray{(int8_t)100});
    src_column3->append_datum(DatumArray{(int8_t)4, Datum(), (int8_t)2, (int8_t)1});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<int8_t>({(int8_t)5, (int8_t)3, (int8_t)6, (int8_t)5, (int8_t)6, (int8_t)5},
                         dest_column->get(0).get_array());
    _check_array<int8_t>({(int8_t)8, (int8_t)2, (int8_t)2, (int8_t)8}, dest_column->get(1).get_array());
    _check_array<int8_t>({(int8_t)4, (int8_t)4, (int8_t)3, (int8_t)2, (int8_t)1, (int8_t)2, (int8_t)1},
                         dest_column->get(2).get_array());

    ASSERT_TRUE(dest_column->get(3).is_null());

    ASSERT_EQ(4, dest_column->get(4).get_array()[0].get_int8());
    ASSERT_EQ(1, dest_column->get(4).get_array()[1].get_int8());
    ASSERT_EQ(4, dest_column->get(4).get_array()[2].get_int8());
    ASSERT_TRUE(dest_column->get(4).get_array()[3].is_null());
    ASSERT_EQ(4, dest_column->get(4).get_array()[4].get_int8());
    ASSERT_TRUE(dest_column->get(4).get_array()[5].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[6].get_int8());
    ASSERT_EQ(1, dest_column->get(4).get_array()[7].get_int8());
}

TEST_F(ArrayFunctionsTest, array_concat_tinyint_not_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column2->append_datum(DatumArray{(int8_t)5, (int8_t)6});
    src_column2->append_datum(DatumArray{(int8_t)2});
    src_column2->append_datum(DatumArray{(int8_t)4, (int8_t)3, (int8_t)2, (int8_t)1});
    src_column2->append_datum(DatumArray{(int8_t)4, Datum()});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column3->append_datum(DatumArray{(int8_t)5});
    src_column3->append_datum(DatumArray{(int8_t)2, (int8_t)8});
    src_column3->append_datum(DatumArray{(int8_t)2, (int8_t)1});
    src_column3->append_datum(DatumArray{(int8_t)4, Datum(), (int8_t)2, (int8_t)1});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<int8_t>({(int8_t)5, (int8_t)3, (int8_t)6, (int8_t)5, (int8_t)6, (int8_t)5},
                         dest_column->get(0).get_array());
    _check_array<int8_t>({(int8_t)8, (int8_t)2, (int8_t)2, (int8_t)8}, dest_column->get(1).get_array());
    _check_array<int8_t>({(int8_t)4, (int8_t)4, (int8_t)3, (int8_t)2, (int8_t)1, (int8_t)2, (int8_t)1},
                         dest_column->get(2).get_array());

    ASSERT_EQ(4, dest_column->get(3).get_array()[0].get_int8());
    ASSERT_EQ(1, dest_column->get(3).get_array()[1].get_int8());
    ASSERT_EQ(4, dest_column->get(3).get_array()[2].get_int8());
    ASSERT_TRUE(dest_column->get(3).get_array()[3].is_null());
    ASSERT_EQ(4, dest_column->get(3).get_array()[4].get_int8());
    ASSERT_TRUE(dest_column->get(3).get_array()[5].is_null());
    ASSERT_EQ(2, dest_column->get(3).get_array()[6].get_int8());
    ASSERT_EQ(1, dest_column->get(3).get_array()[7].get_int8());
}

TEST_F(ArrayFunctionsTest, array_concat_bigint) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column2->append_datum(DatumArray{(int64_t)5, (int64_t)6});
    src_column2->append_datum(DatumArray{(int64_t)2});
    src_column2->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1});
    src_column2->append_datum(DatumArray{(int64_t)4, (int64_t)9});
    src_column2->append_datum(DatumArray{(int64_t)4, Datum()});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column3->append_datum(DatumArray{(int64_t)5});
    src_column3->append_datum(DatumArray{(int64_t)2, (int64_t)8});
    src_column3->append_datum(DatumArray{(int64_t)2, (int64_t)1});
    src_column3->append_datum(DatumArray{(int64_t)100});
    src_column3->append_datum(DatumArray{(int64_t)4, Datum(), (int64_t)2, (int64_t)1});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<int64_t>({(int64_t)5, (int64_t)3, (int64_t)6, (int64_t)5, (int64_t)6, (int64_t)5},
                          dest_column->get(0).get_array());
    _check_array<int64_t>({(int64_t)8, (int64_t)2, (int64_t)2, (int64_t)8}, dest_column->get(1).get_array());
    _check_array<int64_t>({(int64_t)4, (int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1, (int64_t)2, (int64_t)1},
                          dest_column->get(2).get_array());

    ASSERT_TRUE(dest_column->get(3).is_null());

    ASSERT_EQ(4, dest_column->get(4).get_array()[0].get_int64());
    ASSERT_EQ(1, dest_column->get(4).get_array()[1].get_int64());
    ASSERT_EQ(4, dest_column->get(4).get_array()[2].get_int64());
    ASSERT_TRUE(dest_column->get(4).get_array()[3].is_null());
    ASSERT_EQ(4, dest_column->get(4).get_array()[4].get_int64());
    ASSERT_TRUE(dest_column->get(4).get_array()[5].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[6].get_int64());
    ASSERT_EQ(1, dest_column->get(4).get_array()[7].get_int64());
}

TEST_F(ArrayFunctionsTest, array_concat_bigint_not_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4});
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
    src_column2->append_datum(DatumArray{(int64_t)5, (int64_t)6});
    src_column2->append_datum(DatumArray{(int64_t)2});
    src_column2->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1});
    src_column2->append_datum(DatumArray{(int64_t)4, Datum()});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
    src_column3->append_datum(DatumArray{(int64_t)5});
    src_column3->append_datum(DatumArray{(int64_t)2, (int64_t)8});
    src_column3->append_datum(DatumArray{(int64_t)2, (int64_t)1});
    src_column3->append_datum(DatumArray{(int64_t)4, Datum(), (int64_t)2, (int64_t)1});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<int64_t>({(int64_t)5, (int64_t)3, (int64_t)6, (int64_t)5, (int64_t)6, (int64_t)5},
                          dest_column->get(0).get_array());
    _check_array<int64_t>({(int64_t)8, (int64_t)2, (int64_t)2, (int64_t)8}, dest_column->get(1).get_array());
    _check_array<int64_t>({(int64_t)4, (int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1, (int64_t)2, (int64_t)1},
                          dest_column->get(2).get_array());

    ASSERT_EQ(4, dest_column->get(3).get_array()[0].get_int64());
    ASSERT_EQ(1, dest_column->get(3).get_array()[1].get_int64());
    ASSERT_EQ(4, dest_column->get(3).get_array()[2].get_int64());
    ASSERT_TRUE(dest_column->get(3).get_array()[3].is_null());
    ASSERT_EQ(4, dest_column->get(3).get_array()[4].get_int64());
    ASSERT_TRUE(dest_column->get(3).get_array()[5].is_null());
    ASSERT_EQ(2, dest_column->get(3).get_array()[6].get_int64());
    ASSERT_EQ(1, dest_column->get(3).get_array()[7].get_int64());
}

TEST_F(ArrayFunctionsTest, array_concat_double) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)8});
    src_column->append_datum(DatumArray{(double)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(double)4, (double)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column2->append_datum(DatumArray{(double)5, (double)6});
    src_column2->append_datum(DatumArray{(double)2});
    src_column2->append_datum(DatumArray{(double)4, (double)3, (double)2, (double)1});
    src_column2->append_datum(DatumArray{(double)4, (double)9});
    src_column2->append_datum(DatumArray{(double)4, Datum()});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column3->append_datum(DatumArray{(double)5});
    src_column3->append_datum(DatumArray{(double)2, (double)8});
    src_column3->append_datum(DatumArray{(double)2, (double)1});
    src_column3->append_datum(DatumArray{(double)100});
    src_column3->append_datum(DatumArray{(double)4, Datum(), (double)2, (double)1});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<double>({(double)5, (double)3, (double)6, (double)5, (double)6, (double)5},
                         dest_column->get(0).get_array());
    _check_array<double>({(double)8, (double)2, (double)2, (double)8}, dest_column->get(1).get_array());
    _check_array<double>({(double)4, (double)4, (double)3, (double)2, (double)1, (double)2, (double)1},
                         dest_column->get(2).get_array());

    ASSERT_TRUE(dest_column->get(3).is_null());

    ASSERT_EQ(4, dest_column->get(4).get_array()[0].get_double());
    ASSERT_EQ(1, dest_column->get(4).get_array()[1].get_double());
    ASSERT_EQ(4, dest_column->get(4).get_array()[2].get_double());
    ASSERT_TRUE(dest_column->get(4).get_array()[3].is_null());
    ASSERT_EQ(4, dest_column->get(4).get_array()[4].get_double());
    ASSERT_TRUE(dest_column->get(4).get_array()[5].is_null());
    ASSERT_EQ(2, dest_column->get(4).get_array()[6].get_double());
    ASSERT_EQ(1, dest_column->get(4).get_array()[7].get_double());
}

TEST_F(ArrayFunctionsTest, array_concat_double_not_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, false);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)8});
    src_column->append_datum(DatumArray{(double)4});
    src_column->append_datum(DatumArray{(double)4, (double)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, false);
    src_column2->append_datum(DatumArray{(double)5, (double)6});
    src_column2->append_datum(DatumArray{(double)2});
    src_column2->append_datum(DatumArray{(double)4, (double)3, (double)2, (double)1});
    src_column2->append_datum(DatumArray{(double)4, Datum()});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, false);
    src_column3->append_datum(DatumArray{(double)5});
    src_column3->append_datum(DatumArray{(double)2, (double)8});
    src_column3->append_datum(DatumArray{(double)2, (double)1});
    src_column3->append_datum(DatumArray{(double)4, Datum(), (double)2, (double)1});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<double>({(double)5, (double)3, (double)6, (double)5, (double)6, (double)5},
                         dest_column->get(0).get_array());
    _check_array<double>({(double)8, (double)2, (double)2, (double)8}, dest_column->get(1).get_array());
    _check_array<double>({(double)4, (double)4, (double)3, (double)2, (double)1, (double)2, (double)1},
                         dest_column->get(2).get_array());

    ASSERT_EQ(4, dest_column->get(3).get_array()[0].get_double());
    ASSERT_EQ(1, dest_column->get(3).get_array()[1].get_double());
    ASSERT_EQ(4, dest_column->get(3).get_array()[2].get_double());
    ASSERT_TRUE(dest_column->get(3).get_array()[3].is_null());
    ASSERT_EQ(4, dest_column->get(3).get_array()[4].get_double());
    ASSERT_TRUE(dest_column->get(3).get_array()[5].is_null());
    ASSERT_EQ(2, dest_column->get(3).get_array()[6].get_double());
    ASSERT_EQ(1, dest_column->get(3).get_array()[7].get_double());
}

TEST_F(ArrayFunctionsTest, array_concat_varchar) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4")});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column2->append_datum(DatumArray{Slice("5"), Slice("6")});
    src_column2->append_datum(DatumArray{Slice("2")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("9")});
    src_column2->append_datum(DatumArray{Slice("4"), Datum()});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column3->append_datum(DatumArray{Slice("5")});
    src_column3->append_datum(DatumArray{Slice("2"), Slice("8")});
    src_column3->append_datum(DatumArray{Slice("2"), Slice("1")});
    src_column3->append_datum(DatumArray{Slice("100")});
    src_column3->append_datum(DatumArray{Slice("4"), Datum(), Slice("2"), Slice("1")});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 5);
    _check_array<Slice>({Slice("5"), Slice("3"), Slice("6"), Slice("5"), Slice("6"), Slice("5")},
                        dest_column->get(0).get_array());
    _check_array<Slice>({Slice("8"), Slice("2"), Slice("2"), Slice("8")}, dest_column->get(1).get_array());
    _check_array<Slice>({Slice("4"), Slice("4"), Slice("3"), Slice("2"), Slice("1"), Slice("2"), Slice("1")},
                        dest_column->get(2).get_array());

    ASSERT_TRUE(dest_column->get(3).is_null());

    ASSERT_EQ(Slice("4"), dest_column->get(4).get_array()[0].get_slice());
    ASSERT_EQ(Slice("1"), dest_column->get(4).get_array()[1].get_slice());
    ASSERT_EQ(Slice("4"), dest_column->get(4).get_array()[2].get_slice());
    ASSERT_TRUE(dest_column->get(4).get_array()[3].is_null());
    ASSERT_EQ(Slice("4"), dest_column->get(4).get_array()[4].get_slice());
    ASSERT_TRUE(dest_column->get(4).get_array()[5].is_null());
    ASSERT_EQ(Slice("2"), dest_column->get(4).get_array()[6].get_slice());
    ASSERT_EQ(Slice("1"), dest_column->get(4).get_array()[7].get_slice());
}

TEST_F(ArrayFunctionsTest, array_concat_varchar_not_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column2->append_datum(DatumArray{Slice("5"), Slice("6")});
    src_column2->append_datum(DatumArray{Slice("2")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column2->append_datum(DatumArray{Slice("4"), Datum()});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column3->append_datum(DatumArray{Slice("5")});
    src_column3->append_datum(DatumArray{Slice("2"), Slice("8")});
    src_column3->append_datum(DatumArray{Slice("2"), Slice("1")});
    src_column3->append_datum(DatumArray{Slice("4"), Datum(), Slice("2"), Slice("1")});

    auto dest_column = ArrayFunctions::concat(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<Slice>({Slice("5"), Slice("3"), Slice("6"), Slice("5"), Slice("6"), Slice("5")},
                        dest_column->get(0).get_array());
    _check_array<Slice>({Slice("8"), Slice("2"), Slice("2"), Slice("8")}, dest_column->get(1).get_array());
    _check_array<Slice>({Slice("4"), Slice("4"), Slice("3"), Slice("2"), Slice("1"), Slice("2"), Slice("1")},
                        dest_column->get(2).get_array());

    ASSERT_EQ(Slice("4"), dest_column->get(3).get_array()[0].get_slice());
    ASSERT_EQ(Slice("1"), dest_column->get(3).get_array()[1].get_slice());
    ASSERT_EQ(Slice("4"), dest_column->get(3).get_array()[2].get_slice());
    ASSERT_TRUE(dest_column->get(3).get_array()[3].is_null());
    ASSERT_EQ(Slice("4"), dest_column->get(3).get_array()[4].get_slice());
    ASSERT_TRUE(dest_column->get(3).get_array()[5].is_null());
    ASSERT_EQ(Slice("2"), dest_column->get(3).get_array()[6].get_slice());
    ASSERT_EQ(Slice("1"), dest_column->get(3).get_array()[7].get_slice());
}

TEST_F(ArrayFunctionsTest, array_overlap_tinyint_with_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column2->append_datum(DatumArray{(int8_t)5, (int8_t)6});
    src_column2->append_datum(DatumArray{(int8_t)2});
    src_column2->append_datum(DatumArray{(int8_t)4, (int8_t)3, (int8_t)2, (int8_t)1});
    src_column2->append_datum(DatumArray{(int8_t)4, (int8_t)9});
    src_column2->append_datum(DatumArray{(int8_t)4, Datum()});

    ArrayOverlap<LogicalType::TYPE_TINYINT> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(
            ColumnHelper::as_raw_column<NullableColumn>(dest_column)->data_column());
    auto null_data = ColumnHelper::as_raw_column<NullableColumn>(dest_column)->immutable_null_column_data().data();

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_TRUE(null_data[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_tinyint) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(DatumArray{(int8_t)99});
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column2->append_datum(DatumArray{(int8_t)5, (int8_t)6});
    src_column2->append_datum(DatumArray{(int8_t)2});
    src_column2->append_datum(DatumArray{(int8_t)4, (int8_t)3, (int8_t)2, (int8_t)1});
    src_column2->append_datum(DatumArray{(int8_t)4, (int8_t)9});
    src_column2->append_datum(DatumArray{(int8_t)4, Datum()});

    ArrayOverlap<LogicalType::TYPE_TINYINT> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(dest_column);

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_FALSE(v->get_data()[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_bigint_with_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
    src_column2->append_datum(DatumArray{(int64_t)5, (int64_t)6});
    src_column2->append_datum(DatumArray{(int64_t)2});
    src_column2->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1});
    src_column2->append_datum(DatumArray{(int64_t)4, (int64_t)9});
    src_column2->append_datum(DatumArray{(int64_t)4, Datum()});

    ArrayOverlap<LogicalType::TYPE_BIGINT> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(
            ColumnHelper::as_raw_column<NullableColumn>(dest_column)->data_column());
    auto null_data = ColumnHelper::as_raw_column<NullableColumn>(dest_column)->immutable_null_column_data().data();

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_TRUE(null_data[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_bigint) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4});
    src_column->append_datum(DatumArray{(int64_t)99});
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
    src_column2->append_datum(DatumArray{(int64_t)5, (int64_t)6});
    src_column2->append_datum(DatumArray{(int64_t)2});
    src_column2->append_datum(DatumArray{(int64_t)4, (int64_t)3, (int64_t)2, (int64_t)1});
    src_column2->append_datum(DatumArray{(int64_t)4, (int64_t)9});
    src_column2->append_datum(DatumArray{(int64_t)4, Datum()});

    ArrayOverlap<LogicalType::TYPE_BIGINT> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(dest_column);

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_FALSE(v->get_data()[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_double_with_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)8});
    src_column->append_datum(DatumArray{(double)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(double)4, (double)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, false);
    src_column2->append_datum(DatumArray{(double)5, (double)6});
    src_column2->append_datum(DatumArray{(double)2});
    src_column2->append_datum(DatumArray{(double)4, (double)3, (double)2, (double)1});
    src_column2->append_datum(DatumArray{(double)4, (double)9});
    src_column2->append_datum(DatumArray{(double)4, Datum()});

    ArrayOverlap<LogicalType::TYPE_DOUBLE> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(
            ColumnHelper::as_raw_column<NullableColumn>(dest_column)->data_column());
    auto null_data = ColumnHelper::as_raw_column<NullableColumn>(dest_column)->immutable_null_column_data().data();

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_TRUE(null_data[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_double) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, false);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)8});
    src_column->append_datum(DatumArray{(double)4});
    src_column->append_datum(DatumArray{(double)99});
    src_column->append_datum(DatumArray{(double)4, (double)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, false);
    src_column2->append_datum(DatumArray{(double)5, (double)6});
    src_column2->append_datum(DatumArray{(double)2});
    src_column2->append_datum(DatumArray{(double)4, (double)3, (double)2, (double)1});
    src_column2->append_datum(DatumArray{(double)4, (double)9});
    src_column2->append_datum(DatumArray{(double)4, Datum()});

    ArrayOverlap<LogicalType::TYPE_DOUBLE> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(dest_column);

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_FALSE(v->get_data()[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_varchar_with_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4")});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column2->append_datum(DatumArray{Slice("5"), Slice("6")});
    src_column2->append_datum(DatumArray{Slice("2")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("9")});
    src_column2->append_datum(DatumArray{Slice("4"), Datum()});

    ArrayOverlap<LogicalType::TYPE_VARCHAR> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(
            ColumnHelper::as_raw_column<NullableColumn>(dest_column)->data_column());
    auto null_data = ColumnHelper::as_raw_column<NullableColumn>(dest_column)->immutable_null_column_data().data();

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_TRUE(null_data[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_varchar) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4")});
    src_column->append_datum(DatumArray{Slice("99")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column2->append_datum(DatumArray{Slice("5"), Slice("6")});
    src_column2->append_datum(DatumArray{Slice("2")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("9")});
    src_column2->append_datum(DatumArray{Slice("4"), Datum()});

    ArrayOverlap<LogicalType::TYPE_VARCHAR> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2}).value();
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(dest_column);

    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_TRUE(v->get_data()[2]);
    ASSERT_FALSE(v->get_data()[3]);
    ASSERT_TRUE(v->get_data()[4]);
}

TEST_F(ArrayFunctionsTest, array_overlap_with_onlynull) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});

    ColumnPtr src_column2 = ColumnHelper::create_const_null_column(1);

    ArrayOverlap<LogicalType::TYPE_TINYINT> overlap;
    ASSERT_TRUE(overlap.prepare(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    auto dest_column = overlap.process(&_ctx, {src_column, src_column2});
    ASSERT_TRUE(overlap.close(&_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(dest_column->get()->only_null());
}

TEST_F(ArrayFunctionsTest, array_intersect_int) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{(int32_t)5, (int32_t)3, (int32_t)6});
    src_column->append_datum(DatumArray{(int32_t)8});
    src_column->append_datum(DatumArray{(int32_t)4, (int32_t)1});
    src_column->append_datum(Datum());

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column2->append_datum(DatumArray{(int32_t)(5), (int32_t)(6)});
    src_column2->append_datum(DatumArray{(int32_t)(2)});
    src_column2->append_datum(DatumArray{(int32_t)(4), (int32_t)(3), (int32_t)(2), (int32_t)(1)});
    src_column2->append_datum(DatumArray{(int32_t)(4), (int32_t)(9)});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column3->append_datum(DatumArray{(int32_t)(5)});
    src_column3->append_datum(DatumArray{(int32_t)(2), (int32_t)(8)});
    src_column3->append_datum(DatumArray{(int32_t)(4), (int32_t)(1)});
    src_column3->append_datum(DatumArray{(int32_t)(100)});

    ArrayIntersect<LogicalType::TYPE_INT> intersect;
    auto dest_column = intersect.process(nullptr, {src_column, src_column2, src_column3});

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<int32_t>({(int32_t)(5)}, dest_column->get(0).get_array());
    _check_array<int32_t>({}, dest_column->get(1).get_array());
    _check_array<int32_t>({(int32_t)(4), (int32_t)(1)}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
}

TEST_F(ArrayFunctionsTest, array_intersect_int_with_not_null) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
    src_column->append_datum(DatumArray{(int32_t)(5), (int32_t)(3), (int32_t)(6)});
    src_column->append_datum(DatumArray{(int32_t)(8)});
    src_column->append_datum(DatumArray{(int32_t)(4), (int32_t)(1)});
    src_column->append_datum(DatumArray{(int32_t)(4), (int32_t)(22), Datum()});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
    src_column2->append_datum(DatumArray{(int32_t)(5), (int32_t)(6)});
    src_column2->append_datum(DatumArray{(int32_t)(2)});
    src_column2->append_datum(DatumArray{(int32_t)(4), (int32_t)(3), (int32_t)(2), (int32_t)(1)});
    src_column2->append_datum(DatumArray{(int32_t)(4), Datum(), (int32_t)(22), (int32_t)(66)});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
    src_column3->append_datum(DatumArray{(int32_t)(5)});
    src_column3->append_datum(DatumArray{(int32_t)(2), (int32_t)(8)});
    src_column3->append_datum(DatumArray{(int32_t)(4), (int32_t)(1)});
    src_column3->append_datum(DatumArray{(int32_t)(4), Datum(), (int32_t)(2), (int32_t)(22), (int32_t)(1)});

    ArrayIntersect<LogicalType::TYPE_INT> intersect;
    auto dest_column = intersect.process(nullptr, {src_column, src_column2, src_column3});

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<int32_t>({(int32_t)(5)}, dest_column->get(0).get_array());
    _check_array<int32_t>({}, dest_column->get(1).get_array());

    {
        std::unordered_set<int32_t> set_expect = {4, 1};
        std::unordered_set<int32_t> set_actual;
        auto result_array = dest_column->get(2).get_array();
        for (auto& i : result_array) {
            set_actual.insert(i.get_int32());
        }
        ASSERT_TRUE(set_expect == set_actual);
    }

    {
        std::unordered_set<int32_t> set_expect = {4, 22};
        std::unordered_set<int32_t> set_actual;
        size_t null_values = 0;
        auto result_array = dest_column->get(3).get_array();
        for (auto& i : result_array) {
            if (i.is_null()) {
                ++null_values;
            } else {
                set_actual.insert(i.get_int32());
            }
        }
        ASSERT_TRUE(set_expect == set_actual);
        ASSERT_EQ(null_values, 1);
    }
}

TEST_F(ArrayFunctionsTest, array_intersect_varchar) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});
    src_column->append_datum(Datum());

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column2->append_datum(DatumArray{Slice("5"), Slice("6")});
    src_column2->append_datum(DatumArray{Slice("2")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("9")});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column3->append_datum(DatumArray{Slice("5")});
    src_column3->append_datum(DatumArray{Slice("2"), Slice("8")});
    src_column3->append_datum(DatumArray{Slice("4"), Slice("1")});
    src_column3->append_datum(DatumArray{Slice("100")});

    ArrayIntersect<LogicalType::TYPE_VARCHAR> intersect;
    auto dest_column = intersect.process(nullptr, {src_column, src_column2, src_column3});

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<Slice>({Slice("5")}, dest_column->get(0).get_array());
    _check_array<Slice>({}, dest_column->get(1).get_array());
    _check_array<Slice>({Slice("4"), Slice("1")}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
}

TEST_F(ArrayFunctionsTest, array_intersect_varchar_with_not_null) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("22"), Datum()});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column2->append_datum(DatumArray{Slice("5"), Slice("6")});
    src_column2->append_datum(DatumArray{Slice("2")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column2->append_datum(DatumArray{Slice("4"), Datum(), Slice("22"), Slice("66")});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column3->append_datum(DatumArray{Slice("5")});
    src_column3->append_datum(DatumArray{Slice("2"), Slice("8")});
    src_column3->append_datum(DatumArray{Slice("4"), Slice("1")});
    src_column3->append_datum(DatumArray{Slice("4"), Datum(), Slice("2"), Slice("22"), Slice("1")});

    ArrayIntersect<LogicalType::TYPE_VARCHAR> intersect;
    auto dest_column = intersect.process(nullptr, {src_column, src_column2, src_column3});

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<Slice>({Slice("5")}, dest_column->get(0).get_array());
    _check_array<Slice>({}, dest_column->get(1).get_array());

    {
        std::unordered_set<std::string> set_expect = {"4", "1"};
        std::unordered_set<std::string> set_actual;
        auto result_array = dest_column->get(2).get_array();
        for (auto& i : result_array) {
            set_actual.insert(i.get_slice().to_string());
        }
        ASSERT_TRUE(set_expect == set_actual);
    }

    {
        std::unordered_set<std::string> set_expect = {"4", "22"};
        std::unordered_set<std::string> set_actual;
        size_t null_values = 0;
        auto result_array = dest_column->get(3).get_array();
        for (auto& i : result_array) {
            if (i.is_null()) {
                ++null_values;
            } else {
                set_actual.insert(i.get_slice().to_string());
            }
        }
        ASSERT_TRUE(set_expect == set_actual);
        ASSERT_EQ(null_values, 1);
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_join_string) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"352", "66", "4325"});
    src_column->append_datum(DatumArray{"235", "99", "8", "43251"});
    src_column->append_datum(DatumArray{"44", "33", "22", "112"});

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    Slice null_str("NULL");
    auto null_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(null_str, 3);

    auto dest_column = ArrayJoin::process(nullptr, {src_column, sep_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("352__66__4325"), dest_column->get(0).get_slice());
    ASSERT_EQ(Slice("235__99__8__43251"), dest_column->get(1).get_slice());
    ASSERT_EQ(Slice("44__33__22__112"), dest_column->get(2).get_slice());

    dest_column = ArrayJoin::process(nullptr, {src_column, sep_column, null_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("352__66__4325"), dest_column->get(0).get_slice());
    ASSERT_EQ(Slice("235__99__8__43251"), dest_column->get(1).get_slice());
    ASSERT_EQ(Slice("44__33__22__112"), dest_column->get(2).get_slice());
}

TEST_F(ArrayFunctionsTest, array_concat_ws) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"352", "66", "4325"});
    src_column->append_datum(DatumArray{"235", "99", "8", "43251"});
    src_column->append_datum(DatumArray{"44", "33", "22", "112"});

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    ColumnPtr dest_column = ArrayFunctions::array_concat_ws(nullptr, {sep_column, src_column}).value();
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("352__66__4325"), dest_column->get(0).get_slice());
    ASSERT_EQ(Slice("235__99__8__43251"), dest_column->get(1).get_slice());
    ASSERT_EQ(Slice("44__33__22__112"), dest_column->get(2).get_slice());
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_join_nullable_elements) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"55", Datum(), "333", "6666"});
    src_column->append_datum(DatumArray{"22", "333", Datum(), Datum()});
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    Slice null_str("NULL");
    auto null_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(null_str, 3);

    auto dest_column = ArrayJoin::process(nullptr, {src_column, sep_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("55__333__6666"), dest_column->get(0).get_slice());
    ASSERT_EQ(Slice("22__333"), dest_column->get(1).get_slice());
    ASSERT_EQ(Slice(""), dest_column->get(2).get_slice());

    dest_column = ArrayJoin::process(nullptr, {src_column, sep_column, null_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("55__NULL__333__6666"), dest_column->get(0).get_slice());
    ASSERT_EQ(Slice("22__333__NULL__NULL"), dest_column->get(1).get_slice());
    ASSERT_EQ(Slice("NULL__NULL__NULL__NULL"), dest_column->get(2).get_slice());
}

TEST_F(ArrayFunctionsTest, array_concat_ws_nullable_elements) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"55", Datum(), "333", "6666"});
    src_column->append_datum(DatumArray{"22", "333", Datum(), Datum()});
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    ColumnPtr dest_column = ArrayFunctions::array_concat_ws(nullptr, {sep_column, src_column}).value();
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("55__333__6666"), dest_column->get(0).get_slice());
    ASSERT_EQ(Slice("22__333"), dest_column->get(1).get_slice());
    ASSERT_EQ(Slice(""), dest_column->get(2).get_slice());
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_join_nullable_array) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"5", Datum(), "33", "666"});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    Slice null_str("NULL");
    auto null_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(null_str, 3);

    auto dest_column = ArrayJoin::process(nullptr, {src_column, sep_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("5__33__666"), dest_column->get(0).get_slice());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_EQ(Slice(""), dest_column->get(2).get_slice());

    dest_column = ArrayJoin::process(nullptr, {src_column, sep_column, null_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("5__NULL__33__666"), dest_column->get(0).get_slice());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_EQ(Slice("NULL__NULL__NULL__NULL"), dest_column->get(2).get_slice());
}

TEST_F(ArrayFunctionsTest, array_concat_ws_nullable_array) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"5", Datum(), "33", "666"});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    ColumnPtr dest_column = ArrayFunctions::array_concat_ws(nullptr, {sep_column, src_column}).value();
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_EQ(Slice("5__33__666"), dest_column->get(0).get_slice());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_EQ(Slice(""), dest_column->get(2).get_slice());
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_join_only_null) {
    ColumnPtr src_column = ColumnHelper::create_const_null_column(3);

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    Slice null_str("NULL");
    auto null_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(null_str, 3);

    auto dest_column = ArrayJoin::process(nullptr, {src_column, sep_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_TRUE(dest_column->get(0).is_null());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_TRUE(dest_column->get(2).is_null());

    dest_column = ArrayJoin::process(nullptr, {src_column, sep_column, null_column});
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_TRUE(dest_column->get(0).is_null());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_TRUE(dest_column->get(2).is_null());
}

TEST_F(ArrayFunctionsTest, array_concat_ws_only_null) {
    ColumnPtr src_column = ColumnHelper::create_const_null_column(3);

    Slice sep_str("__");
    auto sep_column = ColumnHelper::create_const_column<LogicalType::TYPE_VARCHAR>(sep_str, 3);

    ColumnPtr dest_column = ArrayFunctions::array_concat_ws(nullptr, {sep_column, src_column}).value();
    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_TRUE(dest_column->get(0).is_null());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_TRUE(dest_column->get(2).is_null());
}

TEST_F(ArrayFunctionsTest, array_filter_tinyint_with_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column2->append_datum(DatumArray{true, false, true});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{true, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);
    _check_array<int8_t>({(int8_t)(5), (int8_t)(6)}, dest_column->get(0).get_array());
    ASSERT_TRUE(dest_column->get(1).get_array().empty());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).is_null());
    _check_array<int8_t>({(int8_t)(4)}, dest_column->get(4).get_array());
}

TEST_F(ArrayFunctionsTest, array_filter_tinyint) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(DatumArray{(int8_t)99});
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
    src_column2->append_datum(DatumArray{Datum(), true, false});
    src_column2->append_datum(DatumArray{true});
    src_column2->append_datum(DatumArray{false});
    src_column2->append_datum(DatumArray{Datum()});
    src_column2->append_datum(DatumArray{false, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    _check_array<int8_t>({(int8_t)(3)}, dest_column->get(0).get_array());
    _check_array<int8_t>({(int8_t)(8)}, dest_column->get(1).get_array());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).get_array().empty());
    ASSERT_TRUE(dest_column->get(4).get_array().empty());
}

TEST_F(ArrayFunctionsTest, array_filter_tinyint_with_nullable_notnull) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
    src_column2->append_datum(DatumArray{Datum(), true, false});
    src_column2->append_datum(DatumArray{true});
    src_column2->append_datum(DatumArray{false});
    src_column2->append_datum(DatumArray{Datum()});
    src_column2->append_datum(DatumArray{false, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);
    _check_array<int8_t>({(int8_t)(3)}, dest_column->get(0).get_array());
    _check_array<int8_t>({(int8_t)(8)}, dest_column->get(1).get_array());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_array().empty());
}

TEST_F(ArrayFunctionsTest, array_filter_tinyint_notnull_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)8});
    src_column->append_datum(DatumArray{(int8_t)4});
    src_column->append_datum(DatumArray{(int8_t)99});
    src_column->append_datum(DatumArray{(int8_t)4, (int8_t)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column2->append_datum(DatumArray{true, false, true});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{true, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    _check_array<int8_t>({(int8_t)(5), (int8_t)(6)}, dest_column->get(0).get_array());
    ASSERT_TRUE(dest_column->get(1).get_array().empty());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).get_array().empty());
    _check_array<int8_t>({(int8_t)(4)}, dest_column->get(4).get_array());
}

TEST_F(ArrayFunctionsTest, array_filter_bigint_with_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, true);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column2->append_datum(DatumArray{true, false, true});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{true, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);
    _check_array<int64_t>({(int64_t)(5), (int64_t)(6)}, dest_column->get(0).get_array());
    ASSERT_TRUE(dest_column->get(1).get_array().empty());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).is_null());
    _check_array<int64_t>({(int64_t)(4)}, dest_column->get(4).get_array());
}

TEST_F(ArrayFunctionsTest, array_filter_bigint) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
    src_column->append_datum(DatumArray{(int64_t)5, (int64_t)3, (int64_t)6});
    src_column->append_datum(DatumArray{(int64_t)8});
    src_column->append_datum(DatumArray{(int64_t)4});
    src_column->append_datum(DatumArray{(int64_t)99});
    src_column->append_datum(DatumArray{(int64_t)4, (int64_t)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
    src_column2->append_datum(DatumArray{Datum(), true});
    src_column2->append_datum(DatumArray{true});
    src_column2->append_datum(DatumArray{false});
    src_column2->append_datum(DatumArray{Datum()});
    src_column2->append_datum(DatumArray{false, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    _check_array<int64_t>({(int64_t)(3)}, dest_column->get(0).get_array());
    _check_array<int64_t>({(int64_t)(8)}, dest_column->get(1).get_array());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).get_array().empty());
    ASSERT_TRUE(dest_column->get(4).get_array().empty());
}

TEST_F(ArrayFunctionsTest, array_filter_double_with_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, true);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)8});
    src_column->append_datum(DatumArray{(double)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(double)4, (double)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column2->append_datum(DatumArray{true, false, true});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{true, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);
    _check_array<double>({(double)(5), (double)(6)}, dest_column->get(0).get_array());
    ASSERT_TRUE(dest_column->get(1).get_array().empty());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).is_null());
    _check_array<double>({(double)(4)}, dest_column->get(4).get_array());
}

TEST_F(ArrayFunctionsTest, array_filter_double) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_DOUBLE, false);
    src_column->append_datum(DatumArray{(double)5, (double)3, (double)6});
    src_column->append_datum(DatumArray{(double)8});
    src_column->append_datum(DatumArray{(double)4});
    src_column->append_datum(DatumArray{(double)99});
    src_column->append_datum(DatumArray{(double)4, (double)1});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
    src_column2->append_datum(DatumArray{Datum(), true, false, true}); //more one
    src_column2->append_datum(DatumArray{true});
    src_column2->append_datum(DatumArray{false});
    src_column2->append_datum(DatumArray{Datum()});
    src_column2->append_datum(DatumArray{false, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    _check_array<double>({(double)(3)}, dest_column->get(0).get_array());
    _check_array<double>({(double)(8)}, dest_column->get(1).get_array());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).get_array().empty());
    ASSERT_TRUE(dest_column->get(4).get_array().empty());
}

TEST_F(ArrayFunctionsTest, array_filter_varchar_with_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4")});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column2->append_datum(DatumArray{true, false, true});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{true, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);
    _check_array<Slice>({Slice("5"), Slice("6")}, dest_column->get(0).get_array());
    ASSERT_TRUE(dest_column->get(1).get_array().empty());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).is_null());
    _check_array<Slice>({Slice("4")}, dest_column->get(4).get_array());
}

TEST_F(ArrayFunctionsTest, array_filter_varchar) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4")});
    src_column->append_datum(DatumArray{Slice("99")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
    src_column2->append_datum(DatumArray{Datum(), true, false, true}); //more one
    src_column2->append_datum(DatumArray{true});
    src_column2->append_datum(DatumArray{false});
    src_column2->append_datum(DatumArray{Datum()});
    src_column2->append_datum(DatumArray{false, Datum()});

    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 5);

    _check_array<Slice>({Slice("3")}, dest_column->get(0).get_array());
    _check_array<Slice>({Slice("8")}, dest_column->get(1).get_array());
    ASSERT_TRUE(dest_column->get(2).get_array().empty());
    ASSERT_TRUE(dest_column->get(3).get_array().empty());
    ASSERT_TRUE(dest_column->get(4).get_array().empty());
}

TEST_F(ArrayFunctionsTest, array_filter_with_onlynull) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});

    ColumnPtr src_column2 = ColumnHelper::create_const_null_column(1);

    // bool_array is null
    ArrayFilter filter;
    auto dest_column = filter.process(nullptr, {src_column, src_column2});
    ASSERT_TRUE(dest_column->get(0).get_array().empty());

    // array is null
    dest_column = filter.process(nullptr, {src_column2, src_column});
    ASSERT_TRUE(dest_column->only_null());

    // all null
    dest_column = filter.process(nullptr, {src_column2, src_column2});
    ASSERT_TRUE(dest_column->only_null());

    // src is nullable & bool_array is null
    ColumnPtr src_column_nullable = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column_nullable->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column_nullable->append_datum(Datum());
    dest_column = filter.process(nullptr, {src_column_nullable, src_column2});
    auto null_data = ColumnHelper::as_raw_column<NullableColumn>(dest_column)->immutable_null_column_data();
    ASSERT_TRUE(null_data.size() == 2);
    ASSERT_TRUE(!null_data.data()[0]);
    ASSERT_TRUE(null_data.data()[1]);
}

TEST_F(ArrayFunctionsTest, array_distinct_only_null) {
    // test only null
    {
        ColumnPtr src_column = ColumnHelper::create_const_null_column(3);
        auto dest_column = ArrayDistinct<TYPE_VARCHAR>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_TRUE(dest_column->only_null());
    }
    // test const
    {
        ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        src_column->append_datum(DatumArray{"5", "5", "33", "666"});
        src_column = ConstColumn::create(src_column, 3);
        auto dest_column = ArrayDistinct<TYPE_VARCHAR>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_STREQ(dest_column->debug_string().c_str(), "[['5','33','666'], ['5','33','666'], ['5','33','666']]");
    }
    // test normal
    {
        ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        src_column->append_datum(DatumArray{"5", "5", "33", "666"});
        auto dest_column = ArrayDistinct<TYPE_VARCHAR>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 1);
        ASSERT_STREQ(dest_column->debug_string().c_str(), "[['5','33','666']]");
    }
}

TEST_F(ArrayFunctionsTest, array_sortby_tinyint_with_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column->append_datum(DatumArray{(int8_t)3, (int8_t)4, (int8_t)5});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int8_t)2, (int8_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{});
    src_column->append_datum(DatumArray{Datum(), (int8_t)-23});
    src_column->append_datum(DatumArray{(int8_t)43, (int8_t)23});
    src_column->append_datum(DatumArray{(int8_t)43, (int8_t)23, Datum()});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column2->append_datum(DatumArray{(int8_t)82, (int8_t)1, (int8_t)4});
    src_column2->append_datum(DatumArray{(int8_t)23});
    src_column2->append_datum(Datum());
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{(int8_t)3, (int8_t)6});
    src_column2->append_datum(DatumArray{(int8_t)-23, Datum()});
    src_column2->append_datum(DatumArray{(int8_t)3, (int8_t)6, Datum()});

    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column, src_column2});

        ASSERT_TRUE(dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 10);
        _check_array<int8_t>({(int8_t)(4), (int8_t)(5), (int8_t)(3)}, dest_column->get(0).get_array());
        ASSERT_TRUE(dest_column->get(1).is_null());
        _check_array<int8_t>({(int8_t)(2), (int8_t)(4)}, dest_column->get(2).get_array());
        ASSERT_TRUE(dest_column->get(3).is_null());
        ASSERT_TRUE(dest_column->get(4).get_array().empty());
        ASSERT_TRUE(dest_column->get(5).is_null());
        ASSERT_TRUE(dest_column->get(6).get_array().empty());
        ASSERT_TRUE(dest_column->get(7).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(7).get_array()[1].get_int8(), (int8_t)(-23));
        _check_array<int8_t>({(int8_t)(23), (int8_t)(43)}, dest_column->get(8).get_array());
        ASSERT_TRUE(dest_column->get(9).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(9).get_array()[1].get_int8(), (int8_t)(43));
        ASSERT_EQ(dest_column->get(9).get_array()[2].get_int8(), (int8_t)(23));
    }
    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column2, src_column});

        ASSERT_TRUE(dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 10);
        _check_array<int8_t>({(int8_t)(82), (int8_t)(1), (int8_t)(4)}, dest_column->get(0).get_array());
        _check_array<int8_t>({(int8_t)(23)}, dest_column->get(1).get_array());
        ASSERT_TRUE(dest_column->get(2).is_null());
        ASSERT_TRUE(dest_column->get(3).is_null());
        ASSERT_TRUE(dest_column->get(4).get_array().empty());
        ASSERT_TRUE(dest_column->get(5).get_array().empty());
        ASSERT_TRUE(dest_column->get(6).is_null());
        _check_array<int8_t>({(int8_t)(3), (int8_t)(6)}, dest_column->get(7).get_array());
        ASSERT_TRUE(dest_column->get(8).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(8).get_array()[1].get_int8(), (int8_t)(-23));
        ASSERT_TRUE(dest_column->get(9).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(9).get_array()[1].get_int8(), (int8_t)(6));
        ASSERT_EQ(dest_column->get(9).get_array()[2].get_int8(), (int8_t)(3));
    }
}

TEST_F(ArrayFunctionsTest, array_sortby_tinyint) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});
    src_column->append_datum(DatumArray{});
    src_column->append_datum(DatumArray{(int8_t)125, (int8_t)123});
    src_column->append_datum(DatumArray{Datum()});
    src_column->append_datum(DatumArray{(int8_t)4, Datum()});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column2->append_datum(DatumArray{(int8_t)3, (int8_t)73, (int8_t)30});
    src_column2->append_datum(DatumArray{(int8_t)3, (int8_t)2, (int8_t)1});
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(DatumArray{Datum(), (int8_t)-43});
    src_column2->append_datum(DatumArray{(int8_t)4});
    src_column2->append_datum(DatumArray{Datum(), (int8_t)43});

    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column, src_column2});

        ASSERT_TRUE(!dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 6);

        _check_array<int8_t>({(int8_t)(5), (int8_t)(6), (int8_t)(3)}, dest_column->get(0).get_array());
        _check_array<int8_t>({(int8_t)(6), (int8_t)(3), (int8_t)(5)}, dest_column->get(1).get_array());
        ASSERT_TRUE(dest_column->get(2).get_array().empty());
        _check_array<int8_t>({(int8_t)(125), (int8_t)(123)}, dest_column->get(3).get_array());
        ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(5).get_array()[0].get_int8(), (int8_t)(4));
        ASSERT_TRUE(dest_column->get(5).get_array()[1].is_null());
    }
    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column2, src_column});

        ASSERT_TRUE(!dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 6);

        _check_array<int8_t>({(int8_t)(73), (int8_t)(3), (int8_t)(30)}, dest_column->get(0).get_array());
        _check_array<int8_t>({(int8_t)(2), (int8_t)(3), (int8_t)(1)}, dest_column->get(1).get_array());
        ASSERT_TRUE(dest_column->get(2).get_array().empty());
        ASSERT_EQ(dest_column->get(3).get_array()[0].get_int8(), (int8_t)(-43));
        ASSERT_TRUE(dest_column->get(3).get_array()[1].is_null());
        ASSERT_EQ(dest_column->get(4).get_array()[0].get_int8(), (int8_t)(4));
        ASSERT_EQ(dest_column->get(5).get_array()[0].get_int8(), (int8_t)(43));
        ASSERT_TRUE(dest_column->get(5).get_array()[1].is_null());
    }
}

TEST_F(ArrayFunctionsTest, array_sortby_tinyint_with_nullable_notnull) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column->append_datum(DatumArray{(int8_t)3, (int8_t)4, (int8_t)5});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int8_t)2, (int8_t)4});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{});
    src_column->append_datum(DatumArray{Datum(), Datum()});
    src_column->append_datum(DatumArray{(int8_t)43, (int8_t)23});
    src_column->append_datum(DatumArray{(int8_t)43, (int8_t)23, Datum()});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column2->append_datum(DatumArray{(int8_t)82, (int8_t)1, (int8_t)4});
    src_column2->append_datum(DatumArray{(int8_t)23});
    src_column2->append_datum(DatumArray{(int8_t)3, Datum()});
    src_column2->append_datum(DatumArray{Datum()});
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(DatumArray{});
    src_column2->append_datum(DatumArray{Datum(), Datum()});
    src_column2->append_datum(DatumArray{(int8_t)-33, (int8_t)6});
    src_column2->append_datum(DatumArray{(int8_t)3, (int8_t)6, Datum()});
    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column, src_column2});

        ASSERT_TRUE(dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 10);
        _check_array<int8_t>({(int8_t)(4), (int8_t)(5), (int8_t)(3)}, dest_column->get(0).get_array());
        ASSERT_TRUE(dest_column->get(1).is_null());
        _check_array<int8_t>({(int8_t)(4), (int8_t)(2)}, dest_column->get(2).get_array());
        ASSERT_TRUE(dest_column->get(3).is_null());
        ASSERT_TRUE(dest_column->get(4).get_array().empty());
        ASSERT_TRUE(dest_column->get(5).is_null());
        ASSERT_TRUE(dest_column->get(6).get_array().empty());
        ASSERT_TRUE(dest_column->get(7).get_array()[0].is_null());
        ASSERT_TRUE(dest_column->get(7).get_array()[1].is_null());
        _check_array<int8_t>({(int8_t)(43), (int8_t)(23)}, dest_column->get(8).get_array());
        ASSERT_TRUE(dest_column->get(9).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(9).get_array()[1].get_int8(), (int8_t)(43));
        ASSERT_EQ(dest_column->get(9).get_array()[2].get_int8(), (int8_t)(23));
    }
    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column2, src_column});

        ASSERT_TRUE(!dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 10);
        _check_array<int8_t>({(int8_t)(82), (int8_t)(1), (int8_t)(4)}, dest_column->get(0).get_array());
        _check_array<int8_t>({(int8_t)(23)}, dest_column->get(1).get_array());
        ASSERT_EQ(dest_column->get(2).get_array()[0].get_int8(), (int8_t)(3));
        ASSERT_TRUE(dest_column->get(2).get_array()[1].is_null());
        ASSERT_TRUE(dest_column->get(3).get_array()[0].is_null());
        ASSERT_TRUE(dest_column->get(4).get_array().empty());
        ASSERT_TRUE(dest_column->get(5).get_array().empty());
        ASSERT_TRUE(dest_column->get(6).get_array().empty());
        ASSERT_TRUE(dest_column->get(7).get_array()[0].is_null());
        ASSERT_TRUE(dest_column->get(7).get_array()[1].is_null());
        _check_array<int8_t>({(int8_t)(6), (int8_t)(-33)}, dest_column->get(8).get_array());
        ASSERT_TRUE(dest_column->get(9).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(9).get_array()[1].get_int8(), (int8_t)(6));
        ASSERT_EQ(dest_column->get(9).get_array()[2].get_int8(), (int8_t)(3));
    }
}

TEST_F(ArrayFunctionsTest, array_sortby_varchar_with_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4")});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, true);
    src_column2->append_datum(DatumArray{(int8_t)3, (int8_t)5, (int8_t)6});
    src_column2->append_datum(DatumArray{(int8_t)3});
    src_column2->append_datum(Datum());
    src_column2->append_datum(DatumArray{Datum()});
    src_column2->append_datum(DatumArray{(int8_t)4, Datum()});
    src_column2->append_datum(DatumArray{Datum(), (int8_t)4});

    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column, src_column2});

        ASSERT_TRUE(dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 6);
        _check_array<Slice>({Slice("5"), Slice("3"), Slice("6")}, dest_column->get(0).get_array());
        _check_array<Slice>({Slice("8")}, dest_column->get(1).get_array());
        _check_array<Slice>({Slice("4")}, dest_column->get(2).get_array());
        ASSERT_TRUE(dest_column->get(3).is_null());
        _check_array<Slice>({Slice("1"), Slice("4")}, dest_column->get(4).get_array());
        _check_array<Slice>({Slice("4"), Slice("1")}, dest_column->get(5).get_array());
    }
    {
        ArraySortBy<LogicalType::TYPE_VARCHAR> sort;
        auto dest_column = sort.process(nullptr, {src_column2, src_column});

        ASSERT_TRUE(dest_column->is_nullable());
        ASSERT_EQ(dest_column->size(), 6);
        _check_array<int8_t>({(int8_t)(5), (int8_t)(3), (int8_t)(6)}, dest_column->get(0).get_array());
        _check_array<int8_t>({(int8_t)(3)}, dest_column->get(1).get_array());
        ASSERT_TRUE(dest_column->get(2).is_null());
        ASSERT_TRUE(dest_column->get(3).get_array()[0].is_null());
        ASSERT_TRUE(dest_column->get(4).get_array()[0].is_null());
        ASSERT_EQ(dest_column->get(4).get_array()[1].get_int8(), (int8_t)(4));
        ASSERT_EQ(dest_column->get(5).get_array()[0].get_int8(), (int8_t)(4));
        ASSERT_TRUE(dest_column->get(5).get_array()[1].is_null());
    }
}

TEST_F(ArrayFunctionsTest, array_sortby_with_only_null) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
    src_column->append_datum(DatumArray{(int8_t)5, (int8_t)3, (int8_t)6});

    ColumnPtr src_column2 = ColumnHelper::create_const_null_column(1);

    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column, src_column2});
        _check_array<int8_t>({(int8_t)(5), (int8_t)(3), (int8_t)(6)}, dest_column->get(0).get_array());
    }
    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column2, src_column});
        ASSERT_TRUE(dest_column->only_null());
    }

    {
        ArraySortBy<LogicalType::TYPE_TINYINT> sort;
        auto dest_column = sort.process(nullptr, {src_column2, src_column2});
        ASSERT_TRUE(dest_column->only_null());
    }
}

TEST_F(ArrayFunctionsTest, array_generate_with_integer_columns) {
    ColumnPtr start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
    ColumnPtr stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
    ColumnPtr step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);

    start_column->append_datum(Datum((int32_t)3));
    stop_column->append_datum(Datum((int32_t)9));
    step_column->append_datum(Datum((int32_t)2));

    start_column->append_datum(Datum((int32_t)3));
    stop_column->append_datum(Datum((int32_t)9));
    step_column->append_datum(Datum((int32_t)3));

    start_column->append_datum(Datum((int32_t)3));
    stop_column->append_datum(Datum((int32_t)9));
    step_column->append_datum(Datum((int32_t)4));

    start_column->append_datum(Datum((int32_t)9));
    stop_column->append_datum(Datum((int32_t)3));
    step_column->append_datum(Datum((int32_t)-2));

    // if one input is null, then output is null
    start_column->append_datum(Datum());
    stop_column->append_datum(Datum((int32_t)9));
    step_column->append_datum(Datum((int32_t)2));

    start_column->append_datum(Datum((int32_t)10));
    stop_column->append_datum(Datum((int32_t)3));
    step_column->append_datum(Datum((int32_t)6));

    auto dest_column = ArrayGenerate<TYPE_INT>::process(nullptr, {start_column, stop_column, step_column}).value();

    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 6);

    _check_array<int32_t>({(int32_t)(3), (int32_t)(5), (int32_t)(7), (int32_t)(9)}, dest_column->get(0).get_array());
    _check_array<int32_t>({(int32_t)(3), (int32_t)(6), (int32_t)(9)}, dest_column->get(1).get_array());
    _check_array<int32_t>({(int32_t)(3), (int32_t)(7)}, dest_column->get(2).get_array());
    _check_array<int32_t>({(int32_t)(9), (int32_t)(7), (int32_t)(5), (int32_t)(3)}, dest_column->get(3).get_array());
    ASSERT_TRUE(dest_column->is_null(4));
    ASSERT_TRUE(dest_column->get(5).get_array().empty());
}

TEST_F(ArrayFunctionsTest, array_generate_when_overflow) {
    ColumnPtr start_column = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
    ColumnPtr stop_column = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
    ColumnPtr step_column = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);

    start_column->append_datum(Datum((int8_t)9));
    stop_column->append_datum(Datum((int8_t)100));
    step_column->append_datum(Datum((int8_t)88));

    start_column->append_datum(Datum((int8_t)-9));
    stop_column->append_datum(Datum((int8_t)-100));
    step_column->append_datum(Datum((int8_t)-88));

    auto dest_column = ArrayGenerate<TYPE_TINYINT>::process(nullptr, {start_column, stop_column, step_column}).value();

    ASSERT_TRUE(!dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 2);

    _check_array<int8_t>({(int8_t)(9), (int8_t)(97)}, dest_column->get(0).get_array());
    _check_array<int8_t>({(int8_t)(-9), (int8_t)(-97)}, dest_column->get(1).get_array());
}

TEST_F(ArrayFunctionsTest, array_distinct_any_type_only_null) {
    // test only null
    {
        ColumnPtr src_column = ColumnHelper::create_const_null_column(3);
        auto dest_column = ArrayFunctions::array_distinct_any_type(nullptr, {src_column}).value();
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_TRUE(dest_column->only_null());
    }
    // test const
    {
        ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        src_column->append_datum(DatumArray{"5", "5", "33", "666"});
        src_column = ConstColumn::create(src_column, 3);
        auto dest_column = ArrayFunctions::array_distinct_any_type(nullptr, {src_column}).value();
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_STREQ(dest_column->debug_string().c_str(), "[['5','33','666'], ['5','33','666'], ['5','33','666']]");
    }
    // test array[null]
    {
        ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        src_column->append_datum(DatumArray{"5", Datum(), Datum(), "5", "33", "666", Datum()});
        src_column = ConstColumn::create(src_column, 1);
        auto dest_column = ArrayFunctions::array_distinct_any_type(nullptr, {src_column}).value();
        ASSERT_EQ(dest_column->size(), 1);
        ASSERT_STREQ(dest_column->debug_string().c_str(), "[['5',NULL,'33','666']]");
    }
    // test null array
    {
        ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        src_column->append_datum(DatumArray{"5", "5", "33", "666"});
        src_column->append_nulls(2);
        auto dest_column = ArrayFunctions::array_distinct_any_type(nullptr, {src_column}).value();
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_STREQ(dest_column->debug_string().c_str(), "[['5','33','666'], NULL, NULL]");
    }
    // test normal
    {
        ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        src_column->append_datum(DatumArray{"5", "5", "33", "666"});
        auto dest_column = ArrayFunctions::array_distinct_any_type(nullptr, {src_column}).value();
        ASSERT_EQ(dest_column->size(), 1);
        ASSERT_STREQ(dest_column->debug_string().c_str(), "[['5','33','666']]");
    }
}

TEST_F(ArrayFunctionsTest, array_intersect_any_type_int) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{(int32_t)5, (int32_t)3, (int32_t)6});
    src_column->append_datum(DatumArray{(int32_t)8});
    src_column->append_datum(DatumArray{(int32_t)4, (int32_t)1});
    src_column->append_datum(Datum());

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column2->append_datum(DatumArray{(int32_t)(5), (int32_t)(6)});
    src_column2->append_datum(DatumArray{(int32_t)(2)});
    src_column2->append_datum(DatumArray{(int32_t)(4), (int32_t)(3), (int32_t)(2), (int32_t)(1)});
    src_column2->append_datum(DatumArray{(int32_t)(4), (int32_t)(9)});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column3->append_datum(DatumArray{(int32_t)(5)});
    src_column3->append_datum(DatumArray{(int32_t)(2), (int32_t)(8)});
    src_column3->append_datum(DatumArray{(int32_t)(4), (int32_t)(1)});
    src_column3->append_datum(DatumArray{(int32_t)(100)});

    auto dest_column =
            ArrayFunctions::array_intersect_any_type(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<int32_t>({(int32_t)(5)}, dest_column->get(0).get_array());
    _check_array<int32_t>({}, dest_column->get(1).get_array());
    _check_array<int32_t>({(int32_t)(4), (int32_t)(1)}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
}

TEST_F(ArrayFunctionsTest, array_intersect_any_type_int_with_not_null) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
    src_column->append_datum(DatumArray{(int32_t)(5), (int32_t)(3), (int32_t)(6)});
    src_column->append_datum(DatumArray{(int32_t)(8)});
    src_column->append_datum(DatumArray{(int32_t)(4), (int32_t)(1)});
    src_column->append_datum(DatumArray{(int32_t)(4), (int32_t)(22), Datum()});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
    src_column2->append_datum(DatumArray{(int32_t)(5), (int32_t)(6)});
    src_column2->append_datum(DatumArray{(int32_t)(2)});
    src_column2->append_datum(DatumArray{(int32_t)(4), (int32_t)(3), (int32_t)(2), (int32_t)(1)});
    src_column2->append_datum(DatumArray{(int32_t)(4), Datum(), (int32_t)(22), (int32_t)(66)});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
    src_column3->append_datum(DatumArray{(int32_t)(5)});
    src_column3->append_datum(DatumArray{(int32_t)(2), (int32_t)(8)});
    src_column3->append_datum(DatumArray{(int32_t)(4), (int32_t)(1)});
    src_column3->append_datum(DatumArray{(int32_t)(4), Datum(), (int32_t)(2), (int32_t)(22), (int32_t)(1)});

    auto dest_column =
            ArrayFunctions::array_intersect_any_type(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<int32_t>({(int32_t)(5)}, dest_column->get(0).get_array());
    _check_array<int32_t>({}, dest_column->get(1).get_array());

    {
        std::unordered_set<int32_t> set_expect = {4, 1};
        std::unordered_set<int32_t> set_actual;
        auto result_array = dest_column->get(2).get_array();
        for (auto& i : result_array) {
            set_actual.insert(i.get_int32());
        }
        ASSERT_TRUE(set_expect == set_actual);
    }

    {
        std::unordered_set<int32_t> set_expect = {4, 22};
        std::unordered_set<int32_t> set_actual;
        size_t null_values = 0;
        auto result_array = dest_column->get(3).get_array();
        for (auto& i : result_array) {
            if (i.is_null()) {
                ++null_values;
            } else {
                set_actual.insert(i.get_int32());
            }
        }
        ASSERT_TRUE(set_expect == set_actual);
        ASSERT_EQ(null_values, 1);
    }
}

TEST_F(ArrayFunctionsTest, array_intersect_any_type_varchar) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});
    src_column->append_datum(Datum());

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column2->append_datum(DatumArray{Slice("5"), Slice("6")});
    src_column2->append_datum(DatumArray{Slice("2")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("9")});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column3->append_datum(DatumArray{Slice("5")});
    src_column3->append_datum(DatumArray{Slice("2"), Slice("8")});
    src_column3->append_datum(DatumArray{Slice("4"), Slice("1")});
    src_column3->append_datum(DatumArray{Slice("100")});

    auto dest_column =
            ArrayFunctions::array_intersect_any_type(nullptr, {src_column, src_column2, src_column3}).value();

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<Slice>({Slice("5")}, dest_column->get(0).get_array());
    _check_array<Slice>({}, dest_column->get(1).get_array());
    _check_array<Slice>({Slice("4"), Slice("1")}, dest_column->get(2).get_array());
    ASSERT_TRUE(dest_column->get(3).is_null());
}

TEST_F(ArrayFunctionsTest, array_intersect_any_type_varchar_with_not_null) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column->append_datum(DatumArray{Slice("5"), Slice("3"), Slice("6")});
    src_column->append_datum(DatumArray{Slice("8")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("1")});
    src_column->append_datum(DatumArray{Slice("4"), Slice("22"), Datum()});

    ColumnPtr src_column2 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column2->append_datum(DatumArray{Slice("5"), Slice("6")});
    src_column2->append_datum(DatumArray{Slice("2")});
    src_column2->append_datum(DatumArray{Slice("4"), Slice("3"), Slice("2"), Slice("1")});
    src_column2->append_datum(DatumArray{Slice("4"), Datum(), Slice("22"), Slice("66")});

    ColumnPtr src_column3 = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    src_column3->append_datum(DatumArray{Slice("5")});
    src_column3->append_datum(DatumArray{Slice("2"), Slice("8")});
    src_column3->append_datum(DatumArray{Slice("4"), Slice("1")});
    src_column3->append_datum(DatumArray{Slice("4"), Datum(), Slice("2"), Slice("22"), Slice("1")});

    ArrayIntersect<LogicalType::TYPE_VARCHAR> intersect;
    auto dest_column = intersect.process(nullptr, {src_column, src_column2, src_column3});

    ASSERT_EQ(dest_column->size(), 4);
    _check_array<Slice>({Slice("5")}, dest_column->get(0).get_array());
    _check_array<Slice>({}, dest_column->get(1).get_array());

    {
        std::unordered_set<std::string> set_expect = {"4", "1"};
        std::unordered_set<std::string> set_actual;
        auto result_array = dest_column->get(2).get_array();
        for (auto& i : result_array) {
            set_actual.insert(i.get_slice().to_string());
        }
        ASSERT_TRUE(set_expect == set_actual);
    }

    {
        std::unordered_set<std::string> set_expect = {"4", "22"};
        std::unordered_set<std::string> set_actual;
        size_t null_values = 0;
        auto result_array = dest_column->get(3).get_array();
        for (auto& i : result_array) {
            if (i.is_null()) {
                ++null_values;
            } else {
                set_actual.insert(i.get_slice().to_string());
            }
        }
        ASSERT_TRUE(set_expect == set_actual);
        ASSERT_EQ(null_values, 1);
    }
}

TEST_F(ArrayFunctionsTest, array_reverse_any_types_int) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, 3, 6});
    src_column->append_datum(DatumArray{2, 3, 7, 8});
    src_column->append_datum(DatumArray{4, 3, 2, 1});

    ArrayReverse<LogicalType::TYPE_INT> reverse;
    auto dest_column = reverse.process(nullptr, {src_column});

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<int32_t>({6, 3, 5}, dest_column->get(0).get_array());
    _check_array<int32_t>({8, 7, 3, 2}, dest_column->get(1).get_array());
    _check_array<int32_t>({1, 2, 3, 4}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_any_types_string) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    src_column->append_datum(DatumArray{"352", "66", "4325"});
    src_column->append_datum(DatumArray{"235", "99", "8", "43251"});
    src_column->append_datum(DatumArray{"44", "33", "22", "112"});

    auto dest_column = ArrayFunctions::array_reverse_any_types(nullptr, {src_column}).value();

    ASSERT_EQ(dest_column->size(), 3);
    _check_array<Slice>({"4325", "66", "352"}, dest_column->get(0).get_array());
    _check_array<Slice>({"43251", "8", "99", "235"}, dest_column->get(1).get_array());
    _check_array<Slice>({"112", "22", "33", "44"}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_any_types_nullable_elements) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, Datum(), 3, 6});
    src_column->append_datum(DatumArray{2, 3, Datum(), Datum()});
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    auto dest_column = ArrayFunctions::array_reverse_any_types(nullptr, {src_column}).value();

    ASSERT_EQ(dest_column->size(), 3);
    _check_array_nullable<int32_t>({6, 3, 0, 5}, {0, 0, 1, 0}, dest_column->get(0).get_array());
    _check_array_nullable<int32_t>({0, 0, 3, 2}, {1, 1, 0, 0}, dest_column->get(1).get_array());
    _check_array_nullable<int32_t>({0, 0, 0, 0}, {1, 1, 1, 1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_any_types_nullable_array) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
    src_column->append_datum(DatumArray{5, Datum(), 3, 6});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{Datum(), Datum(), Datum(), Datum()});

    auto dest_column = ArrayFunctions::array_reverse_any_types(nullptr, {src_column}).value();

    ASSERT_EQ(dest_column->size(), 3);
    _check_array_nullable<int32_t>({6, 3, 0, 5}, {0, 0, 1, 0}, dest_column->get(0).get_array());
    ASSERT_TRUE(dest_column->get(1).is_null());
    _check_array_nullable<int32_t>({0, 0, 0, 0}, {1, 1, 1, 1}, dest_column->get(2).get_array());
}

TEST_F(ArrayFunctionsTest, array_reverse_any_types_only_null) {
    ColumnPtr src_column = ColumnHelper::create_const_null_column(3);

    auto dest_column = ArrayFunctions::array_reverse_any_types(nullptr, {src_column}).value();

    ASSERT_EQ(dest_column->size(), 3);
    ASSERT_TRUE(dest_column->get(0).is_null());
    ASSERT_TRUE(dest_column->get(1).is_null());
    ASSERT_TRUE(dest_column->get(2).is_null());
}

TEST_F(ArrayFunctionsTest, array_match_nullable) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, true);
    src_column->append_datum(DatumArray{(int8_t)1, (int8_t)1, (int8_t)0});
    src_column->append_datum(DatumArray{(int8_t)0});
    src_column->append_datum(DatumArray{(int8_t)1});
    src_column->append_datum(Datum());
    src_column->append_datum(DatumArray{(int8_t)1, Datum()});
    src_column->append_datum(DatumArray{(int8_t)0, Datum()});
    src_column->append_datum(DatumArray{});

    auto dest_column = ArrayMatch<true>::process(nullptr, {src_column});
    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 7);
    ASSERT_TRUE(dest_column->get(0).get_int8());
    ASSERT_FALSE(dest_column->get(1).get_int8());
    ASSERT_TRUE(dest_column->get(2).get_int8());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_int8());
    ASSERT_TRUE(dest_column->get(5).is_null());
    ASSERT_FALSE(dest_column->get(6).get_int8());

    dest_column = ArrayMatch<false>::process(nullptr, {src_column});
    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 7);
    ASSERT_FALSE(dest_column->get(0).get_int8());
    ASSERT_FALSE(dest_column->get(1).get_int8());
    ASSERT_TRUE(dest_column->get(2).get_int8());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).is_null());
    ASSERT_FALSE(dest_column->get(5).get_int8());
    ASSERT_TRUE(dest_column->get(6).get_int8());
}

TEST_F(ArrayFunctionsTest, array_match_not_null) {
    ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
    src_column->append_datum(DatumArray{(int8_t)1, (int8_t)1, (int8_t)0});
    src_column->append_datum(DatumArray{(int8_t)0});
    src_column->append_datum(DatumArray{(int8_t)1});
    src_column->append_datum(DatumArray{Datum()});
    src_column->append_datum(DatumArray{(int8_t)1, Datum()});
    src_column->append_datum(DatumArray{(int8_t)0, Datum()});
    src_column->append_datum(DatumArray{});

    auto dest_column = ArrayMatch<true>::process(nullptr, {src_column});
    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 7);
    ASSERT_TRUE(dest_column->get(0).get_int8());
    ASSERT_FALSE(dest_column->get(1).get_int8());
    ASSERT_TRUE(dest_column->get(2).get_int8());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).get_int8());
    ASSERT_TRUE(dest_column->get(5).is_null());
    ASSERT_FALSE(dest_column->get(6).get_int8());

    dest_column = ArrayMatch<false>::process(nullptr, {src_column});
    ASSERT_TRUE(dest_column->is_nullable());
    ASSERT_EQ(dest_column->size(), 7);
    ASSERT_FALSE(dest_column->get(0).get_int8());
    ASSERT_FALSE(dest_column->get(1).get_int8());
    ASSERT_TRUE(dest_column->get(2).get_int8());
    ASSERT_TRUE(dest_column->get(3).is_null());
    ASSERT_TRUE(dest_column->get(4).is_null());
    ASSERT_FALSE(dest_column->get(5).get_int8());
    ASSERT_TRUE(dest_column->get(6).get_int8());
}

TEST_F(ArrayFunctionsTest, array_match_only_null) {
    // test only null
    {
        ColumnPtr src_column = ColumnHelper::create_const_null_column(3);
        auto dest_column = ArrayMatch<false>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_TRUE(dest_column->only_null());

        dest_column = ArrayMatch<true>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_TRUE(dest_column->only_null());
    }
    // test const
    {
        ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        src_column->append_datum(DatumArray{(uint8) false, (uint8) true});
        src_column = ConstColumn::create(src_column, 3);
        auto dest_column = ArrayMatch<false>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_FALSE(dest_column->get(0).get_int8());

        dest_column = ArrayMatch<true>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_TRUE(dest_column->get(0).get_int8());
    }
    // test const
    {
        ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        src_column->append_datum(DatumArray{});
        src_column = ConstColumn::create(src_column, 3);
        auto dest_column = ArrayMatch<true>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_FALSE(dest_column->get(0).get_int8());

        dest_column = ArrayMatch<false>::process(nullptr, {src_column});
        ASSERT_EQ(dest_column->size(), 3);
        ASSERT_TRUE(dest_column->get(0).get_int8());
    }
}
// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_seq) {
    // array_contains_seq(["a", "b", "c"], ["c"])         -> 1
    // array_contains_seq(NULL, ["c"])                    -> NULL
    // array_contains_seq(["a", "b", "c"], NULL)          -> NULL
    // array_contains_seq(["a", "b", NULL], NULL)         -> NULL
    // array_contains_seq(["a", "b", NULL], ["a", NULL])  -> 0
    // array_contains_seq(NULL, ["a", NULL])              -> NULL
    // array_contains_seq(["a", "b", NULL], [NULL])       -> 1
    // array_contains_seq(["a", "b", "c"], ["d"])         -> 0
    // array_contains_seq(["a", "b", "c"], ["a", "d"])    -> 0
    // array_contains_all(["a", "b", "c"], ["a", "c"])    -> 0
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{"a", "b", "c"});
        array->append_datum(Datum());
        array->append_datum(DatumArray{"a", "b", "c"});
        array->append_datum(DatumArray{"a", "b", Datum()});
        array->append_datum(DatumArray{"a", "b", Datum()});
        array->append_datum(Datum());
        array->append_datum(DatumArray{"a", "b", Datum()});
        array->append_datum(DatumArray{"a", "b", "c"});
        array->append_datum(DatumArray{"a", "b", "c"});
        array->append_datum(DatumArray{"a", "b", "c"});

        ColumnPtr target = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});
        target->append_datum(Datum());
        target->append_datum(Datum());
        target->append_datum(DatumArray{"a", Datum()});
        target->append_datum(DatumArray{"a", Datum()});
        target->append_datum(DatumArray{Datum()});
        target->append_datum(DatumArray{"d"});
        target->append_datum(DatumArray{"a", "d"});
        target->append_datum(DatumArray{"a", "c"});
        FunctionContext ctx;
        auto result = ArrayFunctions::array_contains_seq_specific<TYPE_VARCHAR>(&ctx, {array, target}).value();
        EXPECT_EQ(10, result->size());
        EXPECT_EQ(1, result->get(0).get_int8());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
        EXPECT_TRUE(result->get(3).is_null());
        EXPECT_EQ(0, result->get(4).get_int8());
        EXPECT_TRUE(result->get(5).is_null());
        EXPECT_EQ(1, result->get(6).get_int8());
        EXPECT_EQ(0, result->get(7).get_int8());
        EXPECT_EQ(0, result->get(8).get_int8());
        EXPECT_EQ(0, result->get(9).get_int8());
    }

    // array_contains_seq([["a"], ["b"]], [["c"]])
    // array_contains_seq(["a","c"], [["c"]])
    // array_contains_seq([["a", "b"], ["c"]], [["a", "b"]])
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum(DatumArray{"a"}), Datum(DatumArray{"b"})});
        array->append_datum(DatumArray{Datum(DatumArray{"a", "c"})});
        array->append_datum(DatumArray{Datum(DatumArray{"a", "b"}), Datum(DatumArray{"c"})});

        ColumnPtr target = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        target->append_datum(DatumArray{Datum(DatumArray{"c"})});
        target->append_datum(DatumArray{Datum(DatumArray{"c"})});
        target->append_datum(DatumArray{Datum(DatumArray{"a", "b"})});

        auto result = ArrayFunctions::array_contains_seq(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
    // array_contains_seq([["a"], ["b"], [NULL]], [["c"]])
    // array_contains_seq([["a","d","c"]], [["e"]])
    // array_contains_seq([["a", "b"], ["c"]], [["a", "b"]])
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{Datum(DatumArray{"a"}), Datum(DatumArray{"b"}), Datum()});
        array->append_datum(DatumArray{Datum(DatumArray{"a", "d", "c"})});
        array->append_datum(DatumArray{Datum(DatumArray{"a", "b"}), Datum(DatumArray{"c"})});

        ColumnPtr target = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        target->append_datum(DatumArray{Datum(DatumArray{"c"})});
        target->append_datum(DatumArray{Datum(DatumArray{"e"})});
        target->append_datum(DatumArray{Datum(DatumArray{"a", "b"})});

        auto result = ArrayFunctions::array_contains_seq(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
    // array_contains_seq([["a"], ["b"]], [["c"], [NULL]])
    // array_contains_seq([["a","d","c"]], [["e", NULL]])
    // array_contains_seq([["a", "b"], ["c"]], [["a", "b"]])
    {
        ColumnPtr array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum(DatumArray{"a"}), Datum(DatumArray{"b"})});
        array->append_datum(DatumArray{Datum(DatumArray{"a", "d", "c"})});
        array->append_datum(DatumArray{Datum(DatumArray{"a", "b"}), Datum(DatumArray{"c"})});

        ColumnPtr target = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        target->append_datum(DatumArray{Datum(DatumArray{"c"}), Datum()});
        target->append_datum(DatumArray{Datum(DatumArray{"e"}), Datum()});
        target->append_datum(DatumArray{Datum(DatumArray{"a", "b"})});

        auto result = ArrayFunctions::array_contains_seq(nullptr, {array, target}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_EQ(0, result->get(1).get_int8());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
}

template <LogicalType Type>
void array_repeat_test(Datum element_0, Datum element_1, Datum element_2, Datum element_null) {
    {
        using CppType = RunTimeCppType<Type>;

        int32_t repeat_count_0 = (int32_t)1;
        int32_t repeat_count_1 = (int32_t)-2;
        int32_t repeat_count_2 = (int32_t)3;
        Datum repeat_count_null;

        // The normal case
        {
            ColumnPtr src_column = ColumnHelper::create_column(TypeDescriptor(Type), false, false, 0);
            src_column->append_datum(element_0);
            src_column->append_datum(element_1);
            src_column->append_datum(element_2);

            Int32Column::Ptr repeat_count_column = Int32Column::create();
            repeat_count_column->append(repeat_count_0);
            repeat_count_column->append(repeat_count_1);
            repeat_count_column->append(repeat_count_2);

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), 3);
            ASSERT_EQ(dest_column->get(0).get_array().size(), 1);
            if (Type == TYPE_JSON) {
                ASSERT_EQ(element_0.get_json()->get_slice(),
                          dest_column->get(0).get_array()[0].get_json()->get_slice());
                ASSERT_EQ(dest_column->get(1).get_array().size(), 0);
                ASSERT_EQ(dest_column->get(2).get_array().size(), 3);
                ASSERT_EQ(element_2.get_json()->get_slice(),
                          dest_column->get(2).get_array()[0].get_json()->get_slice());
                ASSERT_EQ(element_2.get_json()->get_slice(),
                          dest_column->get(2).get_array()[1].get_json()->get_slice());
                ASSERT_EQ(element_2.get_json()->get_slice(),
                          dest_column->get(2).get_array()[2].get_json()->get_slice());
            } else {
                ASSERT_EQ(element_0.get<CppType>(), dest_column->get(0).get_array()[0].get<CppType>());
                ASSERT_EQ(dest_column->get(1).get_array().size(), 0);
                ASSERT_EQ(dest_column->get(2).get_array().size(), 3);
                ASSERT_EQ(element_2.get<CppType>(), dest_column->get(2).get_array()[0].get<CppType>());
                ASSERT_EQ(element_2.get<CppType>(), dest_column->get(2).get_array()[1].get<CppType>());
                ASSERT_EQ(element_2.get<CppType>(), dest_column->get(2).get_array()[2].get<CppType>());
            }
        }

        // The case for testing NullableColumn
        {
            ColumnPtr src_column = ColumnHelper::create_column(TypeDescriptor(Type), true, false, 0);
            src_column->append_datum(element_0);
            src_column->append_datum(element_1);
            src_column->append_datum(element_null);

            NullableColumn::Ptr repeat_count_column =
                    NullableColumn::create(Int32Column::create(), NullColumn::create(0, DATUM_NULL));
            repeat_count_column->append_datum(repeat_count_null);
            repeat_count_column->append_datum(Datum(repeat_count_1));
            repeat_count_column->append_datum(Datum(repeat_count_2));

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), 3);
            ASSERT_TRUE(dest_column->get(0).is_null());
            ASSERT_EQ(dest_column->get(1).get_array().size(), 0);
            ASSERT_EQ(dest_column->get(2).get_array().size(), 3);
            ASSERT_TRUE(dest_column->get(2).get_array()[0].is_null());
            ASSERT_TRUE(dest_column->get(2).get_array()[1].is_null());
            ASSERT_TRUE(dest_column->get(2).get_array()[2].is_null());
        }

        // The case for testing ConstColumn
        {
            size_t const_column_row_count = 2;

            ColumnPtr src_column = ColumnHelper::create_column(TypeDescriptor(Type), false, true, 0);
            for (int i = 0; i < const_column_row_count; i++) {
                src_column->append_datum(element_0);
            }

            Int32Column::Ptr repeat_count_data_column = Int32Column::create();
            repeat_count_data_column->append(repeat_count_0);
            ConstColumn::Ptr repeat_count_column =
                    ConstColumn::create(std::move(repeat_count_data_column), const_column_row_count);

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), const_column_row_count);
            if (Type == TYPE_JSON) {
                ASSERT_EQ(element_0.get_json()->get_slice(),
                          dest_column->get(0).get_array()[0].get_json()->get_slice());
                ASSERT_EQ(element_0.get_json()->get_slice(),
                          dest_column->get(1).get_array()[0].get_json()->get_slice());
            } else {
                ASSERT_EQ(element_0.get<CppType>(), dest_column->get(0).get_array()[0].get<CppType>());
                ASSERT_EQ(element_0.get<CppType>(), dest_column->get(1).get_array()[0].get<CppType>());
            }
        }
    }
}

TEST_F(ArrayFunctionsTest, array_repeat) {
    {
        array_repeat_test<TYPE_INT>(Datum((int32_t)0), Datum((int32_t)1), Datum((int32_t)2), Datum());
        array_repeat_test<TYPE_BIGINT>(Datum((int64_t)0), Datum((int64_t)1), Datum((int64_t)2), Datum());
        array_repeat_test<TYPE_FLOAT>(Datum((float)0), Datum((float)0.1), Datum((float)0.2), Datum());
        array_repeat_test<TYPE_DOUBLE>(Datum((double)0), Datum((double)0.1), Datum((double)0.2), Datum());
        array_repeat_test<TYPE_DECIMALV2>(Datum(DecimalV2Value(std::string("0.0000000000"))),
                                          Datum(DecimalV2Value(std::string("1.0000000000"))),
                                          Datum(DecimalV2Value(std::string("2.0000000000"))), Datum());
        array_repeat_test<TYPE_BOOLEAN>(Datum(true), (false), Datum(false), Datum());
        array_repeat_test<TYPE_DATE>(DateValue::create(2020, 0, 0), DateValue::create(2021, 1, 1),
                                     DateValue::create(2022, 2, 2), Datum());
        array_repeat_test<TYPE_DATETIME>(TimestampValue::create(2020, 0, 0, 0, 0, 0),
                                         TimestampValue::create(2021, 1, 1, 1, 1, 1),
                                         TimestampValue::create(2022, 2, 2, 2, 2, 2), Datum());
        array_repeat_test<TYPE_VARCHAR>(Datum(Slice("0")), Datum(Slice("1")), Datum(Slice("2")), Datum());
        JsonValue json_element_0 = JsonValue::parse("{\"a\": 0}").value();
        JsonValue json_element_1 = JsonValue::parse("{\"b\": 1}").value();
        JsonValue json_element_2 = JsonValue::parse("{\"c\": 2}").value();
        array_repeat_test<TYPE_JSON>(Datum(&json_element_0), Datum(&json_element_1), Datum(&json_element_2), Datum());
    }
}

TEST_F(ArrayFunctionsTest, array_repeat_array) {
    {
        Datum element_0 = DatumArray{(int32_t)0};
        Datum element_1 = DatumArray{Datum()};
        Datum element_2 = DatumArray{Datum(), (int32_t)2};
        Datum element_null;
        int32_t repeat_count_0 = (int32_t)1;
        int32_t repeat_count_1 = (int32_t)-2;
        int32_t repeat_count_2 = (int32_t)3;
        Datum repeat_count_null;

        // The normal case
        {
            ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
            src_column->append_datum(element_0);
            src_column->append_datum(element_1);
            src_column->append_datum(element_2);

            Int32Column::Ptr repeat_count_column = Int32Column::create();
            repeat_count_column->append(repeat_count_0);
            repeat_count_column->append(repeat_count_1);
            repeat_count_column->append(repeat_count_2);

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), 3);
            ASSERT_EQ(dest_column->get(0).get_array().size(), 1);
            _check_array<int32_t>({(int32_t)0}, dest_column->get(0).get_array()[0].get_array());
            ASSERT_EQ(dest_column->get(1).get_array().size(), 0);
            _check_array<int32_t>({}, dest_column->get(1).get_array());
            ASSERT_EQ(dest_column->get(2).get_array().size(), 3);
            ASSERT_TRUE(dest_column->get(2).get_array()[0].get_array()[0].is_null());
            ASSERT_EQ((int32_t)2, dest_column->get(2).get_array()[0].get_array()[1].get_int32());
            ASSERT_TRUE(dest_column->get(2).get_array()[1].get_array()[0].is_null());
            ASSERT_EQ((int32_t)2, dest_column->get(2).get_array()[1].get_array()[1].get_int32());
            ASSERT_TRUE(dest_column->get(2).get_array()[2].get_array()[0].is_null());
            ASSERT_EQ((int32_t)2, dest_column->get(2).get_array()[2].get_array()[1].get_int32());
        }

        // The case for testing NullableColumn
        {
            ColumnPtr src_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
            src_column->append_datum(element_0);
            src_column->append_datum(element_1);
            src_column->append_datum(element_null);

            NullableColumn::Ptr repeat_count_column =
                    NullableColumn::create(Int32Column::create(), NullColumn::create(0, DATUM_NULL));
            repeat_count_column->append_datum(repeat_count_null);
            repeat_count_column->append_datum(Datum(repeat_count_1));
            repeat_count_column->append_datum(Datum(repeat_count_2));

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), 3);
            ASSERT_TRUE(dest_column->get(0).is_null());
            ASSERT_EQ(dest_column->get(1).get_array().size(), 0);
            _check_array<int32_t>({}, dest_column->get(1).get_array());
            ASSERT_EQ(dest_column->get(2).get_array().size(), 3);
            ASSERT_TRUE(dest_column->get(2).get_array()[0].is_null());
            ASSERT_TRUE(dest_column->get(2).get_array()[1].is_null());
            ASSERT_TRUE(dest_column->get(2).get_array()[2].is_null());
        }

        // The case for testing ConstColumn
        {
            size_t const_column_row_count = 2;

            ColumnPtr src_data_column = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
            src_data_column->append_datum(element_0);
            ConstColumn::Ptr src_column = ConstColumn::create(std::move(src_data_column), const_column_row_count);

            Int32Column::Ptr repeat_count_data_column = Int32Column::create();
            repeat_count_data_column->append(repeat_count_0);
            ConstColumn::Ptr repeat_count_column =
                    ConstColumn::create(std::move(repeat_count_data_column), const_column_row_count);

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), const_column_row_count);
            _check_array<int32_t>({(int32_t)0}, dest_column->get(0).get_array()[0].get_array());
        }
    }
}

TEST_F(ArrayFunctionsTest, array_repeat_map) {
    {
        DatumMap element_0;
        DatumMap element_1;
        DatumMap element_2;
        Datum element_null;
        int32_t repeat_count_0 = (int32_t)1;
        int32_t repeat_count_1 = (int32_t)-2;
        int32_t repeat_count_2 = (int32_t)3;
        Datum repeat_count_null;
        element_0[(int32_t)0] = (int32_t)0;
        element_0[(int32_t)1] = (int32_t)11;
        element_0[(int32_t)2] = (int32_t)22;
        element_1[(int32_t)3] = (int32_t)33;
        element_1[(int32_t)4] = (int32_t)44;
        element_1[(int32_t)5] = (int32_t)55;
        element_2[(int32_t)6] = (int32_t)66;
        element_2[(int32_t)7] = (int32_t)77;
        element_2[(int32_t)8] = (int32_t)88;

        // The normal case
        {
            UInt32Column::Ptr offsets = UInt32Column::create();
            Int32Column::Ptr keys_data = Int32Column::create();
            NullColumn::Ptr keys_null = NullColumn::create();
            NullableColumn::Ptr keys = NullableColumn::create(keys_data, keys_null);
            Int32Column::Ptr values_data = Int32Column::create();
            NullColumn::Ptr values_null = NullColumn::create();
            NullableColumn::Ptr values = NullableColumn::create(values_data, values_null);
            MapColumn::Ptr src_column = MapColumn::create(keys, values, offsets);
            src_column->append_datum(element_0);
            src_column->append_datum(element_1);
            src_column->append_datum(element_2);

            Int32Column::Ptr repeat_count_column = Int32Column::create();
            repeat_count_column->append(repeat_count_0);
            repeat_count_column->append(repeat_count_1);
            repeat_count_column->append(repeat_count_2);

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), 3);
            ASSERT_EQ(dest_column->get(0).get_array().size(), repeat_count_0);
            ASSERT_EQ(element_0.find(2)->second.get_int32(),
                      dest_column->get(0).get_array()[0].get<DatumMap>().find(2)->second.get_int32());
            ASSERT_EQ(dest_column->get(1).get_array().size(), 0);
            _check_array<int32_t>({}, dest_column->get(1).get_array());
            ASSERT_EQ(dest_column->get(2).get_array().size(), repeat_count_2);
            ASSERT_EQ(element_2.find(8)->second.get_int32(),
                      dest_column->get(2).get_array()[2].get<DatumMap>().find(8)->second.get_int32());
        }

        // The case for testing NullableColumn
        {
            UInt32Column::Ptr offsets = UInt32Column::create();
            Int32Column::Ptr keys_data = Int32Column::create();
            NullColumn::Ptr keys_null = NullColumn::create();
            NullableColumn::Ptr keys = NullableColumn::create(keys_data, keys_null);
            Int32Column::Ptr values_data = Int32Column::create();
            NullColumn::Ptr values_null = NullColumn::create();
            NullableColumn::Ptr values = NullableColumn::create(values_data, values_null);
            MapColumn::Ptr map_column = MapColumn::create(keys, values, offsets);
            NullableColumn::Ptr src_column =
                    NullableColumn::create(std::move(map_column), NullColumn::create(0, DATUM_NULL));
            src_column->append_datum(element_0);
            src_column->append_datum(element_1);
            src_column->append_datum(element_null);

            NullableColumn::Ptr count_column =
                    NullableColumn::create(Int32Column::create(), NullColumn::create(0, DATUM_NULL));
            count_column->append_datum(repeat_count_null);
            count_column->append_datum(Datum(repeat_count_1));
            count_column->append_datum(Datum(repeat_count_2));

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, count_column}).value();
            ASSERT_EQ(dest_column->size(), 3);
            ASSERT_TRUE(dest_column->get(0).is_null());
            ASSERT_EQ(dest_column->get(1).get_array().size(), 0);
            _check_array<int32_t>({}, dest_column->get(1).get_array());
            ASSERT_EQ(dest_column->get(2).get_array().size(), 3);
            ASSERT_TRUE(dest_column->get(2).get_array()[0].is_null());
            ASSERT_TRUE(dest_column->get(2).get_array()[1].is_null());
            ASSERT_TRUE(dest_column->get(2).get_array()[2].is_null());
        }

        // The case for testing ConstColumn
        {
            size_t const_column_row_count = 2;
            UInt32Column::Ptr offsets = UInt32Column::create();
            Int32Column::Ptr keys_data = Int32Column::create();
            NullColumn::Ptr keys_null = NullColumn::create();
            NullableColumn::Ptr keys = NullableColumn::create(keys_data, keys_null);
            Int32Column::Ptr values_data = Int32Column::create();
            NullColumn::Ptr values_null = NullColumn::create();
            NullableColumn::Ptr values = NullableColumn::create(values_data, values_null);
            MapColumn::Ptr map_column = MapColumn::create(keys, values, offsets);
            map_column->append_datum(element_0);
            ConstColumn::Ptr src_column = ConstColumn::create(std::move(map_column), const_column_row_count);

            Int32Column::Ptr repeat_count_data_column = Int32Column::create();
            repeat_count_data_column->append(repeat_count_0);
            ConstColumn::Ptr repeat_count_column =
                    ConstColumn::create(std::move(repeat_count_data_column), const_column_row_count);

            auto dest_column = ArrayFunctions::repeat(nullptr, {src_column, repeat_count_column}).value();
            ASSERT_EQ(dest_column->size(), const_column_row_count);
            ASSERT_EQ(element_0.find(2)->second.get_int32(),
                      dest_column->get(0).get_array()[0].get<DatumMap>().find(2)->second.get_int32());
            ASSERT_EQ(element_0.find(2)->second.get_int32(),
                      dest_column->get(1).get_array()[0].get<DatumMap>().find(2)->second.get_int32());
        }
    }
}

TEST_F(ArrayFunctionsTest, array_flatten_int) {
    // array_flatten(NULL): NULL
    // array_flatten([[1, 2], [1, 4]]): [1,2,1,4]
    // array_flatten([[1, 2], [3]]): [1,2,3]
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, true);
        array->append_nulls(1);
        array->append_datum(DatumArray{DatumArray{1, 2}, DatumArray{1, 4}});
        array->append_datum(DatumArray{DatumArray{1, 2}, DatumArray{3}});

        auto result = ArrayFunctions::array_flatten(nullptr, {std::move(array)}).value();
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->get(0).is_null());
        EXPECT_EQ("[1,2,1,4]", result->debug_item(1));
        EXPECT_EQ("[1,2,3]", result->debug_item(2));
    }
}
} // namespace starrocks
