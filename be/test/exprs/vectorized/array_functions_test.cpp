// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/array_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

namespace starrocks::vectorized {

namespace {
TypeDescriptor array_type(const TypeDescriptor& child_type) {
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.children.emplace_back(child_type);
    return t;
}

TypeDescriptor array_type(const PrimitiveType& child_type) {
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.children.resize(1);
    t.children[0].type = child_type;
    t.children[0].len = child_type == TYPE_VARCHAR ? 10 : child_type == TYPE_CHAR ? 10 : -1;
    return t;
}
} // namespace

class ArrayFunctionsTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}

    TypeDescriptor TYPE_ARRAY_BOOLEAN = array_type(TYPE_BOOLEAN);
    TypeDescriptor TYPE_ARRAY_TINYINT = array_type(TYPE_TINYINT);
    TypeDescriptor TYPE_ARRAY_SMALLINT = array_type(TYPE_SMALLINT);
    TypeDescriptor TYPE_ARRAY_INT = array_type(TYPE_INT);
    TypeDescriptor TYPE_ARRAY_VARCHAR = array_type(TYPE_VARCHAR);
    TypeDescriptor TYPE_ARRAY_ARRAY_INT = array_type(array_type(TYPE_INT));
    TypeDescriptor TYPE_ARRAY_ARRAY_VARCHAR = array_type(array_type(TYPE_VARCHAR));

    TypeDescriptor TYPE_ARRAY_BIGINT = array_type(TYPE_BIGINT);
    TypeDescriptor TYPE_ARRAY_LARGEINT = array_type(TYPE_LARGEINT);
    TypeDescriptor TYPE_ARRAY_DATE = array_type(TYPE_DATE);
    TypeDescriptor TYPE_ARRAY_DATETIME = array_type(TYPE_DATETIME);
};

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_length) {
    // []
    // NULL
    // [NULL]
    // [1]
    // [1, 2]
    {
        auto c = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        c->append_datum(Datum(DatumArray{}));
        c->append_datum(Datum());
        c->append_datum(Datum(DatumArray{Datum()}));
        c->append_datum(Datum(DatumArray{Datum((int32_t)1)}));
        c->append_datum(Datum(DatumArray{Datum((int32_t)1), Datum((int32_t)2)}));

        auto result = ArrayFunctions::array_length(nullptr, {c});
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
    // ["a"]
    // ["a", "b"]
    {
        auto c = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        c->append_datum(Datum(DatumArray{}));
        c->append_datum(Datum());
        c->append_datum(Datum(DatumArray{Datum()}));
        c->append_datum(Datum(DatumArray{Datum("a")}));
        c->append_datum(Datum(DatumArray{Datum("a"), Datum("b")}));

        auto result = ArrayFunctions::array_length(nullptr, {c});
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
        auto c = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, true);
        c->append_datum(Datum(DatumArray{}));
        c->append_datum(Datum());
        c->append_datum(Datum(DatumArray{Datum()}));
        c->append_datum(Datum(DatumArray{DatumArray{Datum()}}));
        c->append_datum(Datum(DatumArray{DatumArray{}}));
        c->append_datum(Datum(DatumArray{Datum(DatumArray{}), Datum(DatumArray{})}));
        c->append_datum(Datum(DatumArray{Datum(DatumArray{Datum((int32_t)1)}), Datum(DatumArray{Datum((int32_t)2)}),
                                         Datum(DatumArray{Datum((int32_t)3)})}));

        auto result = ArrayFunctions::array_length(nullptr, {c});
        EXPECT_EQ(7, result->size());

        ASSERT_FALSE(result->get(0).is_null());
        ASSERT_TRUE(result->get(1).is_null());
        ASSERT_FALSE(result->get(2).is_null());
        ASSERT_FALSE(result->get(3).is_null());
        ASSERT_FALSE(result->get(4).is_null());
        ASSERT_FALSE(result->get(5).is_null());
        ASSERT_FALSE(result->get(6).is_null());

        EXPECT_EQ(0, result->get(0).get_int32());
        ASSERT_TRUE(result->get(1).is_null());
        EXPECT_EQ(1, result->get(2).get_int32());
        EXPECT_EQ(1, result->get(3).get_int32());
        EXPECT_EQ(1, result->get(4).get_int32());
        EXPECT_EQ(2, result->get(5).get_int32());
        EXPECT_EQ(3, result->get(6).get_int32());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_contains_empty_array) {
    // array_contains([], 1)
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 1);
        target->append_datum(Datum{(int32_t)1});

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // array_contains([], "abc")
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));

        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 1);
        target->append_datum(Datum{"abc"});

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // array_contains(ARRAY<ARRAY<int>>[], [1])
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), false);
        target->append_datum(Datum(DatumArray{Datum{(int32_t)1}}));

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // array_contains(ARRAY<ARRAY<int>>[], ARRAY<int>[])
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), false);
        target->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // multiple lines with const target:
    //  array_contains([], 1);
    //  array_contains([], 1);
    //  array_contains([], 1);
    //  array_contains([], 1);
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 1);
        DCHECK(target->is_constant());
        target->append_datum(Datum((int32_t)1));
        target->resize(4);

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        target->append_datum(Datum((int32_t)1));
        target->append_datum(Datum((int32_t)2));
        target->append_datum(Datum{});
        target->append_datum(Datum((int32_t)3));

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto target = ColumnHelper::create_const_null_column(1);
        target->resize(4);

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
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
        result = ArrayFunctions::array_contains(nullptr, {array, target});
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
        result = ArrayFunctions::array_contains(nullptr, {array, target});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});

        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), false);
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});
        target->append_datum(Datum{(int8_t)0});
        target->append_datum(Datum{(int8_t)1});

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});

        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false, true, 1);
        target->append_datum(Datum{3});
        target->resize(5);

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{DatumArray{}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});
        array->append_datum(DatumArray{DatumArray{"d", "o"}, DatumArray{"r"}, DatumArray{"i", "s"}});

        auto target = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
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

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum{}});
        array->append_datum(DatumArray{"abc", Datum{}});
        array->append_datum(DatumArray{Datum{}, "abc"});

        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 1);
        target->append_datum(Datum{"abc"});
        target->append_datum(Datum{"abc"});
        target->append_datum(Datum{"abc"});

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{"abc", "def"});

        // const-null column.
        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 1);

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
    }
    // array_contains(ARRAY<TINYINT>[1, 2, 3], 2)
    // array_contains(ARRAY<TINYINT>[1, 2, 3], 4)
    // array_contains(ARRAY<TINYINT>[1, 2, 3], NULL)
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});
        array->append_datum(DatumArray{(int8_t)1, (int8_t)2, (int8_t)3});

        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_TINYINT), true);
        target->append_datum(Datum((int8_t)2));
        target->append_datum(Datum((int8_t)4));
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), "abc"});

        // const-null column.
        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 1);

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, false);
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{Datum(), DatumArray{1, 2}});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});
        array->append_datum(DatumArray{DatumArray{1, 2}, Datum()});

        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_INT), true);
        target->append_datum(Datum());
        target->append_datum(Datum());
        target->append_datum(DatumArray{1, 2});
        target->append_datum(DatumArray{1, 2});
        target->append_datum(Datum());

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{"a", "b"});
        array->append_datum(Datum());
        array->append_datum(DatumArray{"a", "b", "c"});

        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 1);
        target->append_datum(Datum("c"));
        target->append_datum(Datum("c"));
        target->append_datum(Datum("c"));

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
    // array_contains([["a"], ["b"]], ["c"])
    // array_contains(NULL, ["c"])
    // array_contains([["a", "b"], ["c"]], ["c"])
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{DatumArray{"a"}, DatumArray{"b"}});
        array->append_datum(Datum());
        array->append_datum(DatumArray{DatumArray{"a", "b"}, DatumArray{"c"}});

        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_VARCHAR), false);
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});
        target->append_datum(DatumArray{"c"});

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(0, result->get(0).get_int8());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_EQ(1, result->get(2).get_int8());
    }
    // array_contains(NULL, NULL)
    // array_contains(NULL, ["a"])
    // array_contains(NULL, [NULL])
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_VARCHAR, true);
        array->append_datum(Datum());
        array->append_datum(Datum());
        array->append_datum(Datum());

        auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_ARRAY_VARCHAR), true);
        target->append_datum(Datum());
        target->append_datum(DatumArray{"a"});
        target->append_datum(DatumArray{Datum()});

        auto result = ArrayFunctions::array_contains(nullptr, {array, target});
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->get(0).is_null());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_TRUE(result->get(2).is_null());
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayFunctionsTest, array_append) {
    // array_append([], NULL)
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));

        auto null = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true, true, 1);

        auto result = ArrayFunctions::array_append(nullptr, {array, null});
        EXPECT_EQ(1, result->size());
        EXPECT_EQ(1, result->get(0).get_array().size());
        EXPECT_TRUE(result->get(0).get_array()[0].is_null());
    }
    // array_append(['abc'], 'def')
    // array_append(['xyz', 'xxx'], 'def')
    // array_append([], 'def')
    // array_append(NULL, 'def')
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
        array->append_datum(DatumArray{"abc"});
        array->append_datum(DatumArray{"xyz", "xxx"});
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum());

        auto data = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false, true, 1);
        data->append_datum("def");

        auto result = ArrayFunctions::array_append(nullptr, {array, data});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_ARRAY_INT, true);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{DatumArray{0, 1}});
        array->append_datum(DatumArray{Datum()});
        array->append_datum(Datum());
        array->append_datum(DatumArray{DatumArray{10, 11}, DatumArray{12, 13}});

        auto data = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        data->append_datum(Datum(DatumArray{}));
        data->append_datum(DatumArray{2});
        data->append_datum(DatumArray{3, 4});
        data->append_datum(Datum());
        data->append_datum(DatumArray{14, 15});

        auto result = ArrayFunctions::array_append(nullptr, {array, data});
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
}

TEST_F(ArrayFunctionsTest, array_sum_empty_array) {
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum_int(nullptr, {array});
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum_boolean(nullptr, {array});
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum_int(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum_tinyint(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_sum_int(nullptr, {array});
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

        result = ArrayFunctions::array_sum_boolean(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

TEST_F(ArrayFunctionsTest, array_avg_empty_array) {
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg_int(nullptr, {array});
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg_boolean(nullptr, {array});
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg_int(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg_tinyint(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_avg_int(nullptr, {array});
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

        result = ArrayFunctions::array_avg_boolean(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

TEST_F(ArrayFunctionsTest, array_min_empty_array) {
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min_int(nullptr, {array});
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min_boolean(nullptr, {array});
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min_int(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min_tinyint(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_min_int(nullptr, {array});
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

        result = ArrayFunctions::array_min_boolean(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

TEST_F(ArrayFunctionsTest, array_max_empty_array) {
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max_int(nullptr, {array});
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max_boolean(nullptr, {array});
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max_int(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max_tinyint(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));

        auto result = ArrayFunctions::array_max_int(nullptr, {array});
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

        result = ArrayFunctions::array_max_boolean(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

TEST_F(ArrayFunctionsTest, array_sum_no_null) {
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});

        auto result = ArrayFunctions::array_sum_boolean(nullptr, {array});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});
        array->append_datum(DatumArray{1, 2, 3, Datum()});

        auto result = ArrayFunctions::array_sum_int(nullptr, {array});
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2, result->get(1).get_int64());
        EXPECT_EQ(6, result->get(2).get_int64());
        EXPECT_EQ(6, result->get(3).get_int64());
        EXPECT_EQ(6, result->get(4).get_int64());
        EXPECT_EQ(6, result->get(5).get_int64());
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t)127, (int8_t)100, (int8_t)-1});
        array->append_datum(DatumArray{(int8_t)-128, (int8_t)-1, (int8_t)10});

        auto result = ArrayFunctions::array_sum_tinyint(nullptr, {array});
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(226, result->get(1).get_int64());
        EXPECT_EQ(-119, result->get(2).get_int64());
    }
}

TEST_F(ArrayFunctionsTest, array_avg_no_null) {
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});

        auto result = ArrayFunctions::array_avg_boolean(nullptr, {array});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});
        array->append_datum(DatumArray{1, 2, 3, Datum()});

        auto result = ArrayFunctions::array_avg_int(nullptr, {array});
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2, result->get(1).get_double());
        EXPECT_EQ(2, result->get(2).get_double());
        EXPECT_EQ(2, result->get(3).get_double());
        EXPECT_EQ(2, result->get(4).get_double());
        EXPECT_EQ(1.5, result->get(5).get_double());
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) true, Datum(), Datum(), (int8_t) false});
        array->append_datum(DatumArray{(int8_t) false, Datum()});
        array->append_datum(DatumArray{(int8_t) true, Datum()});

        auto result = ArrayFunctions::array_avg_boolean(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(0.25, result->get(1).get_double());
        EXPECT_EQ(0, result->get(2).get_double());
        EXPECT_EQ(0.5, result->get(3).get_double());
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_TINYINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t)-128, (int8_t)127, (int8_t)0, Datum()});
        array->append_datum(DatumArray{(int8_t)127, (int8_t)10, (int8_t)100});

        auto result = ArrayFunctions::array_avg_tinyint(nullptr, {array});
        EXPECT_EQ(3, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(-0.25, result->get(1).get_double());
        EXPECT_EQ(79, result->get(2).get_double());
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_SMALLINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int16_t)30000, (int16_t)30000, Datum()});
        array->append_datum(DatumArray{(int16_t)-32768, (int16_t)32767, Datum(), (int16_t)0, (int16_t)1});

        auto result = ArrayFunctions::array_avg_smallint(nullptr, {array});
        EXPECT_EQ(3, result->size());

        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(20000, result->get(1).get_double());
        EXPECT_EQ(0, result->get(2).get_double());
    }
}

TEST_F(ArrayFunctionsTest, array_min_no_null) {
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});

        auto result = ArrayFunctions::array_min_boolean(nullptr, {array});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});

        auto result = ArrayFunctions::array_min_int(nullptr, {array});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BOOLEAN, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) false});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});
        array->append_datum(DatumArray{(int8_t) true, (int8_t) false});

        auto result = ArrayFunctions::array_max_boolean(nullptr, {array});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{2});
        array->append_datum(DatumArray{1, 2, 3});
        array->append_datum(DatumArray{3, 2, 1});
        array->append_datum(DatumArray{2, 1, 3});

        auto result = ArrayFunctions::array_max_int(nullptr, {array});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int64_t)2000});
        array->append_datum(DatumArray{(int64_t)1000, (int64_t)2, (int64_t)3});
        array->append_datum(DatumArray{(int64_t)3, (int64_t)2, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)200000000, (int64_t)121, (int64_t)300});
        array->append_datum(DatumArray{(int64_t)33, Datum(), (int64_t)300});

        auto result = ArrayFunctions::array_sum_bigint(nullptr, {array});
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int64());
        EXPECT_EQ(1005, result->get(2).get_int64());
        EXPECT_EQ(6, result->get(3).get_int64());
        EXPECT_EQ(200000421, result->get(4).get_int64());
        EXPECT_EQ(333, result->get(5).get_int64());
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_LARGEINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int128_t)2000});
        array->append_datum(DatumArray{(int128_t)1000, (int128_t)2, (int128_t)3});
        array->append_datum(DatumArray{(int128_t)3, (int128_t)2, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)200000000, (int128_t)121, (int128_t)300});
        array->append_datum(DatumArray{(int128_t)33, Datum(), (int128_t)300});

        auto result = ArrayFunctions::array_sum_largeint(nullptr, {array});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int64_t)2000});
        array->append_datum(DatumArray{(int64_t)1000, (int64_t)2, (int64_t)3});
        array->append_datum(DatumArray{(int64_t)3, (int64_t)2, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)1, (int64_t)1, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)2, Datum(), (int64_t)1});

        auto result = ArrayFunctions::array_avg_bigint(nullptr, {array});
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_double());
        EXPECT_EQ(335, result->get(2).get_double());
        EXPECT_EQ(2, result->get(3).get_double());
        EXPECT_EQ(1, result->get(4).get_double());
        EXPECT_EQ(1, result->get(5).get_double());
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_LARGEINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int128_t)2000});
        array->append_datum(DatumArray{(int128_t)1000, (int128_t)2, (int128_t)3});
        array->append_datum(DatumArray{(int128_t)3, (int128_t)2, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)1, (int128_t)1, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)2, Datum(), (int128_t)1});

        auto result = ArrayFunctions::array_avg_largeint(nullptr, {array});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int64_t)2000});
        array->append_datum(DatumArray{(int64_t)1000, (int64_t)2, (int64_t)3});
        array->append_datum(DatumArray{(int64_t)3, (int64_t)2, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)1, (int64_t)1, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)1, Datum(), (int64_t)1});

        auto result = ArrayFunctions::array_min_bigint(nullptr, {array});
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int64());
        EXPECT_EQ(2, result->get(2).get_int64());
        EXPECT_EQ(1, result->get(3).get_int64());
        EXPECT_EQ(1, result->get(4).get_int64());
        EXPECT_EQ(1, result->get(5).get_int64());
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_LARGEINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int128_t)2000});
        array->append_datum(DatumArray{(int128_t)1000, (int128_t)2, (int128_t)3});
        array->append_datum(DatumArray{(int128_t)3, (int128_t)2, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)1, (int128_t)1, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)1, Datum(), (int128_t)1});

        auto result = ArrayFunctions::array_min_largeint(nullptr, {array});
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int128());
        EXPECT_EQ(2, result->get(2).get_int128());
        EXPECT_EQ(1, result->get(3).get_int128());
        EXPECT_EQ(1, result->get(4).get_int128());
        EXPECT_EQ(1, result->get(5).get_int128());
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_DATE, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 24)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 26)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 28)});
        array->append_datum(DatumArray{Datum(), DateValue::create(1990, 3, 28)});

        auto result = ArrayFunctions::array_min_date(nullptr, {array});
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(1).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(2).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(3).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(4).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 28), result->get(5).get_date());
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_DATETIME, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 34)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 36)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 38)});
        array->append_datum(DatumArray{Datum(), TimestampValue::create(1990, 3, 22, 5, 32, 38)});

        auto result = ArrayFunctions::array_min_datetime(nullptr, {array});
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(1).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(2).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(3).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(4).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 38), result->get(5).get_timestamp());
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{"varchar"});
        array->append_datum(DatumArray{"varchar1", "varchar2"});
        array->append_datum(DatumArray{"varchar1", "varchar3"});
        array->append_datum(DatumArray{"varchar1", "varchar4"});
        array->append_datum(DatumArray{Datum(), "varchar4"});

        auto result = ArrayFunctions::array_min_varchar(nullptr, {array});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int64_t)2000});
        array->append_datum(DatumArray{(int64_t)1000, (int64_t)2, (int64_t)3});
        array->append_datum(DatumArray{(int64_t)3, (int64_t)2, (int64_t)1});
        array->append_datum(DatumArray{(int64_t)1, (int64_t)1, (int64_t)1});

        auto result = ArrayFunctions::array_max_bigint(nullptr, {array});
        EXPECT_EQ(5, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int64());
        EXPECT_EQ(1000, result->get(2).get_int64());
        EXPECT_EQ(3, result->get(3).get_int64());
        EXPECT_EQ(1, result->get(4).get_int64());
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_LARGEINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{(int128_t)2000});
        array->append_datum(DatumArray{(int128_t)1000, (int128_t)2, (int128_t)3});
        array->append_datum(DatumArray{(int128_t)3, (int128_t)2, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)1, (int128_t)1, (int128_t)1});
        array->append_datum(DatumArray{(int128_t)2, (int128_t)1, Datum()});

        auto result = ArrayFunctions::array_max_largeint(nullptr, {array});
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(2000, result->get(1).get_int128());
        EXPECT_EQ(1000, result->get(2).get_int128());
        EXPECT_EQ(3, result->get(3).get_int128());
        EXPECT_EQ(1, result->get(4).get_int128());
        EXPECT_EQ(2, result->get(5).get_int128());
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_DATE, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 24)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 26)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), DateValue::create(1990, 3, 28)});
        array->append_datum(DatumArray{DateValue::create(1990, 3, 22), Datum()});

        auto result = ArrayFunctions::array_max_date(nullptr, {array});
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(1).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 24), result->get(2).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 26), result->get(3).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 28), result->get(4).get_date());
        EXPECT_EQ(DateValue::create(1990, 3, 22), result->get(5).get_date());
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_DATETIME, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 34)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 36)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32),
                                       TimestampValue::create(1990, 3, 22, 5, 32, 38)});
        array->append_datum(DatumArray{TimestampValue::create(1990, 3, 22, 5, 32, 32), Datum()});

        auto result = ArrayFunctions::array_max_datetime(nullptr, {array});
        EXPECT_EQ(6, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(1).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 34), result->get(2).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 36), result->get(3).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 38), result->get(4).get_timestamp());
        EXPECT_EQ(TimestampValue::create(1990, 3, 22, 5, 32, 32), result->get(5).get_timestamp());
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{"varchar"});
        array->append_datum(DatumArray{"varchar1", "varchar2"});
        array->append_datum(DatumArray{"varchar1", "varchar3"});
        array->append_datum(DatumArray{"varchar1", "varchar4"});
        array->append_datum(DatumArray{"varchar1", Datum()});

        auto result = ArrayFunctions::array_max_varchar(nullptr, {array});
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
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        array->append_datum(DatumArray{3, 5});
        array->append_datum(DatumArray{Datum(), 54});
        array->append_datum(DatumArray{5352, 121, 30});

        auto result = ArrayFunctions::array_sum_int(nullptr, {array});
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(8, result->get(0).get_int64());
        EXPECT_EQ(54, result->get(1).get_int64());
        EXPECT_EQ(5503, result->get(2).get_int64());
    }
}

TEST_F(ArrayFunctionsTest, array_avg_nullable_array) {
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        array->append_datum(DatumArray{3, 5});
        array->append_datum(DatumArray{Datum(), 54});
        array->append_datum(DatumArray{5352, 121, 32});

        auto result = ArrayFunctions::array_avg_int(nullptr, {array});
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(4, result->get(0).get_double());
        EXPECT_EQ(27, result->get(1).get_double());
        EXPECT_EQ(1835, result->get(2).get_double());
    }
}

TEST_F(ArrayFunctionsTest, array_min_nullable_array) {
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        array->append_datum(DatumArray{3, 5});
        array->append_datum(DatumArray{Datum(), 54});
        array->append_datum(DatumArray{5352, 121, 32});

        auto result = ArrayFunctions::array_min_int(nullptr, {array});
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(3, result->get(0).get_int32());
        EXPECT_EQ(54, result->get(1).get_int32());
        EXPECT_EQ(32, result->get(2).get_int32());
    }
}

TEST_F(ArrayFunctionsTest, array_max_nullable_array) {
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_INT, true);
        array->append_datum(DatumArray{3, 5});
        array->append_datum(DatumArray{Datum(), 54});
        array->append_datum(DatumArray{5352, 121, 32});

        auto result = ArrayFunctions::array_max_int(nullptr, {array});
        EXPECT_EQ(3, result->size());
        EXPECT_EQ(5, result->get(0).get_int32());
        EXPECT_EQ(54, result->get(1).get_int32());
        EXPECT_EQ(5352, result->get(2).get_int32());
    }
}

TEST_F(ArrayFunctionsTest, array_all_null) {
    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_sum_bigint(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_avg_bigint(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_min_bigint(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_min_varchar(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_BIGINT, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_max_bigint(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    {
        auto array = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
        array->append_datum(Datum(DatumArray{}));
        array->append_datum(DatumArray{Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});
        array->append_datum(DatumArray{Datum(), Datum(), Datum()});

        auto result = ArrayFunctions::array_max_varchar(nullptr, {array});
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

} // namespace starrocks::vectorized
