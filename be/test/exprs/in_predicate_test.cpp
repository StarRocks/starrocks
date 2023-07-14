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

#include "exprs/in_predicate.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "butil/time.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "exprs/mock_vectorized_expr.h"

namespace starrocks {

class VectorizedInPredicateTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::IN_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);

        ttype_desc.__isset.types = true;
        ttype_desc.types.emplace_back();
        ttype_desc.types.back().__set_type(TTypeNodeType::ARRAY);
        ttype_desc.types.emplace_back();
        ttype_desc.types.back().__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types.back().__set_scalar_type(TScalarType());
        ttype_desc.types.back().scalar_type.__set_type(TPrimitiveType::VARCHAR);
        ttype_desc.types.back().scalar_type.__set_len(10);

        is_not_in.push_back(true);
        is_not_in.push_back(false);
    }
    FakeConstExpr* new_fake_const_expr(ColumnPtr value, const TypeDescriptor& type) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::INT_LITERAL);
        node.__set_num_children(0);
        node.__set_type(type.to_thrift());
        FakeConstExpr* e = _objpool.add(new FakeConstExpr(node));
        e->_column = std::move(value);
        return e;
    }

public:
    TExprNode expr_node;
    TTypeDesc ttype_desc;
    std::vector<bool> is_not_in;

private:
    ObjectPool _objpool;
};

TEST_F(VectorizedInPredicateTest, sliceInTrue) {
    for (auto i = 0; i < 2; i++) {
        if (i == 0) {
            expr_node.__isset.child_type_desc = true;
            expr_node.child_type_desc = ttype_desc;
        } else {
            expr_node.__isset.child_type_desc = false;
            expr_node.child_type = TPrimitiveType::VARCHAR;
        }
        expr_node.child_type = TPrimitiveType::VARCHAR;
        expr_node.opcode = TExprOpcode::FILTER_IN;
        expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
        expr_node.in_predicate.is_not_in = false;

        auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

        std::string v1("test1");
        std::string v2("test2");
        std::string v3("test3");
        std::string v4("test4");
        std::string v5("test5");
        std::string v6("test6");

        Slice s1(v1);
        Slice s2(v2);
        Slice s3(v3);
        Slice s4(v4);
        Slice s5(v5);
        Slice s6(v6);

        MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, s1);
        MockConstVectorizedExpr<TYPE_VARCHAR> col2(expr_node, s2);
        MockConstVectorizedExpr<TYPE_VARCHAR> col3(expr_node, s3);
        MockConstVectorizedExpr<TYPE_VARCHAR> col4(expr_node, s4);
        MockConstVectorizedExpr<TYPE_VARCHAR> col5(expr_node, s1);
        MockConstVectorizedExpr<TYPE_VARCHAR> col6(expr_node, s6);

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        expr->_children.push_back(&col3);
        expr->_children.push_back(&col4);
        expr->_children.push_back(&col5);
        expr->_children.push_back(&col6);

        {
            ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
            ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ASSERT_TRUE(ptr->is_numeric());

            auto v = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(ptr);
            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_TRUE(v->get_data()[j]);
            }
        }
    }
}

TEST_F(VectorizedInPredicateTest, dateInFalse) {
    for (auto i = 0; i < 2; i++) {
        if (i == 0) {
            expr_node.__isset.child_type_desc = true;
            expr_node.child_type_desc = ttype_desc;
        } else {
            expr_node.__isset.child_type_desc = false;
            expr_node.child_type = TPrimitiveType::VARCHAR;
        }
        expr_node.child_type = TPrimitiveType::DATETIME;
        expr_node.opcode = TExprOpcode::FILTER_IN;
        expr_node.type = gen_type_desc(TPrimitiveType::DATETIME);
        expr_node.in_predicate.is_not_in = false;

        auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

        MockVectorizedExpr<TYPE_DATETIME> col1(expr_node, 10, TimestampValue::create(2020, 6, 8, 12, 20, 30));
        MockConstVectorizedExpr<TYPE_DATETIME> col2(expr_node, TimestampValue::create(2020, 6, 8, 13, 20, 30));
        MockConstVectorizedExpr<TYPE_DATETIME> col3(expr_node, TimestampValue::create(2020, 6, 8, 14, 20, 30));
        MockConstVectorizedExpr<TYPE_DATETIME> col4(expr_node, TimestampValue::create(2020, 6, 8, 15, 20, 30));
        MockConstVectorizedExpr<TYPE_DATETIME> col5(expr_node, TimestampValue::create(2021, 6, 8, 12, 20, 30));
        MockConstVectorizedExpr<TYPE_DATETIME> col6(expr_node, TimestampValue::create(2022, 6, 8, 12, 20, 30));

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        expr->_children.push_back(&col3);
        expr->_children.push_back(&col4);
        expr->_children.push_back(&col5);
        expr->_children.push_back(&col6);

        {
            ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
            ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ASSERT_TRUE(ptr->is_numeric());

            auto v = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(ptr);
            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_FALSE(v->get_data()[j]);
            }
        }
    }
}

TEST_F(VectorizedInPredicateTest, intNotInTrue) {
    for (auto i = 0; i < 2; i++) {
        if (i == 0) {
            expr_node.__isset.child_type_desc = true;
            expr_node.child_type_desc = ttype_desc;
        } else {
            expr_node.__isset.child_type_desc = false;
            expr_node.child_type = TPrimitiveType::VARCHAR;
        }
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.opcode = TExprOpcode::FILTER_IN;
        expr_node.type = gen_type_desc(TPrimitiveType::INT);
        expr_node.in_predicate.is_not_in = true;

        auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

        MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 0);
        MockConstVectorizedExpr<TYPE_INT> col2(expr_node, 1);
        MockConstVectorizedExpr<TYPE_INT> col3(expr_node, 2);
        MockConstVectorizedExpr<TYPE_INT> col4(expr_node, 3);
        MockConstVectorizedExpr<TYPE_INT> col5(expr_node, 4);
        MockConstVectorizedExpr<TYPE_INT> col6(expr_node, 5);

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        expr->_children.push_back(&col3);
        expr->_children.push_back(&col4);
        expr->_children.push_back(&col5);
        expr->_children.push_back(&col6);

        {
            ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
            ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ASSERT_TRUE(ptr->is_numeric());

            auto v = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(ptr);
            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_TRUE(v->get_data()[j]);
            }
        }
    }
}

TEST_F(VectorizedInPredicateTest, nullSliceIn) {
    for (auto i = 0; i < 2; i++) {
        if (i == 0) {
            expr_node.__isset.child_type_desc = true;
            expr_node.child_type_desc = ttype_desc;
        } else {
            expr_node.__isset.child_type_desc = false;
            expr_node.child_type = TPrimitiveType::VARCHAR;
        }
        expr_node.opcode = TExprOpcode::FILTER_IN;
        expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
        expr_node.in_predicate.is_not_in = false;

        auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

        std::string v1("test1");
        std::string v2("test2");

        Slice s1(v1);
        Slice s2(v2);

        MockNullVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, s1);
        MockConstVectorizedExpr<TYPE_VARCHAR> col2(expr_node, s2);
        MockConstVectorizedExpr<TYPE_VARCHAR> col3(expr_node, s1);

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        expr->_children.push_back(&col3);

        {
            ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
            ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ASSERT_TRUE(ptr->is_nullable());

            auto v = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(
                    ColumnHelper::as_raw_column<NullableColumn>(ptr)->data_column());
            for (int j = 0; j < ptr->size(); ++j) {
                if (j % 2 == 0) {
                    ASSERT_FALSE(ptr->is_null(j));
                    ASSERT_TRUE(v->get_data()[j]);
                } else {
                    ASSERT_TRUE(ptr->is_null(j));
                }
            }
        }
    }
}

TEST_F(VectorizedInPredicateTest, sliceNotInNull) {
    for (auto i = 0; i < 2; i++) {
        if (i == 0) {
            expr_node.__isset.child_type_desc = true;
            expr_node.child_type_desc = ttype_desc;
        } else {
            expr_node.__isset.child_type_desc = false;
            expr_node.child_type = TPrimitiveType::VARCHAR;
        }
        expr_node.child_type = TPrimitiveType::VARCHAR;
        expr_node.opcode = TExprOpcode::FILTER_IN;
        expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
        expr_node.in_predicate.is_not_in = true;

        auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

        std::string v1("test1");
        std::string v2("test2");
        std::string v3("test3");

        Slice s1(v1);
        Slice s2(v2);
        Slice s3(v2);

        MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, s1);
        MockConstVectorizedExpr<TYPE_VARCHAR> col2(expr_node, s2);
        MockNullVectorizedExpr<TYPE_VARCHAR> col3(expr_node, 1, s3);
        col3.all_null = true;
        col3.only_null = true;

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        expr->_children.push_back(&col3);

        {
            ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
            ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ASSERT_TRUE(ptr->is_nullable());

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_TRUE(ptr->is_null(j));
            }
        }
    }
}

TEST_F(VectorizedInPredicateTest, inConstPred) {
    for (auto i = 0; i < 2; i++) {
        if (i == 0) {
            expr_node.__isset.child_type_desc = true;
            expr_node.child_type_desc = ttype_desc;
        } else {
            expr_node.__isset.child_type_desc = false;
            expr_node.child_type = TPrimitiveType::VARCHAR;
        }
        expr_node.child_type = TPrimitiveType::VARCHAR;
        expr_node.opcode = TExprOpcode::FILTER_NOT_IN;
        expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
        expr_node.in_predicate.is_not_in = true;

        auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

        std::string v2("test2");
        Slice s2(v2);

        auto mock_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(s2, 5);
        MockExpr col1(expr_node, mock_col);

        MockNullVectorizedExpr<TYPE_VARCHAR> col2(expr_node, 1, s2);
        col2.all_null = true;
        col2.only_null = true;

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);

        {
            ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
            ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ASSERT_TRUE(ptr->size() == 5);
        }
    }
}

TEST_F(VectorizedInPredicateTest, inArray) {
    for (auto not_in : is_not_in) {
        expr_node.__isset.child_type_desc = true;
        expr_node.child_type_desc = ttype_desc;
        expr_node.opcode = not_in ? TExprOpcode::FILTER_NOT_IN : TExprOpcode::FILTER_IN;
        expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
        expr_node.in_predicate.is_not_in = not_in;

        auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

        TypeDescriptor type_arr_int = array_type(TYPE_INT);
        auto array0 = ColumnHelper::create_column(type_arr_int, true);
        array0->append_datum(DatumArray{Datum((int32_t)11), Datum((int32_t)4)}); // [11,4]
        array0->append_datum(Datum{});                                           // NULL
        array0->append_datum(DatumArray{Datum(), Datum((int32_t)1)});            // [NULL, 1]
        auto array_expr0 = MockExpr(type_arr_int, array0);

        auto array1 = ColumnHelper::create_column(type_arr_int, false);
        array1->append_datum(DatumArray{Datum((int32_t)1), Datum((int32_t)4)}); // [1,4]
        array1->append_datum(DatumArray{Datum(), Datum()});                     // [NULL, NULL]
        array1->append_datum(DatumArray{Datum(), Datum((int32_t)2)});           // [NULL, 2]
        auto array_expr1 = MockExpr(type_arr_int, array1);

        auto array = ColumnHelper::create_column(type_arr_int, false);
        array->append_datum(DatumArray{Datum((int32_t)11), Datum((int32_t)4)}); // [11,4]
        auto const_col = ConstColumn::create(array, 3);
        auto* const_array = new_fake_const_expr(const_col, type_arr_int);

        expr->add_child(&array_expr0);
        expr->add_child(&array_expr1);
        expr->add_child(const_array);

        {
            ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
            ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ASSERT_TRUE(ptr->is_nullable());
            auto data = down_cast<NullableColumn*>(ptr.get())->data_column();
            auto* v = down_cast<BooleanColumn*>(data.get());
            // true,NULL,false
            ASSERT_FALSE(ptr->is_null(0));
            ASSERT_TRUE(ptr->is_null(1));
            ASSERT_FALSE(ptr->is_null(2));
            ASSERT_EQ(!not_in, v->get_data()[0]);
            ASSERT_EQ(not_in, v->get_data()[2]);
        }
    }
}

TEST_F(VectorizedInPredicateTest, inArrayConstAll) {
    for (auto not_in : is_not_in) {
        expr_node.__isset.child_type_desc = true;
        expr_node.child_type_desc = ttype_desc;
        expr_node.opcode = not_in ? TExprOpcode::FILTER_NOT_IN : TExprOpcode::FILTER_IN;
        expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
        expr_node.in_predicate.is_not_in = not_in;

        auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

        TypeDescriptor type_arr_int = array_type(TYPE_INT);
        auto array = ColumnHelper::create_column(type_arr_int, false);
        array->append_datum(DatumArray{Datum((int32_t)11), Datum((int32_t)4)}); // [11,4]
        auto const_col = ConstColumn::create(array, 3);
        auto* const_array = new_fake_const_expr(const_col, type_arr_int);

        auto array0 = ColumnHelper::create_column(type_arr_int, false);
        array0->append_datum(DatumArray{Datum(), Datum((int32_t)4)}); // [NULL,4]
        auto const_col0 = ConstColumn::create(array0, 3);
        auto* const_array0 = new_fake_const_expr(const_col0, type_arr_int);

        auto array1 = ColumnHelper::create_column(type_arr_int, false);
        array1->append_datum(DatumArray{Datum((int32_t)11), Datum((int32_t)4)}); // [11,4]
        auto const_col1 = ConstColumn::create(array1, 3);
        auto* const_array1 = new_fake_const_expr(const_col1, type_arr_int);

        expr->add_child(const_array);
        expr->add_child(const_array0);
        expr->add_child(const_array1);

        {
            ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
            ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ASSERT_TRUE(ptr->is_constant());
            auto v = down_cast<BooleanColumn*>(down_cast<ConstColumn*>(ptr.get())->data_column().get());
            ASSERT_EQ(!not_in, v->get_data()[0]);
            ASSERT_EQ(ptr->size(), 3);
        }
    }
}

TEST_F(VectorizedInPredicateTest, inArrayConstAllNULL) {
    for (auto not_in : is_not_in) {
        expr_node.__isset.child_type_desc = true;
        expr_node.child_type_desc = ttype_desc;
        expr_node.opcode = not_in ? TExprOpcode::FILTER_NOT_IN : TExprOpcode::FILTER_IN;
        expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
        expr_node.in_predicate.is_not_in = not_in;

        auto expr = std::unique_ptr<Expr>(VectorizedInPredicateFactory::from_thrift(expr_node));

        TypeDescriptor type_arr_int = array_type(TYPE_INT);
        auto array = ColumnHelper::create_column(type_arr_int, true);
        array->append_datum(Datum{}); // NULL
        auto const_col = ConstColumn::create(array, 3);
        auto* const_array = new_fake_const_expr(const_col, type_arr_int);

        auto array0 = ColumnHelper::create_column(type_arr_int, false);
        array0->append_datum(DatumArray{Datum(), Datum((int32_t)4)}); // [NULL,4]
        auto const_col0 = ConstColumn::create(array0, 3);
        auto* const_array0 = new_fake_const_expr(const_col0, type_arr_int);

        auto array1 = ColumnHelper::create_column(type_arr_int, false);
        array1->append_datum(DatumArray{Datum((int32_t)11), Datum((int32_t)4)}); // [11,4]
        auto const_col1 = ConstColumn::create(array1, 3);
        auto* const_array1 = new_fake_const_expr(const_col1, type_arr_int);

        expr->add_child(const_array);
        expr->add_child(const_array0);
        expr->add_child(const_array1);

        {
            ASSERT_TRUE(expr->prepare(nullptr, nullptr).ok());
            ASSERT_TRUE(expr->open(nullptr, nullptr, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ASSERT_TRUE(ptr->only_null());
        }
    }
}

} // namespace starrocks
