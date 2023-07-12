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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "exprs/condition_expr.h"
#include "exprs/mock_vectorized_expr.h"

namespace starrocks {

class VectorizedIfExprTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BIGINT);
        tttype_desc.push_back(expr_node.type);

        TTypeDesc ttype_desc;
        ttype_desc.__isset.types = true;
        ttype_desc.types.emplace_back();
        ttype_desc.types.back().__set_type(TTypeNodeType::ARRAY);
        ttype_desc.types.emplace_back();
        ttype_desc.types.back().__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types.back().__set_scalar_type(TScalarType());
        ttype_desc.types.back().scalar_type.__set_type(TPrimitiveType::INT);
        ttype_desc.types.back().scalar_type.__set_len(10);
        tttype_desc.push_back(ttype_desc);
    }

private:
    std::vector<TTypeDesc> tttype_desc;
    TExprNode expr_node;
};

TEST_F(VectorizedIfExprTest, ifArray) {
    expr_node.type = tttype_desc[1];
    auto expr = VectorizedConditionExprFactory::create_if_expr(expr_node);
    std::unique_ptr<Expr> expr_ptr(expr);

    auto target = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), false);
    target->append_datum(Datum{(int8_t)0});
    target->append_datum(Datum{(int8_t)1});
    target->append_datum(Datum{(int8_t)0});
    auto cond = MockExpr(TypeDescriptor(TYPE_BOOLEAN), target);

    TypeDescriptor type_arr_int = array_type(TYPE_INT);

    auto array0 = ColumnHelper::create_column(type_arr_int, true);
    array0->append_datum(DatumArray{Datum((int32_t)1), Datum((int32_t)4)}); // [1,4]
    array0->append_datum(DatumArray{Datum(), Datum()});                     // [NULL, NULL]
    array0->append_datum(DatumArray{Datum(), Datum((int32_t)12)});          // [NULL, 12]
    auto array_expr0 = MockExpr(type_arr_int, array0);

    auto array1 = ColumnHelper::create_column(type_arr_int, false);
    array1->append_datum(DatumArray{Datum((int32_t)11), Datum((int32_t)41)}); // [11,41]
    array1->append_datum(DatumArray{Datum(), Datum()});                       // [NULL, NULL]
    array1->append_datum(DatumArray{Datum(), Datum((int32_t)1)});             // [NULL, 1]
    auto array_expr1 = MockExpr(type_arr_int, array1);

    expr->_children.push_back(&cond);
    expr->_children.push_back(&array_expr0);
    expr->_children.push_back(&array_expr1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        if (ptr->is_nullable()) {
            ptr = down_cast<NullableColumn*>(ptr.get())->data_column();
        }
        ASSERT_TRUE(ptr->is_array());
        ASSERT_TRUE(array1->equals(0, *ptr, 0));
        ASSERT_TRUE(array0->equals(1, *ptr, 1));
        ASSERT_TRUE(array1->equals(2, *ptr, 2));
    }
}

TEST_F(VectorizedIfExprTest, ifConstTrue) {
    for (auto desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = VectorizedConditionExprFactory::create_if_expr(expr_node);
        std::unique_ptr<Expr> expr_ptr(expr);
        expr->set_type(TypeDescriptor(TYPE_BIGINT));

        MockConstVectorizedExpr<TYPE_BOOLEAN> bol(expr_node, true);
        MockVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
        MockVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

        expr->_children.push_back(&bol);
        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        {
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ASSERT_TRUE(ptr->is_numeric());

            auto v = ColumnHelper::cast_to_raw<TYPE_BIGINT>(ptr);
            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_EQ(10, v->get_data()[j]);
            }
        }
    }
}

TEST_F(VectorizedIfExprTest, ifAllFalse) {
    for (auto desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = VectorizedConditionExprFactory::create_if_expr(expr_node);
        std::unique_ptr<Expr> expr_ptr(expr);
        expr->set_type(TypeDescriptor(TYPE_BIGINT));

        MockVectorizedExpr<TYPE_BOOLEAN> bol(expr_node, 10, false);
        MockVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
        MockVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

        expr->_children.push_back(&bol);
        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        {
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ASSERT_TRUE(ptr->is_numeric());

            auto v = ColumnHelper::cast_to_raw<TYPE_BIGINT>(ptr);
            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_EQ(20, v->get_data()[j]);
            }
        }
    }
}

TEST_F(VectorizedIfExprTest, ifNull) {
    for (auto desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = VectorizedConditionExprFactory::create_if_expr(expr_node);
        std::unique_ptr<Expr> expr_ptr(expr);
        expr->set_type(TypeDescriptor(TYPE_BIGINT));

        MockMultiVectorizedExpr<TYPE_BOOLEAN> bol(expr_node, 10, true, false);
        MockNullVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
        MockNullVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

        expr->_children.push_back(&bol);
        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        {
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ASSERT_TRUE(ptr->is_nullable());
            ASSERT_FALSE(ptr->is_numeric());

            auto v = ColumnHelper::cast_to_raw<TYPE_BIGINT>(
                    ColumnHelper::as_raw_column<NullableColumn>(ptr)->data_column());
            for (int j = 0; j < ptr->size(); ++j) {
                if (j % 2 == 0) {
                    ASSERT_FALSE(ptr->is_null(j));
                    ASSERT_EQ(10, v->get_data()[j]);
                } else {
                    ASSERT_TRUE(ptr->is_null(j));
                }
            }
        }
    }
}

TEST_F(VectorizedIfExprTest, ifNullRightConst) {
    for (auto desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = VectorizedConditionExprFactory::create_if_expr(expr_node);
        std::unique_ptr<Expr> expr_ptr(expr);
        expr->set_type(TypeDescriptor(TYPE_BIGINT));

        MockMultiVectorizedExpr<TYPE_BOOLEAN> bol(expr_node, 10, true, false);
        MockNullVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
        MockConstVectorizedExpr<TYPE_BIGINT> col2(expr_node, 20);

        expr->_children.push_back(&bol);
        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        {
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ASSERT_TRUE(ptr->is_nullable());
            ASSERT_FALSE(ptr->is_numeric());

            auto v = ColumnHelper::cast_to_raw<TYPE_BIGINT>(
                    ColumnHelper::as_raw_column<NullableColumn>(ptr)->data_column());
            for (int j = 0; j < ptr->size(); ++j) {
                if (j % 2 == 0) {
                    ASSERT_FALSE(ptr->is_null(j));
                    ASSERT_EQ(10, v->get_data()[j]);
                } else {
                    ASSERT_FALSE(ptr->is_null(j));
                    ASSERT_EQ(20, v->get_data()[j]);
                }
            }
        }
    }
}

} // namespace starrocks
