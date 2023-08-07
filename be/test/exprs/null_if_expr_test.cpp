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

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "exprs/condition_expr.h"
#include "exprs/mock_vectorized_expr.h"

namespace starrocks {

class VectorizedNullIfExprTest : public ::testing::Test {
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

TEST_F(VectorizedNullIfExprTest, nullIfArray) {
    expr_node.type = tttype_desc[1];
    auto expr = VectorizedConditionExprFactory::create_null_if_expr(expr_node);
    std::unique_ptr<Expr> expr_ptr(expr);

    TypeDescriptor type_arr_int = array_type(TYPE_INT);

    auto array0 = ColumnHelper::create_column(type_arr_int, true);
    array0->append_datum(DatumArray{Datum((int32_t)1), Datum((int32_t)4)}); // [1,4]
    array0->append_datum(DatumArray{Datum(), Datum()});                     // [NULL, NULL]
    array0->append_datum(DatumArray{Datum(), Datum((int32_t)12)});          // [NULL, 12]
    auto array_expr0 = MockExpr(type_arr_int, array0);

    auto array1 = ColumnHelper::create_column(type_arr_int, false);
    array1->append_datum(DatumArray{Datum((int32_t)1), Datum((int32_t)4)}); // [1,4]
    array1->append_datum(DatumArray{Datum(), Datum()});                     // [NULL, NULL]
    array1->append_datum(DatumArray{Datum(), Datum((int32_t)1)});           // [NULL, 1]
    auto array_expr1 = MockExpr(type_arr_int, array1);

    expr->_children.push_back(&array_expr0);
    expr->_children.push_back(&array_expr1);

    {
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->is_nullable());

        ASSERT_TRUE(ptr->is_null(0));
        ASSERT_TRUE(!ptr->is_null(1));
        ASSERT_TRUE(!ptr->is_null(2));
    }
}

TEST_F(VectorizedNullIfExprTest, nullIfEquals) {
    for (auto desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = VectorizedConditionExprFactory::create_null_if_expr(expr_node);
        std::unique_ptr<Expr> expr_ptr(expr);
        expr->set_type(TypeDescriptor(TYPE_BIGINT));

        MockVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
        MockVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 10);

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        {
            Chunk chunk;
            ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
            ASSERT_TRUE(ptr->is_nullable());

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_TRUE(ptr->is_null(j));
            }
        }
    }
}

TEST_F(VectorizedNullIfExprTest, nullIfAllFalse) {
    for (auto desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = VectorizedConditionExprFactory::create_null_if_expr(expr_node);
        std::unique_ptr<Expr> expr_ptr(expr);
        expr->set_type(TypeDescriptor(TYPE_BIGINT));

        MockVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
        MockVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        {
            Chunk chunk;
            ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
            if (ptr->is_nullable()) {
                ptr = down_cast<NullableColumn*>(ptr.get())->data_column();
            }
            ASSERT_TRUE(ptr->is_numeric());

            auto v = ColumnHelper::cast_to_raw<TYPE_BIGINT>(ptr);
            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_EQ(10, v->get_data()[j]);
            }
        }
    }
}

TEST_F(VectorizedNullIfExprTest, nullIfLeftAllNull) {
    for (auto desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = VectorizedConditionExprFactory::create_null_if_expr(expr_node);
        std::unique_ptr<Expr> expr_ptr(expr);
        expr->set_type(TypeDescriptor(TYPE_BIGINT));

        MockNullVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
        MockNullVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

        col1.all_null = true;
        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        {
            Chunk chunk;
            ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
            ASSERT_TRUE(ptr->is_nullable());
            ASSERT_TRUE(ptr->only_null());
            ASSERT_FALSE(ptr->is_numeric());
        }
    }
}

TEST_F(VectorizedNullIfExprTest, nullIfLeftAllNullConstNULL) {
    for (auto desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = VectorizedConditionExprFactory::create_null_if_expr(expr_node);
        std::unique_ptr<Expr> expr_ptr(expr);
        expr->set_type(TypeDescriptor(TYPE_BIGINT));

        MockNullVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10, true); // only null
        MockNullVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

        col1.all_null = true;
        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        {
            Chunk chunk;
            ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
            ASSERT_TRUE(ptr->is_nullable());
            ASSERT_TRUE(ptr->only_null());
            ASSERT_FALSE(ptr->is_numeric());
        }
    }
}

TEST_F(VectorizedNullIfExprTest, nullIfLeftConst) {
    for (auto desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = VectorizedConditionExprFactory::create_null_if_expr(expr_node);
        std::unique_ptr<Expr> expr_ptr(expr);
        expr->set_type(TypeDescriptor(TYPE_BIGINT));

        MockConstVectorizedExpr<TYPE_BIGINT> col1(expr_node, 20); // only const
        MockNullVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        {
            Chunk chunk;
            ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
            ASSERT_TRUE(ptr->is_nullable());
            ASSERT_FALSE(ptr->is_numeric());

            auto v = ColumnHelper::cast_to_raw<TYPE_BIGINT>(
                    ColumnHelper::as_raw_column<NullableColumn>(ptr)->data_column());
            for (int j = 0; j < ptr->size(); ++j) {
                if (j % 2 == 1) {
                    ASSERT_FALSE(ptr->is_null(j));
                    ASSERT_EQ(20, v->get_data()[j]);
                } else {
                    ASSERT_TRUE(ptr->is_null(j));
                }
            }
        }
    }
}

TEST_F(VectorizedNullIfExprTest, nullIfLeftNullRightNull) {
    for (auto desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = VectorizedConditionExprFactory::create_null_if_expr(expr_node);
        std::unique_ptr<Expr> expr_ptr(expr);
        expr->set_type(TypeDescriptor(TYPE_BIGINT));

        MockNullVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
        MockNullVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        {
            Chunk chunk;
            ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
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

} // namespace starrocks
