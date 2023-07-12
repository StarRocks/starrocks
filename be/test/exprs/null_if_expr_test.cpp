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
<<<<<<< HEAD
=======
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
>>>>>>> fdc1cb7a3b ([BugFix] return column created from this->type() and hold const inputs for map functions (#26974))
    }

public:
    TExprNode expr_node;
};

TEST_F(VectorizedNullIfExprTest, nullIfEquals) {
    auto expr = VectorizedConditionExprFactory::create_null_if_expr(expr_node);
    std::unique_ptr<Expr> expr_ptr(expr);

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

<<<<<<< HEAD
TEST_F(VectorizedNullIfExprTest, nullIfAllFalse) {
    auto expr = VectorizedConditionExprFactory::create_null_if_expr(expr_node);
    std::unique_ptr<Expr> expr_ptr(expr);
=======
TEST_F(VectorizedNullIfExprTest, nullIfEquals) {
    for (auto desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = VectorizedConditionExprFactory::create_null_if_expr(expr_node);
        std::unique_ptr<Expr> expr_ptr(expr);
        expr->set_type(TypeDescriptor(TYPE_BIGINT));
>>>>>>> fdc1cb7a3b ([BugFix] return column created from this->type() and hold const inputs for map functions (#26974))

    MockVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
    MockVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);
    {
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->is_numeric());

        auto v = ColumnHelper::cast_to_raw<TYPE_BIGINT>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_EQ(10, v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedNullIfExprTest, nullIfLeftAllNull) {
    auto expr = VectorizedConditionExprFactory::create_null_if_expr(expr_node);
    std::unique_ptr<Expr> expr_ptr(expr);

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

TEST_F(VectorizedNullIfExprTest, nullIfLeftNullRightNull) {
    auto expr = VectorizedConditionExprFactory::create_null_if_expr(expr_node);
    std::unique_ptr<Expr> expr_ptr(expr);

    MockNullVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
    MockNullVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);
    {
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->is_nullable());
        ASSERT_FALSE(ptr->is_numeric());

        auto v =
                ColumnHelper::cast_to_raw<TYPE_BIGINT>(ColumnHelper::as_raw_column<NullableColumn>(ptr)->data_column());
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

<<<<<<< HEAD
=======
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

>>>>>>> fdc1cb7a3b ([BugFix] return column created from this->type() and hold const inputs for map functions (#26974))
} // namespace starrocks
