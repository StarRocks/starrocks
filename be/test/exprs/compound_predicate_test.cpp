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

#include "exprs/compound_predicate.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/fixed_length_column.h"
#include "exprs/exprs_test_helper.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class VectorizedCompoundPredicateTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::BIGINT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    }

public:
    RuntimeState runtime_state;
    TExprNode expr_node;
};

TEST_F(VectorizedCompoundPredicateTest, andExpr) {
    expr_node.opcode = TExprOpcode::COMPOUND_AND;
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 10, 0);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    // normal int8
    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());
            ASSERT_TRUE(ptr->is_numeric());

            auto v = BooleanColumn::static_pointer_cast(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(0, (int)v->get_data()[j]);
            }
        });
    }
}

TEST_F(VectorizedCompoundPredicateTest, orExpr) {
    expr_node.opcode = TExprOpcode::COMPOUND_OR;
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 10, 0);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    // normal int8
    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());
            ASSERT_TRUE(ptr->is_numeric());

            auto v = BooleanColumn::static_pointer_cast(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(1, v->get_data()[j]);
            }
        });
    }
}

TEST_F(VectorizedCompoundPredicateTest, nullAndExpr) {
    expr_node.opcode = TExprOpcode::COMPOUND_AND;
    expr_node.is_nullable = true;
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(expr_node));

    MockNullVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 1);
    MockNullVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 10, 1);
    ++col2.flag;

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr v = col1.evaluate(nullptr, nullptr);
        ASSERT_TRUE(v->is_nullable());
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            if (j % 2) {
                ASSERT_TRUE(v->is_null(j));
            } else {
                ASSERT_FALSE(v->is_null(j));
            }
        }

        auto ptr = NullableColumn::static_pointer_cast(v)->data_column();
        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(1, (int)BooleanColumn::static_pointer_cast(ptr)->get_data()[j]);
        }
    }

    {
        ColumnPtr v = col2.evaluate(nullptr, nullptr);
        ASSERT_TRUE(v->is_nullable());
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            if (j % 2) {
                ASSERT_FALSE(v->is_null(j));
            } else {
                ASSERT_TRUE(v->is_null(j));
            }
        }
    }
    {
        ColumnPtr v = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(v, expr.get(), &runtime_state, [](ColumnPtr const& v) {
            auto ptr = ColumnHelper::cast_to<TYPE_BOOLEAN>(NullableColumn::static_pointer_cast(v)->data_column());

            ASSERT_TRUE(v->is_nullable());
            ASSERT_FALSE(v->is_numeric());

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_EQ(1, (int)ptr->get_data()[j]);
            }

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_TRUE(v->is_null(j));
            }
        });
    }
}

TEST_F(VectorizedCompoundPredicateTest, nullAndTrueExpr) {
    expr_node.opcode = TExprOpcode::COMPOUND_AND;
    expr_node.is_nullable = true;
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(expr_node));

    MockNullVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 1);
    expr_node.is_nullable = false;
    MockVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 10, 1);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr v = expr->evaluate(nullptr, nullptr);
        ColumnPtr colv1 = col1.evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(v, expr.get(), &runtime_state, [&colv1](ColumnPtr const& v) {
            ColumnPtr ptr = NullableColumn::static_pointer_cast(v)->data_column();

            ASSERT_TRUE(v->is_nullable());
            ASSERT_FALSE(v->is_numeric());

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_EQ(1, (int)BooleanColumn::static_pointer_cast(ptr)->get_data()[j]);
            }

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_EQ(v->is_null(j), colv1->is_null(j));
            }
        });
    }
}

TEST_F(VectorizedCompoundPredicateTest, constAndExpr) {
    expr_node.opcode = TExprOpcode::COMPOUND_AND;
    expr_node.is_nullable = true;
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(expr_node));

    MockNullVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 1);
    expr_node.is_nullable = false;
    MockConstVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 0);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr v = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(v, expr.get(), &runtime_state, [](ColumnPtr const& v) {
            ColumnPtr ptr = NullableColumn::static_pointer_cast(v)->data_column();

            ASSERT_TRUE(v->is_nullable());
            ASSERT_FALSE(v->is_numeric());

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_EQ(0, (int)BooleanColumn::static_pointer_cast(ptr)->get_data()[j]);
            }

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_FALSE(v->is_null(j));
            }
        });
    }
}

TEST_F(VectorizedCompoundPredicateTest, nullAndFalseExpr) {
    expr_node.opcode = TExprOpcode::COMPOUND_AND;
    expr_node.is_nullable = true;
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(expr_node));

    MockNullVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 1);
    expr_node.is_nullable = false;
    MockVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 10, 0);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr v = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(v, expr.get(), &runtime_state, [](ColumnPtr const& v) {
            ColumnPtr ptr = NullableColumn::static_pointer_cast(v)->data_column();

            ASSERT_TRUE(v->is_nullable());
            ASSERT_FALSE(v->is_numeric());

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_EQ(0, (int)BooleanColumn::static_pointer_cast(ptr)->get_data()[j]);
            }

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_FALSE(v->is_null(j));
            }
        });
    }
}

TEST_F(VectorizedCompoundPredicateTest, mergeNullOrExpr) {
    expr_node.opcode = TExprOpcode::COMPOUND_OR;
    expr_node.is_nullable = false;
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(expr_node));

    expr_node.is_nullable = false;
    MockVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 1);
    expr_node.is_nullable = true;
    MockNullVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 10, 0);
    ++col2.flag;

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr v = col1.evaluate(nullptr, nullptr);
        ASSERT_FALSE(v->is_nullable());

        for (int j = 0; j < v->size(); ++j) {
            for (int j = 0; j < v->size(); ++j) {
                ASSERT_FALSE(v->is_null(j));
            }
        }
    }

    {
        ColumnPtr v = col2.evaluate(nullptr, nullptr);
        ASSERT_TRUE(v->is_nullable());

        for (int j = 0; j < v->size(); ++j) {
            if (j % 2) {
                ASSERT_FALSE(v->is_null(j));
            } else {
                ASSERT_TRUE(v->is_null(j));
            }
        }
    }

    col2.flag = 1;
    {
        ColumnPtr v = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(v, expr.get(), &runtime_state, [](ColumnPtr const& v) {
            ASSERT_TRUE(v->is_numeric());
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(1, (int)BooleanColumn::static_pointer_cast(v)->get_data()[j]);
            }

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_FALSE(v->is_null(j));
            }
        });
    }
}

TEST_F(VectorizedCompoundPredicateTest, FalseNullOrExpr) {
    expr_node.opcode = TExprOpcode::COMPOUND_OR;
    expr_node.is_nullable = true;
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(expr_node));

    expr_node.is_nullable = false;
    MockVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 0);
    expr_node.is_nullable = true;
    MockNullVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 10, 1);
    ++col2.flag;

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr v = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(v, expr.get(), &runtime_state, [](ColumnPtr const& v) {
            ASSERT_FALSE(v->is_numeric());
            ASSERT_EQ(10, v->size());

            auto p = BooleanColumn::static_pointer_cast(ColumnHelper::as_raw_column<NullableColumn>(v)->data_column());

            for (int j = 0; j < v->size(); ++j) {
                if (j % 2) {
                    ASSERT_FALSE(v->is_null(j));
                    ASSERT_TRUE(p->get_data()[j]);
                } else {
                    ASSERT_TRUE(v->is_null(j));
                }
            }
        });
    }
}

TEST_F(VectorizedCompoundPredicateTest, OnlyNullOrExpr) {
    expr_node.opcode = TExprOpcode::COMPOUND_OR;
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(expr_node));

    MockConstVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 0);
    MockNullVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 10, 0);
    col2.only_null = true;

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr v = expr->evaluate(nullptr, nullptr);
        ASSERT_TRUE(v->only_null());
        ASSERT_EQ(1, v->size());

        ASSERT_TRUE(nullptr != ConstColumn::dynamic_pointer_cast(v));
        ASSERT_TRUE(nullptr == NullableColumn::dynamic_pointer_cast(v));
        ASSERT_TRUE(nullptr !=
                    NullableColumn::dynamic_pointer_cast(ConstColumn::dynamic_pointer_cast(v)->data_column()));
    }
}

TEST_F(VectorizedCompoundPredicateTest, notExpr) {
    expr_node.opcode = TExprOpcode::COMPOUND_NOT;
    expr_node.is_nullable = false;
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(expr_node));
    {
        MockVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 1);
        expr->_children.push_back(&col1);

        // normal int8
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());
            ASSERT_TRUE(ptr->is_numeric());

            auto v = BooleanColumn::static_pointer_cast(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(0, v->get_data()[j]);
            }
        });
    }

    {
        MockVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 0);
        expr->_children.clear();
        expr->_children.push_back(&col1);

        // normal int8
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());
            ASSERT_TRUE(ptr->is_numeric());

            auto v = BooleanColumn::static_pointer_cast(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(1, v->get_data()[j]);
            }
        });
    }
}

TEST_F(VectorizedCompoundPredicateTest, testOnlyNullAndZeroRow) {
    expr_node.opcode = TExprOpcode::COMPOUND_AND;
    expr_node.is_nullable = true;
    std::unique_ptr<Expr> expr(VectorizedCompoundPredicateFactory::from_thrift(expr_node));

    MockNullVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 0, 0);
    col1.only_null = true;
    ASSERT_EQ(0, col1.evaluate(nullptr, nullptr)->size());

    MockNullVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 0, 0);
    ASSERT_EQ(0, col2.evaluate(nullptr, nullptr)->size());

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr v = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(v, expr.get(), &runtime_state,
                                         [](ColumnPtr const& v) { ASSERT_EQ(0, v->size()); });
    }
}

} // namespace starrocks
