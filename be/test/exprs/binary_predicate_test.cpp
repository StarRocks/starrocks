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

#include "exprs/binary_predicate.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/fixed_length_column.h"
#include "exprs/mock_vectorized_expr.h"

namespace starrocks {

class VectorizedBinaryPredicateTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    }

public:
    TExprNode expr_node;
};

TEST_F(VectorizedBinaryPredicateTest, eqExpr) {
    expr_node.opcode = TExprOpcode::EQ;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 10, 0);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    // normal int8
    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_FALSE(ptr->is_nullable());
        ASSERT_TRUE(ptr->is_numeric());

        auto v = std::static_pointer_cast<BooleanColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(0, (int)v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedBinaryPredicateTest, neExpr) {
    expr_node.opcode = TExprOpcode::NE;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 10, 0);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    // normal int8
    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_FALSE(ptr->is_nullable());
        ASSERT_TRUE(ptr->is_numeric());

        auto v = std::static_pointer_cast<BooleanColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(1, (int)v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedBinaryPredicateTest, geExpr) {
    expr_node.opcode = TExprOpcode::GE;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 10, 0);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    // normal int8
    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_FALSE(ptr->is_nullable());
        ASSERT_TRUE(ptr->is_numeric());

        auto v = std::static_pointer_cast<BooleanColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(1, v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedBinaryPredicateTest, nullLtExpr) {
    expr_node.opcode = TExprOpcode::LT;
    expr_node.child_type = TPrimitiveType::BOOLEAN;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));

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

        auto ptr = std::static_pointer_cast<NullableColumn>(v)->data_column();
        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(1, (int)std::static_pointer_cast<BooleanColumn>(ptr)->get_data()[j]);
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
        ColumnPtr ptr = std::static_pointer_cast<NullableColumn>(v)->data_column();

        ASSERT_TRUE(v->is_nullable());
        ASSERT_FALSE(v->is_numeric());

        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_EQ(0, (int)std::static_pointer_cast<BooleanColumn>(ptr)->get_data()[j]);
        }

        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }
    }
}

TEST_F(VectorizedBinaryPredicateTest, mergeNullLtExpr) {
    expr_node.opcode = TExprOpcode::LT;
    expr_node.child_type = TPrimitiveType::BOOLEAN;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 0);
    MockNullVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 10, 1);
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
            for (int j = 0; j < v->size(); ++j) {
                if (j % 2) {
                    ASSERT_FALSE(v->is_null(j));
                } else {
                    ASSERT_TRUE(v->is_null(j));
                }
            }
        }
    }

    col2.flag = 1;
    {
        ColumnPtr v = expr->evaluate(nullptr, nullptr);
        ColumnPtr ptr = std::static_pointer_cast<NullableColumn>(v)->data_column();

        ASSERT_TRUE(v->is_nullable());
        ASSERT_FALSE(v->is_numeric());

        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_EQ(1, (int)std::static_pointer_cast<BooleanColumn>(ptr)->get_data()[j]);
        }

        for (int j = 0; j < ptr->size(); ++j) {
            if (j % 2) {
                ASSERT_FALSE(v->is_null(j));
            } else {
                ASSERT_TRUE(v->is_null(j));
            }
        }
    }
}

TEST_F(VectorizedBinaryPredicateTest, eqForNullExpr) {
    expr_node.opcode = TExprOpcode::EQ_FOR_NULL;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 10, 1);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    // normal int8
    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_FALSE(ptr->is_nullable());
        ASSERT_TRUE(ptr->is_numeric());

        auto v = std::static_pointer_cast<BooleanColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(1, v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedBinaryPredicateTest, nullEqForNullExpr) {
    expr_node.opcode = TExprOpcode::EQ_FOR_NULL;
    expr_node.child_type = TPrimitiveType::BOOLEAN;

    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));

    MockNullVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 0);
    MockNullVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 10, 1);

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

        auto ptr = std::static_pointer_cast<NullableColumn>(v)->data_column();
        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(0, (int)std::static_pointer_cast<BooleanColumn>(ptr)->get_data()[j]);
        }
    }

    {
        ColumnPtr v = col2.evaluate(nullptr, nullptr);
        ASSERT_TRUE(v->is_nullable());
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            if (j % 2) {
                ASSERT_TRUE(v->is_null(j));
            } else {
                ASSERT_FALSE(v->is_null(j));
            }
        }
    }
    {
        ColumnPtr v = expr->evaluate(nullptr, nullptr);
        auto ptr = std::static_pointer_cast<BooleanColumn>(v);

        ASSERT_FALSE(v->is_nullable());
        ASSERT_TRUE(v->is_numeric());

        for (int j = 0; j < ptr->size(); ++j) {
            if (j % 2) {
                ASSERT_EQ(1, (int)ptr->get_data()[j]);
            } else {
                ASSERT_EQ(0, (int)ptr->get_data()[j]);
            }
        }
    }
}

TEST_F(VectorizedBinaryPredicateTest, nullAndNotNullEqForNullExpr) {
    expr_node.opcode = TExprOpcode::EQ_FOR_NULL;
    expr_node.child_type = TPrimitiveType::BOOLEAN;

    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));

    MockNullVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 10, 1);

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

        auto ptr = std::static_pointer_cast<NullableColumn>(v)->data_column();
        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(1, (int)std::static_pointer_cast<BooleanColumn>(ptr)->get_data()[j]);
        }
    }

    {
        ColumnPtr v = expr->evaluate(nullptr, nullptr);
        auto ptr = std::static_pointer_cast<BooleanColumn>(v);

        ASSERT_FALSE(v->is_nullable());
        ASSERT_TRUE(v->is_numeric());

        for (int j = 0; j < ptr->size(); ++j) {
            if (j % 2) {
                ASSERT_EQ(0, (int)ptr->get_data()[j]);
            } else {
                ASSERT_EQ(1, (int)ptr->get_data()[j]);
            }
        }
    }
}

TEST_F(VectorizedBinaryPredicateTest, diffNullEqForNullExpr) {
    expr_node.opcode = TExprOpcode::EQ_FOR_NULL;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));

    MockNullVectorizedExpr<TYPE_INT> col1(expr_node, 10, 1);
    MockNullVectorizedExpr<TYPE_INT> col2(expr_node, 10, 1);
    col2.flag++;

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
        auto ptr = std::static_pointer_cast<BooleanColumn>(v);

        ASSERT_FALSE(v->is_nullable());
        ASSERT_TRUE(v->is_numeric());

        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_EQ(0, (int)ptr->get_data()[j]);
        }
    }
}

// Unit test cases for string predicates.
// Since TYPE_CHAR and TYPE_VARCHAR are both mapping to Slice objects, we only offer cases of TYPE_VARCHAR.
class VectorizedBinaryPredicateStringTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::INVALID_OPCODE;
        expr_node.child_type = TPrimitiveType::VARCHAR;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    }

public:
    TExprNode expr_node;
};

TEST_F(VectorizedBinaryPredicateStringTest, eqExpr) {
    expr_node.opcode = TExprOpcode::EQ;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
    const int size = 10;
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, size, "dorisDB");
    MockVectorizedExpr<TYPE_VARCHAR> col2(expr_node, size, "dorisDB");
    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
    ASSERT_FALSE(ptr->is_nullable());
    ASSERT_TRUE(ptr->is_numeric());

    auto v = std::static_pointer_cast<BooleanColumn>(ptr);
    ASSERT_EQ(size, v->size());
    for (int j = 0; j < v->size(); ++j) {
        ASSERT_TRUE(v->get_data()[j]);
    }
}

TEST_F(VectorizedBinaryPredicateStringTest, neExpr) {
    expr_node.opcode = TExprOpcode::NE;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
    const int size = 10;
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, size, "dorisDB");
    MockVectorizedExpr<TYPE_VARCHAR> col2(expr_node, size, "DorisDB");
    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
    ASSERT_FALSE(ptr->is_nullable());
    ASSERT_TRUE(ptr->is_numeric());

    auto v = std::static_pointer_cast<BooleanColumn>(ptr);
    ASSERT_EQ(size, v->size());
    for (int j = 0; j < v->size(); ++j) {
        ASSERT_TRUE(v->get_data()[j]);
    }
}

TEST_F(VectorizedBinaryPredicateStringTest, gtExpr) {
    expr_node.opcode = TExprOpcode::GT;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
    const int size = 10;
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, size, "bbbbb");
    MockVectorizedExpr<TYPE_VARCHAR> col2(expr_node, size, "aaaaa");
    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
    ASSERT_FALSE(ptr->is_nullable());
    ASSERT_TRUE(ptr->is_numeric());

    auto v = std::static_pointer_cast<BooleanColumn>(ptr);
    ASSERT_EQ(size, v->size());
    for (int j = 0; j < v->size(); ++j) {
        ASSERT_TRUE(v->get_data()[j]);
    }
}

TEST_F(VectorizedBinaryPredicateStringTest, ltExpr) {
    expr_node.opcode = TExprOpcode::LT;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
    const int size = 10;
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, size, "aaaaa");
    MockVectorizedExpr<TYPE_VARCHAR> col2(expr_node, size, "bbbbb");
    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
    ASSERT_FALSE(ptr->is_nullable());
    ASSERT_TRUE(ptr->is_numeric());

    auto v = std::static_pointer_cast<BooleanColumn>(ptr);
    ASSERT_EQ(size, v->size());
    for (int j = 0; j < v->size(); ++j) {
        ASSERT_TRUE(v->get_data()[j]);
    }
}

TEST_F(VectorizedBinaryPredicateStringTest, nullEqExpr) {
    expr_node.opcode = TExprOpcode::EQ;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
    const int size = 10;
    MockNullVectorizedExpr<TYPE_VARCHAR> col1(expr_node, size, "");
    MockNullVectorizedExpr<TYPE_VARCHAR> col2(expr_node, size, "");
    col1.all_null = true;
    col2.all_null = true;
    col1.evaluate(nullptr, nullptr);
    col2.evaluate(nullptr, nullptr);

    expr->add_child(&col1);
    expr->add_child(&col2);

    ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
    ASSERT_TRUE(ptr->is_nullable());
    ASSERT_FALSE(ptr->is_numeric());

    ASSERT_EQ(size, ptr->size());
    for (int j = 0; j < ptr->size(); ++j) {
        ASSERT_TRUE(ptr->is_null(j));
    }
}

} // namespace starrocks
