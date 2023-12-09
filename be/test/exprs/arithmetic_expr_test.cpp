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

#include "exprs/arithmetic_expr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "butil/time.h"
#include "column/column_hash.h"
#include "column/fixed_length_column.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/runtime_state.h"
#include "testutil/exprs_test_helper.h"

namespace starrocks {

class VectorizedArithmeticExprTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::INT);
    }

public:
    RuntimeState runtime_state;
    TExprNode expr_node;
};

TEST_F(VectorizedArithmeticExprTest, addExpr) {
    // Normal int8
    {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.type = gen_type_desc(TPrimitiveType::TINYINT);

        std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

        MockVectorizedExpr<TYPE_TINYINT> col1(expr_node, 10, 1);
        MockVectorizedExpr<TYPE_TINYINT> col2(expr_node, 10, 2);

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);

        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());
            ASSERT_TRUE(ptr->is_numeric());

            auto v = std::static_pointer_cast<Int8Column>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(3, v->get_data()[j]);
            }
        });
    }

    // Normal int
    {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.type = gen_type_desc(TPrimitiveType::INT);

        std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

        MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 1);
        MockVectorizedExpr<TYPE_INT> col2(expr_node, 10, 2);

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);

        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());
            ASSERT_TRUE(ptr->is_numeric());

            auto v = std::static_pointer_cast<Int32Column>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(3, v->get_data()[j]);
            }
        });
    }

    // Large int
    {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.type = gen_type_desc(TPrimitiveType::LARGEINT);

        std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

        MockVectorizedExpr<TYPE_LARGEINT> col1(expr_node, 10, 3);
        MockVectorizedExpr<TYPE_LARGEINT> col2(expr_node, 10, 4);

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);

        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());
            ASSERT_TRUE(ptr->is_numeric());

            auto v = std::static_pointer_cast<Int128Column>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(7, v->get_data()[j]);
            }
        });
    }

    // Normal float
    {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.type = gen_type_desc(TPrimitiveType::FLOAT);

        std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

        MockVectorizedExpr<TYPE_FLOAT> col1(expr_node, 10, 1);
        MockVectorizedExpr<TYPE_FLOAT> col2(expr_node, 10, 2);

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);

        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());
            ASSERT_TRUE(ptr->is_numeric());

            auto v = std::static_pointer_cast<FloatColumn>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(3, v->get_data()[j]);
            }
        });
    }
}

TEST_F(VectorizedArithmeticExprTest, mulExpr) {
    expr_node.opcode = TExprOpcode::MULTIPLY;
    std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 10);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 10, 2);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            auto v = std::static_pointer_cast<Int32Column>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(20, v->get_data()[j]);
            }
        });
    }
}

// TODO(Yueyang: Implement null)
TEST_F(VectorizedArithmeticExprTest, nullMulExpr) {
    expr_node.opcode = TExprOpcode::MULTIPLY;
    std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

    MockNullVectorizedExpr<TYPE_INT> col1(expr_node, 10, 10);
    MockNullVectorizedExpr<TYPE_INT> col2(expr_node, 10, 2);
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
            ASSERT_EQ(10, std::static_pointer_cast<Int32Column>(ptr)->get_data()[j]);
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
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_TRUE(ptr->is_nullable());
            ASSERT_FALSE(ptr->is_numeric());

            auto v = std::static_pointer_cast<NullableColumn>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_TRUE(v->is_null(j));
            }
        });
    }
}

TEST_F(VectorizedArithmeticExprTest, divExpr) {
    expr_node.opcode = TExprOpcode::DIVIDE;
    std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 10);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 10, 2);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            auto v = std::static_pointer_cast<NullableColumn>(ptr);
            ASSERT_TRUE(v->is_nullable());
            ASSERT_EQ(10, v->size());

            auto nums = std::static_pointer_cast<Int32Column>(v->data_column());

            for (int j = 0; j < nums->size(); ++j) {
                ASSERT_EQ(5, nums->get_data()[j]);
            }

            for (int j = 0; j < nums->size(); ++j) {
                ASSERT_FALSE(v->is_null(j));
            }
        });
    }
    {
        // when left value is 0 and right value is negative number
        expr->_children.clear();
        MockVectorizedExpr<TYPE_INT> int_col1(expr_node, 1, 0);
        MockVectorizedExpr<TYPE_INT> int_col2(expr_node, 1, -5);

        expr->_children.push_back(&int_col1);
        expr->_children.push_back(&int_col1);
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            auto v = std::static_pointer_cast<NullableColumn>(ptr);
            ASSERT_TRUE(v->is_nullable());
            ASSERT_EQ(1, v->size());

            auto nums = std::static_pointer_cast<Int32Column>(v->data_column());

            ASSERT_EQ(nums->size(), 1);
            int32_t zero = 0;
            ASSERT_EQ(crc_hash_32(&nums->get_data()[0], sizeof(int32_t), 0x811C9DC5),
                      crc_hash_32(&zero, sizeof(int32_t), 0x811C9DC5));
        });
    }
}

TEST_F(VectorizedArithmeticExprTest, produceNullModExpr) {
    expr_node.opcode = TExprOpcode::MOD;
    std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 10);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 10, 0);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr v = col1.evaluate(nullptr, nullptr);
        ASSERT_FALSE(v->is_nullable());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_FALSE(v->is_null(j));
        }
    }

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_TRUE(ptr->is_nullable());
            ASSERT_FALSE(ptr->is_numeric());

            auto v = std::static_pointer_cast<NullableColumn>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(0, std::static_pointer_cast<Int32Column>(v->data_column())->get_data()[j]);
            }

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_TRUE(v->is_null(j));
            }
        });
    }
}

TEST_F(VectorizedArithmeticExprTest, mergeNullDivExpr) {
    expr_node.opcode = TExprOpcode::DIVIDE;
    std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

    MockNullVectorizedExpr<TYPE_INT> col1(expr_node, 10, 10);
    MockNullVectorizedExpr<TYPE_INT> col2(expr_node, 10, 2);
    ++col2.flag;

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr v = col1.evaluate(nullptr, nullptr);
        ASSERT_TRUE(v->is_nullable());

        for (int j = 0; j < v->size(); ++j) {
            for (int j = 0; j < v->size(); ++j) {
                if (j % 2) {
                    ASSERT_TRUE(v->is_null(j));
                } else {
                    ASSERT_FALSE(v->is_null(j));
                }
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

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_TRUE(ptr->is_nullable());
            ASSERT_FALSE(ptr->is_numeric());

            auto v = std::static_pointer_cast<NullableColumn>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(5, std::static_pointer_cast<Int32Column>(v->data_column())->get_data()[j]);
            }

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_TRUE(v->is_null(j));
            }
        });
    }
}

TEST_F(VectorizedArithmeticExprTest, constVectorMulExpr) {
    expr_node.opcode = TExprOpcode::MULTIPLY;
    std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

    MockConstVectorizedExpr<TYPE_INT> col1(expr_node, 10);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 10, 5);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            auto v = std::static_pointer_cast<Int32Column>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(50, v->get_data()[j]);
            }
        });
    }
}

TEST_F(VectorizedArithmeticExprTest, constConstAddExpr) {
    expr_node.opcode = TExprOpcode::ADD;
    std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

    MockConstVectorizedExpr<TYPE_INT> col1(expr_node, 10);
    MockConstVectorizedExpr<TYPE_INT> col2(expr_node, 3);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_TRUE(ptr->is_constant());

            auto v = std::static_pointer_cast<ConstColumn>(ptr)->data_column();
            ASSERT_EQ(1, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(13, std::static_pointer_cast<Int32Column>(v)->get_data()[j]);
            }
        });
    }
}

TEST_F(VectorizedArithmeticExprTest, produceNullVecotConstModExpr) {
    expr_node.opcode = TExprOpcode::MOD;
    std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 10);
    MockConstVectorizedExpr<TYPE_INT> col2(expr_node, 0);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr v = col1.evaluate(nullptr, nullptr);
        ASSERT_FALSE(v->is_nullable());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_FALSE(v->is_null(j));
        }

        ColumnPtr v2 = col2.evaluate(nullptr, nullptr);
        ASSERT_FALSE(v2->is_nullable());
        ASSERT_TRUE(v2->is_constant());
        ASSERT_EQ(1, v2->size());
    }

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_TRUE(ptr->is_nullable());
            ASSERT_FALSE(ptr->is_numeric());

            auto v = std::static_pointer_cast<NullableColumn>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(0, std::static_pointer_cast<Int32Column>(v->data_column())->get_data()[j]);
            }

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_TRUE(v->is_null(j));
            }
        });
    }
}

TEST_F(VectorizedArithmeticExprTest, bitNotExpr) {
    expr_node.opcode = TExprOpcode::BITNOT;
    std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 1);

    {
        expr->_children.push_back(&col1);
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            auto v = std::static_pointer_cast<Int32Column>(ptr);

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_EQ(~1, v->get_data()[j]);
            }

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_FALSE(v->is_null(j));
            }
        });
    }
}

TEST_F(VectorizedArithmeticExprTest, constBitNotExpr) {
    expr_node.opcode = TExprOpcode::BITNOT;
    std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

    MockConstVectorizedExpr<TYPE_INT> col1(expr_node, 2);

    {
        expr->_children.push_back(&col1);
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_TRUE(ptr->is_constant());

            auto re = std::static_pointer_cast<ConstColumn>(ptr);
            auto v = std::static_pointer_cast<Int32Column>(re->data_column());

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_EQ(~2, v->get_data()[j]);
            }

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_FALSE(v->is_null(j));
            }
        });
    }
}

TEST_F(VectorizedArithmeticExprTest, constModExpr) {
    expr_node.opcode = TExprOpcode::MOD;
    expr_node.child_type = TPrimitiveType::BIGINT;
    expr_node.type = gen_type_desc(TPrimitiveType::BIGINT);

    std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

    MockConstVectorizedExpr<TYPE_BIGINT> col1(expr_node, INT64_MIN);
    MockConstVectorizedExpr<TYPE_BIGINT> col2(expr_node, -1);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr v1 = col1.evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(v1, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_FALSE(ptr->is_null(j));
            }
        });

        ColumnPtr v2 = col2.evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(v2, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());
            ASSERT_TRUE(ptr->is_constant());
            ASSERT_EQ(1, ptr->size());
        });
    }

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());
            ASSERT_TRUE(ptr->is_constant());
            ASSERT_FALSE(ptr->is_numeric());

            auto v = std::static_pointer_cast<ConstColumn>(ptr);
            ASSERT_EQ(1, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(0, ColumnHelper::cast_to_raw<TYPE_BIGINT>(v->data_column())->get_data()[j]);
            }

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_FALSE(v->is_null(j));
            }
        });
    }
}

TEST_F(VectorizedArithmeticExprTest, constMod128Expr) {
    expr_node.opcode = TExprOpcode::MOD;
    expr_node.child_type = TPrimitiveType::LARGEINT;
    expr_node.type = gen_type_desc(TPrimitiveType::LARGEINT);

    std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

    MockConstVectorizedExpr<TYPE_LARGEINT> col1(expr_node, MIN_INT128);
    MockConstVectorizedExpr<TYPE_LARGEINT> col2(expr_node, -1);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr v1 = col1.evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(v1, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_FALSE(ptr->is_null(j));
            }
        });

        ColumnPtr v2 = col2.evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(v2, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());
            ASSERT_TRUE(ptr->is_constant());
            ASSERT_EQ(1, ptr->size());
        });
    }

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());
            ASSERT_TRUE(ptr->is_constant());
            ASSERT_FALSE(ptr->is_numeric());

            auto v = std::static_pointer_cast<ConstColumn>(ptr);
            ASSERT_EQ(1, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(0, ColumnHelper::cast_to_raw<TYPE_LARGEINT>(v->data_column())->get_data()[j]);
            }

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_FALSE(v->is_null(j));
            }
        });
    }
}

TEST_F(VectorizedArithmeticExprTest, constModN128Expr) {
    expr_node.opcode = TExprOpcode::MOD;
    expr_node.child_type = TPrimitiveType::LARGEINT;
    expr_node.type = gen_type_desc(TPrimitiveType::LARGEINT);

    std::unique_ptr<Expr> expr(VectorizedArithmeticExprFactory::from_thrift(expr_node));

    MockConstVectorizedExpr<TYPE_LARGEINT> col1(expr_node, MAX_INT128);
    MockConstVectorizedExpr<TYPE_LARGEINT> col2(expr_node, -1);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    {
        ColumnPtr v1 = col1.evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(v1, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_FALSE(ptr->is_null(j));
            }
        });

        ColumnPtr v2 = col2.evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(v2, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());
            ASSERT_TRUE(ptr->is_constant());
            ASSERT_EQ(1, ptr->size());
        });
    }

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());
            ASSERT_TRUE(ptr->is_constant());
            ASSERT_FALSE(ptr->is_numeric());

            auto v = std::static_pointer_cast<ConstColumn>(ptr);
            ASSERT_EQ(1, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(0, ColumnHelper::cast_to_raw<TYPE_LARGEINT>(v->data_column())->get_data()[j]);
            }

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_FALSE(v->is_null(j));
            }
        });
    }
}

} // namespace starrocks
