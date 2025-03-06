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
#include "exprs/exprs_test_helper.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/runtime_state.h"

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
    RuntimeState runtime_state;
    TExprNode expr_node;
};

TEST_F(VectorizedBinaryPredicateTest, eqExpr) {
    expr_node.opcode = TExprOpcode::EQ;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 10, 0);
    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    // normal int8
    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(
                ptr, expr.get(), &runtime_state,
                [](ColumnPtr const& ptr) {
                    ASSERT_FALSE(ptr->is_nullable());
                    ASSERT_TRUE(ptr->is_numeric());

                    auto v = BooleanColumn::static_pointer_cast(ptr);
                    ASSERT_EQ(10, v->size());

                    for (int j = 0; j < v->size(); ++j) {
                        ASSERT_EQ(0, (int)v->get_data()[j]);
                    }
                },
                expr->is_compilable(&runtime_state));
    }
}

TEST_F(VectorizedBinaryPredicateTest, neExpr) {
    expr_node.opcode = TExprOpcode::NE;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 10, 0);

    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    // normal int8
    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(
                ptr, expr.get(), &runtime_state,
                [](ColumnPtr const& ptr) {
                    ASSERT_FALSE(ptr->is_nullable());
                    ASSERT_TRUE(ptr->is_numeric());

                    auto v = BooleanColumn::static_pointer_cast(ptr);
                    ASSERT_EQ(10, v->size());

                    for (int j = 0; j < v->size(); ++j) {
                        ASSERT_EQ(1, (int)v->get_data()[j]);
                    }
                },
                expr->is_compilable(&runtime_state));
    }
}

TEST_F(VectorizedBinaryPredicateTest, geExpr) {
    expr_node.opcode = TExprOpcode::GE;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 10, 0);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    // normal int8
    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(
                ptr, expr.get(), &runtime_state,
                [](ColumnPtr const& ptr) {
                    ASSERT_FALSE(ptr->is_nullable());
                    ASSERT_TRUE(ptr->is_numeric());

                    auto v = BooleanColumn::static_pointer_cast(ptr);
                    ASSERT_EQ(10, v->size());

                    for (int j = 0; j < v->size(); ++j) {
                        ASSERT_EQ(1, v->get_data()[j]);
                    }
                },
                expr->is_compilable(&runtime_state));
    }
}

TEST_F(VectorizedBinaryPredicateTest, nullLtExpr) {
    expr_node.opcode = TExprOpcode::LT;
    expr_node.child_type = TPrimitiveType::BOOLEAN;
    expr_node.is_nullable = true;
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
        ExprsTestHelper::verify_with_jit(
                v, expr.get(), &runtime_state,
                [](ColumnPtr const& v) {
                    ColumnPtr ptr = NullableColumn::static_pointer_cast(v)->data_column();

                    ASSERT_TRUE(v->is_nullable());
                    ASSERT_FALSE(v->is_numeric());

                    for (int j = 0; j < ptr->size(); ++j) {
                        ASSERT_EQ(0, (int)BooleanColumn::static_pointer_cast(ptr)->get_data()[j]);
                    }

                    for (int j = 0; j < ptr->size(); ++j) {
                        ASSERT_TRUE(v->is_null(j));
                    }
                },
                expr->is_compilable(&runtime_state));
    }
}

TEST_F(VectorizedBinaryPredicateTest, mergeNullLtExpr) {
    expr_node.opcode = TExprOpcode::LT;
    expr_node.child_type = TPrimitiveType::BOOLEAN;
    expr_node.is_nullable = true;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));

    expr_node.is_nullable = false;
    MockVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 0);
    expr_node.is_nullable = true;
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
        ExprsTestHelper::verify_with_jit(
                v, expr.get(), &runtime_state,
                [](ColumnPtr const& v) {
                    ColumnPtr ptr = NullableColumn::static_pointer_cast(v)->data_column();

                    ASSERT_TRUE(v->is_nullable());
                    ASSERT_FALSE(v->is_numeric());

                    for (int j = 0; j < ptr->size(); ++j) {
                        ASSERT_EQ(1, (int)BooleanColumn::static_pointer_cast(ptr)->get_data()[j]);
                    }

                    for (int j = 0; j < ptr->size(); ++j) {
                        if (j % 2) {
                            ASSERT_FALSE(v->is_null(j));
                        } else {
                            ASSERT_TRUE(v->is_null(j));
                        }
                    }
                },
                expr->is_compilable(&runtime_state));
    }
}

TEST_F(VectorizedBinaryPredicateTest, eqForNullExpr) {
    expr_node.opcode = TExprOpcode::EQ_FOR_NULL;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    expr_node.is_nullable = false;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 10, 1);

    expr->_children.clear();
    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);

    // normal int8
    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(
                ptr, expr.get(), &runtime_state,
                [](ColumnPtr const& ptr) {
                    ASSERT_FALSE(ptr->is_nullable());
                    ASSERT_TRUE(ptr->is_numeric());

                    auto v = BooleanColumn::static_pointer_cast(ptr);
                    ASSERT_EQ(10, v->size());

                    for (int j = 0; j < v->size(); ++j) {
                        ASSERT_EQ(1, v->get_data()[j]);
                    }
                },
                expr->is_compilable(&runtime_state));
    }
}

TEST_F(VectorizedBinaryPredicateTest, nullEqForNullExpr) {
    expr_node.opcode = TExprOpcode::EQ_FOR_NULL;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    expr_node.child_type = TPrimitiveType::BOOLEAN;
    expr_node.is_nullable = false;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
    expr_node.is_nullable = true;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    MockNullVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 0);
    expr_node.is_nullable = true;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    MockNullVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 10, 1);

    expr->_children.clear();
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
            ASSERT_EQ(0, (int)BooleanColumn::static_pointer_cast(ptr)->get_data()[j]);
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
        ExprsTestHelper::verify_with_jit(
                v, expr.get(), &runtime_state,
                [](ColumnPtr const& v) {
                    auto ptr = BooleanColumn::static_pointer_cast(v);

                    ASSERT_FALSE(v->is_nullable());
                    ASSERT_TRUE(v->is_numeric());

                    for (int j = 0; j < ptr->size(); ++j) {
                        if (j % 2) {
                            ASSERT_EQ(1, (int)ptr->get_data()[j]);
                        } else {
                            ASSERT_EQ(0, (int)ptr->get_data()[j]);
                        }
                    }
                },
                expr->is_compilable(&runtime_state));
    }
}

TEST_F(VectorizedBinaryPredicateTest, nullAndNotNullEqForNullExpr) {
    expr_node.opcode = TExprOpcode::EQ_FOR_NULL;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    expr_node.child_type = TPrimitiveType::BOOLEAN;
    expr_node.is_nullable = false;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    expr_node.is_nullable = true;
    MockNullVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, 1);
    expr_node.is_nullable = false;
    MockVectorizedExpr<TYPE_BOOLEAN> col2(expr_node, 10, 1);

    expr->_children.clear();
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
        ColumnPtr v = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(
                v, expr.get(), &runtime_state,
                [](ColumnPtr const& v) {
                    auto ptr = BooleanColumn::static_pointer_cast(v);

                    ASSERT_FALSE(v->is_nullable());
                    ASSERT_TRUE(v->is_numeric());

                    for (int j = 0; j < ptr->size(); ++j) {
                        if (j % 2) {
                            ASSERT_EQ(0, (int)ptr->get_data()[j]);
                        } else {
                            ASSERT_EQ(1, (int)ptr->get_data()[j]);
                        }
                    }
                },
                expr->is_compilable(&runtime_state));
    }
}

TEST_F(VectorizedBinaryPredicateTest, diffNullEqForNullExpr) {
    expr_node.opcode = TExprOpcode::EQ_FOR_NULL;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    expr_node.is_nullable = false;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    expr_node.is_nullable = true;
    MockNullVectorizedExpr<TYPE_INT> col1(expr_node, 10, 1);
    MockNullVectorizedExpr<TYPE_INT> col2(expr_node, 10, 1);
    col2.flag++;
    expr->_children.clear();
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
        ExprsTestHelper::verify_with_jit(
                v, expr.get(), &runtime_state,
                [](ColumnPtr const& v) {
                    auto ptr = BooleanColumn::static_pointer_cast(v);

                    ASSERT_FALSE(v->is_nullable());
                    ASSERT_TRUE(v->is_numeric());

                    for (int j = 0; j < ptr->size(); ++j) {
                        ASSERT_EQ(0, (int)ptr->get_data()[j]);
                    }
                },
                expr->is_compilable(&runtime_state));
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

    auto v = BooleanColumn::static_pointer_cast(ptr);
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

    auto v = BooleanColumn::static_pointer_cast(ptr);
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

    auto v = BooleanColumn::static_pointer_cast(ptr);
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

    auto v = BooleanColumn::static_pointer_cast(ptr);
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

class VectorizedBinaryPredicateArrayTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::INVALID_OPCODE;
        expr_node.child_type = TPrimitiveType::VARCHAR;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);

        std::vector<TTypeNode> types_list;

        TTypeNode type_array;
        type_array.type = TTypeNodeType::ARRAY;
        types_list.push_back(type_array);

        TTypeNode type_scalar;
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::INT);
        type_scalar.__set_scalar_type(scalar_type);
        types_list.push_back(type_scalar);

        TTypeDesc type_desc;
        type_desc.__set_types(types_list);

        expr_node.__set_child_type_desc(type_desc);
    }

    FakeConstExpr* new_fake_const_expr(MutableColumnPtr&& value, const TypeDescriptor& type) {
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
    ObjectPool _objpool;
};

TEST_F(VectorizedBinaryPredicateArrayTest, arrayGT) {
    expr_node.opcode = TExprOpcode::GT;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));

    TypeDescriptor type_arr_int = array_type(TYPE_INT);
    auto array0 = ColumnHelper::create_column(type_arr_int, true);
    array0->append_datum(DatumArray{Datum((int32_t)11), Datum((int32_t)4)}); // [11,4]
    array0->append_datum(DatumArray{Datum(), Datum()});                      // [NULL, NULL]
    array0->append_datum(DatumArray{Datum(), Datum((int32_t)1)});            // [NULL, 1]
    auto array_expr0 = MockExpr(type_arr_int, std::move(array0));

    auto array1 = ColumnHelper::create_column(type_arr_int, false);
    array1->append_datum(DatumArray{Datum((int32_t)1), Datum((int32_t)4)}); // [1,4]
    array1->append_datum(DatumArray{Datum(), Datum()});                     // [NULL, NULL]
    array1->append_datum(DatumArray{Datum(), Datum((int32_t)1)});           // [NULL, 1]
    auto array_expr1 = MockExpr(type_arr_int, std::move(array1));
    expr->add_child(&array_expr0);
    expr->add_child(&array_expr1);

    ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
    ASSERT_FALSE(ptr->is_nullable());

    auto v = BooleanColumn::static_pointer_cast(ptr);
    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]); // TODO: should be null
    ASSERT_FALSE(v->get_data()[2]); // TODO: should be null
}

TEST_F(VectorizedBinaryPredicateArrayTest, arrayConstGT) {
    expr_node.opcode = TExprOpcode::GT;
    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));

    TypeDescriptor type_arr_int = array_type(TYPE_INT);
    auto array0 = ColumnHelper::create_column(type_arr_int, true);
    array0->append_datum(DatumArray{Datum((int32_t)11), Datum((int32_t)4)}); // [11,4]
    array0->append_datum(DatumArray{Datum(), Datum()});                      // [NULL, NULL]
    array0->append_datum(DatumArray{Datum(), Datum((int32_t)1)});            // [NULL, 1]
    auto array_expr0 = MockExpr(type_arr_int, std::move(array0));

    auto array = ColumnHelper::create_column(type_arr_int, false);
    array->append_datum(DatumArray{Datum((int32_t)1), Datum((int32_t)4)}); // [1,4]
    auto const_col = ConstColumn::create(std::move(array), 3);
    auto* const_array = new_fake_const_expr(std::move(const_col), type_arr_int);
    expr->add_child(&array_expr0);
    expr->add_child(const_array);

    ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
    ASSERT_FALSE(ptr->is_nullable());

    auto v = BooleanColumn::static_pointer_cast(ptr);
    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_TRUE(v->get_data()[1]); // TODO: should be null
    ASSERT_TRUE(v->get_data()[2]); // TODO: should be null
}

class VectorizedBinaryPredicateMapTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::INVALID_OPCODE;
        expr_node.child_type = TPrimitiveType::VARCHAR;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);

        std::vector<TTypeNode> types_list;

        TTypeNode type_map;
        type_map.type = TTypeNodeType::MAP;
        types_list.push_back(type_map);

        TTypeNode type_scalar;
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::INT);
        type_scalar.__set_scalar_type(scalar_type);
        types_list.push_back(type_scalar);
        types_list.push_back(type_scalar);

        TTypeDesc type_desc;
        type_desc.__set_types(types_list);

        expr_node.__set_child_type_desc(type_desc);
    }

public:
    TExprNode expr_node;
};

TEST_F(VectorizedBinaryPredicateMapTest, mapEqExpr1) {
    expr_node.opcode = TExprOpcode::EQ;

    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
    auto col1 = ColumnHelper::create_column(map_type(LogicalType::TYPE_INT, LogicalType::TYPE_INT), true);
    DatumMap map1;
    map1[(int32_t)1] = (int32_t)1;
    map1[(int32_t)2] = (int32_t)2;
    col1->append_datum(map1);

    MockColumnExpr expr1(expr_node, std::move(col1));
    expr->add_child(&expr1);

    auto col2 = ColumnHelper::create_column(map_type(LogicalType::TYPE_INT, LogicalType::TYPE_INT), true);
    DatumMap map2;
    map2[(int32_t)1] = (int32_t)1;
    map2[(int32_t)2] = (int32_t)2;
    col2->append_datum(map2);

    MockColumnExpr expr2(expr_node, std::move(col2));
    expr->add_child(&expr2);

    ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
    ASSERT_FALSE(ptr->is_nullable());
    ASSERT_TRUE(ptr->is_numeric());

    auto v = BooleanColumn::static_pointer_cast(ptr);
    for (int j = 0; j < v->size(); ++j) {
        ASSERT_TRUE(v->get_data()[j]);
    }
}

TEST_F(VectorizedBinaryPredicateMapTest, mapEqExpr2) {
    expr_node.opcode = TExprOpcode::EQ;

    std::unique_ptr<Expr> expr(VectorizedBinaryPredicateFactory::from_thrift(expr_node));
    auto col1 = ColumnHelper::create_column(map_type(LogicalType::TYPE_INT, LogicalType::TYPE_INT), true);
    DatumMap map1;
    map1[(int32_t)1] = (int32_t)1;
    map1[(int32_t)2] = (int32_t)2;
    col1->append_datum(map1);

    MockColumnExpr expr1(expr_node, std::move(col1));
    expr->add_child(&expr1);

    auto col2 = ColumnHelper::create_column(map_type(LogicalType::TYPE_INT, LogicalType::TYPE_INT), true);
    DatumMap map2;
    map2[(int32_t)3] = (int32_t)1;
    map2[(int32_t)4] = (int32_t)2;
    col2->append_datum(map2);

    MockColumnExpr expr2(expr_node, std::move(col2));
    expr->add_child(&expr2);

    ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
    ASSERT_FALSE(ptr->is_nullable());
    ASSERT_TRUE(ptr->is_numeric());

    auto v = BooleanColumn::static_pointer_cast(ptr);
    for (int j = 0; j < v->size(); ++j) {
        ASSERT_FALSE(v->get_data()[j]);
    }
}

} // namespace starrocks
