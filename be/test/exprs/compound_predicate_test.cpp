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

#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "column/fixed_length_column.h"
#include "exprs/expr_context.h"
#include "exprs/exprs_test_helper.h"
#include "exprs/function_call_expr.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "util/bloom_filter.h"

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

class NgramBloomFilterNotPredicateTest : public ::testing::Test {
protected:
    void SetUp() override {
        ASSERT_OK(BloomFilter::create(BLOCK_BLOOM_FILTER, &_bf));
        ASSERT_OK(_bf->init(64, 0.01, HashStrategyPB::HASH_MURMUR3_X64_64));
        _options.index_gram_num = 4;
        _options.index_case_sensitive = true;
    }

    void TearDown() override {
        if (_context != nullptr) {
            _context->close(&_state);
        }
    }

    ColumnPtr make_input(std::initializer_list<const char*> values) {
        ColumnBuilder<TYPE_VARCHAR> builder(values.size());
        for (const char* value : values) {
            if (value == nullptr) {
                builder.append_null();
                continue;
            }
            Slice slice(value);
            builder.append(slice);
            // These test inputs are ASCII, so each four-byte window is a gram.
            for (size_t i = 0; i + _options.index_gram_num <= slice.size; ++i) {
                _bf->add_bytes(slice.data + i, _options.index_gram_num);
            }
        }
        _input = builder.build(false);
        return _input;
    }

    Expr* make_like(const ColumnPtr& input, const std::string& pattern) {
        TFunction function;
        TFunctionName name;
        name.__set_function_name("LIKE");
        function.__set_name(name);
        function.__set_binary_type(TFunctionBinaryType::BUILTIN);
        function.__set_fid(60010);
        function.__set_has_var_args(false);
        function.__set_arg_types({gen_type_desc(TPrimitiveType::VARCHAR), gen_type_desc(TPrimitiveType::VARCHAR)});
        function.__set_ret_type(gen_type_desc(TPrimitiveType::BOOLEAN));

        TExprNode node;
        node.node_type = TExprNodeType::FUNCTION_CALL;
        node.num_children = 2;
        node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
        node.__set_fn(function);
        auto* like = _pool.add(new VectorizedFunctionCallExpr(node));

        TExprNode argument;
        argument.node_type = TExprNodeType::SLOT_REF;
        argument.type = gen_type_desc(TPrimitiveType::VARCHAR);
        like->add_child(_pool.add(new MockColumnExpr(argument, input)));
        like->add_child(_pool.add(new MockConstVectorizedExpr<TYPE_VARCHAR>(argument, pattern)));
        return like;
    }

    Expr* make_compound(TExprOpcode::type opcode, std::initializer_list<Expr*> children) {
        TExprNode node;
        node.node_type = TExprNodeType::COMPOUND_PRED;
        node.__set_opcode(opcode);
        node.num_children = children.size();
        node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
        auto* expr = _pool.add(VectorizedCompoundPredicateFactory::from_thrift(node));
        for (Expr* child : children) {
            expr->add_child(child);
        }
        return expr;
    }

    void prepare(Expr* root) {
        _context = std::make_unique<ExprContext>(root);
        ASSERT_OK(_context->prepare(&_state));
        ASSERT_OK(_context->open(&_state));
    }

    void check_rows(std::initializer_list<int> expected) {
        Chunk chunk;
        chunk.append_column(_input, 0);
        auto result = _context->evaluate(&chunk);
        ASSERT_OK(result);
        ASSERT_EQ(expected.size(), result.value()->size());
        ColumnViewer<TYPE_BOOLEAN> viewer(result.value());
        size_t row = 0;
        for (int value : expected) {
            EXPECT_EQ(value < 0, viewer.is_null(row)) << "row " << row;
            if (value >= 0) {
                EXPECT_EQ(value, viewer.value(row)) << "row " << row;
            }
            ++row;
        }
    }

    RuntimeState _state;
    ObjectPool _pool;
    std::unique_ptr<ExprContext> _context;
    std::unique_ptr<BloomFilter> _bf;
    ColumnPtr _input;
    NgramBloomFilterReaderOptions _options;
};

TEST_F(NgramBloomFilterNotPredicateTest, MissingGramsKeepNonmatchingRows) {
    auto* like = make_like(make_input({"alpha", "", nullptr}), "%30:30%");
    auto* not_like = make_compound(TExprOpcode::COMPOUND_NOT, {like});
    ASSERT_NO_FATAL_FAILURE(prepare(not_like));

    ASSERT_TRUE(like->support_ngram_bloom_filter(_context.get()));
    ASSERT_FALSE(like->ngram_bloom_filter(_context.get(), _bf.get(), _options));
    EXPECT_FALSE(_context->support_ngram_bloom_filter());
    EXPECT_TRUE(_context->ngram_bloom_filter(_bf.get(), _options));
    check_rows({1, 1, -1});
}

TEST_F(NgramBloomFilterNotPredicateTest, MatchingGramsKeepMixedPage) {
    auto* like = make_like(make_input({"prefix30:30suffix", "alpha", "", nullptr}), "%30:30%");
    ASSERT_NO_FATAL_FAILURE(prepare(make_compound(TExprOpcode::COMPOUND_NOT, {like})));

    ASSERT_TRUE(like->ngram_bloom_filter(_context.get(), _bf.get(), _options));
    EXPECT_FALSE(_context->support_ngram_bloom_filter());
    // A positive bloom-filter probe cannot be inverted: some rows still match NOT LIKE.
    EXPECT_TRUE(_context->ngram_bloom_filter(_bf.get(), _options));
    check_rows({0, 1, 1, -1});
}

TEST_F(NgramBloomFilterNotPredicateTest, AllMatchingRowsAreRejectedByRowEvaluation) {
    auto* like = make_like(make_input({"30:30", "prefix30:30suffix"}), "%30:30%");
    ASSERT_NO_FATAL_FAILURE(prepare(make_compound(TExprOpcode::COMPOUND_NOT, {like})));

    ASSERT_TRUE(like->ngram_bloom_filter(_context.get(), _bf.get(), _options));
    EXPECT_FALSE(_context->support_ngram_bloom_filter());
    EXPECT_TRUE(_context->ngram_bloom_filter(_bf.get(), _options));
    check_rows({0, 0});
}

TEST_F(NgramBloomFilterNotPredicateTest, NullOnlyPagePreservesNullSemantics) {
    auto* like = make_like(make_input({nullptr, nullptr}), "%30:30%");
    ASSERT_NO_FATAL_FAILURE(prepare(make_compound(TExprOpcode::COMPOUND_NOT, {like})));

    EXPECT_FALSE(_context->support_ngram_bloom_filter());
    EXPECT_TRUE(_context->ngram_bloom_filter(_bf.get(), _options));
    check_rows({-1, -1});
}

TEST_F(NgramBloomFilterNotPredicateTest, NestedNegationsKeepPage) {
    Expr* root = make_like(make_input({"alpha", nullptr}), "%30:30%");
    for (int i = 0; i < 3; ++i) {
        root = make_compound(TExprOpcode::COMPOUND_NOT, {root});
    }
    ASSERT_NO_FATAL_FAILURE(prepare(root));

    EXPECT_FALSE(_context->support_ngram_bloom_filter());
    EXPECT_TRUE(_context->ngram_bloom_filter(_bf.get(), _options));
    check_rows({1, -1});
}

TEST_F(NgramBloomFilterNotPredicateTest, NegatedDisjunctionKeepsPage) {
    auto input = make_input({"alpha", "", nullptr});
    auto* either_like =
            make_compound(TExprOpcode::COMPOUND_OR, {make_like(input, "%30:30%"), make_like(input, "%missing%")});
    ASSERT_NO_FATAL_FAILURE(prepare(make_compound(TExprOpcode::COMPOUND_NOT, {either_like})));

    EXPECT_FALSE(_context->support_ngram_bloom_filter());
    EXPECT_TRUE(_context->ngram_bloom_filter(_bf.get(), _options));
    check_rows({1, 1, -1});
}

TEST_F(NgramBloomFilterNotPredicateTest, ConjunctionDoesNotPruneUsingNegatedChild) {
    auto input = make_input({"keep", "skip", nullptr});
    auto* not_like = make_compound(TExprOpcode::COMPOUND_NOT, {make_like(input, "%30:30%")});
    ASSERT_NO_FATAL_FAILURE(prepare(make_compound(TExprOpcode::COMPOUND_AND, {not_like, make_like(input, "%keep%")})));

    // The positive LIKE enables bloom-filter evaluation of the parent. Its
    // recursive evaluation must still leave the NOT child's page unpruned.
    EXPECT_TRUE(_context->support_ngram_bloom_filter());
    EXPECT_TRUE(_context->ngram_bloom_filter(_bf.get(), _options));
    check_rows({1, 0, -1});
}

TEST_F(NgramBloomFilterNotPredicateTest, ConjunctionStillPrunesUsingPositiveChild) {
    auto input = make_input({"alpha", "", nullptr});
    auto* not_like = make_compound(TExprOpcode::COMPOUND_NOT, {make_like(input, "%30:30%")});
    ASSERT_NO_FATAL_FAILURE(prepare(make_compound(TExprOpcode::COMPOUND_AND, {not_like, make_like(input, "%keep%")})));

    EXPECT_TRUE(_context->support_ngram_bloom_filter());
    EXPECT_FALSE(_context->ngram_bloom_filter(_bf.get(), _options));
    check_rows({0, 0, -1});
}

} // namespace starrocks
