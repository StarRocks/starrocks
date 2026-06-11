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

#include "exprs/function_call_expr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cmath>

#include "base/testutil/assert.h"
#include "butil/time.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "exprs/cast_expr.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/bloom_filter.h"

namespace starrocks {

class VectorizedFunctionCallExprTest : public ::testing::Test {
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
    RuntimeState _runtime_state;
    TExprNode expr_node;
};

TEST_F(VectorizedFunctionCallExprTest, mathPiExprTest) {
    TFunction function;
    TFunctionName functionName;
    functionName.__set_db_name("db");
    functionName.__set_function_name("pi");

    function.__set_name(functionName);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);

    std::vector<TTypeDesc> vec;
    function.__set_arg_types(vec);
    function.__set_has_var_args(false);
    function.__set_fid(10010);

    expr_node.__set_fn(function);

    VectorizedFunctionCallExpr expr(expr_node);

    ExprContext exprContext(&expr);
    std::vector<ExprContext*> expr_ctxs = {&exprContext};

    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));

    ColumnPtr result = expr.evaluate(&exprContext, nullptr);

    ASSERT_TRUE(result->is_constant());
    ASSERT_FALSE(result->is_numeric());

    {
        double value = ColumnHelper::get_const_value<TYPE_DOUBLE>(result);
        ASSERT_EQ(M_PI, value);
    }

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

TEST_F(VectorizedFunctionCallExprTest, mathModExprTest) {
    TFunction function;
    TFunctionName functionName;
    functionName.__set_db_name("db");
    functionName.__set_function_name("mode");

    function.__set_name(functionName);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);

    std::vector<TTypeDesc> vec;
    function.__set_arg_types(vec);
    function.__set_has_var_args(false);
    function.__set_fid(10252);

    expr_node.__set_fn(function);

    VectorizedFunctionCallExpr expr(expr_node);

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 5, 10);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 5, 3);

    expr.add_child(&col1);
    expr.add_child(&col2);

    ExprContext exprContext(&expr);
    std::vector<ExprContext*> expr_ctxs = {&exprContext};

    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));

    ColumnPtr result = expr.evaluate(&exprContext, nullptr);

    ASSERT_FALSE(result->is_constant());
    ASSERT_FALSE(result->is_numeric());
    ASSERT_TRUE(result->is_nullable());

    {
        auto value = ColumnHelper::cast_to<TYPE_INT>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        for (const int& j : value->get_data()) {
            ASSERT_EQ(1, j);
        }
    }

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

TEST_F(VectorizedFunctionCallExprTest, mathLeastExprTest) {
    TFunction function;
    TFunctionName functionName;
    functionName.__set_db_name("db");
    functionName.__set_function_name("mode");

    function.__set_name(functionName);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);

    std::vector<TTypeDesc> vec;
    function.__set_arg_types(vec);
    function.__set_has_var_args(false);
    function.__set_fid(10282);

    expr_node.__set_fn(function);

    VectorizedFunctionCallExpr expr(expr_node);

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 5, 10);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 5, 3);
    MockVectorizedExpr<TYPE_INT> col3(expr_node, 5, 20);
    MockVectorizedExpr<TYPE_INT> col4(expr_node, 5, 1);
    MockVectorizedExpr<TYPE_INT> col5(expr_node, 5, 15);
    MockVectorizedExpr<TYPE_INT> col6(expr_node, 5, 2);
    MockVectorizedExpr<TYPE_INT> col7(expr_node, 5, 5);

    expr.add_child(&col1);
    expr.add_child(&col2);
    expr.add_child(&col3);
    expr.add_child(&col4);
    expr.add_child(&col5);
    expr.add_child(&col6);
    expr.add_child(&col7);

    ExprContext exprContext(&expr);
    std::vector<ExprContext*> expr_ctxs = {&exprContext};

    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));

    ColumnPtr result = expr.evaluate(&exprContext, nullptr);

    ASSERT_FALSE(result->is_constant());
    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    {
        auto value = ColumnHelper::cast_to<TYPE_INT>(result);

        for (const int& j : value->get_data()) {
            ASSERT_EQ(1, j);
        }
    }

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

TEST_F(VectorizedFunctionCallExprTest, mathNullGreatestExprTest) {
    TFunction function;
    TFunctionName functionName;
    functionName.__set_db_name("db");
    functionName.__set_function_name("mode");

    function.__set_name(functionName);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);

    std::vector<TTypeDesc> vec;
    function.__set_arg_types(vec);
    function.__set_has_var_args(false);
    function.__set_fid(10292);

    expr_node.__set_fn(function);

    VectorizedFunctionCallExpr expr(expr_node);

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 5, 10);
    MockVectorizedExpr<TYPE_INT> col2(expr_node, 5, 3);
    MockNullVectorizedExpr<TYPE_INT> col3(expr_node, 5, 20);
    MockVectorizedExpr<TYPE_INT> col4(expr_node, 5, 1);
    MockVectorizedExpr<TYPE_INT> col5(expr_node, 5, 15);
    MockVectorizedExpr<TYPE_INT> col6(expr_node, 5, 2);
    MockVectorizedExpr<TYPE_INT> col7(expr_node, 5, 5);

    expr.add_child(&col1);
    expr.add_child(&col2);
    expr.add_child(&col3);
    expr.add_child(&col4);
    expr.add_child(&col5);
    expr.add_child(&col6);
    expr.add_child(&col7);

    ExprContext exprContext(&expr);
    std::vector<ExprContext*> expr_ctxs = {&exprContext};

    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));
    ColumnPtr result = expr.evaluate(&exprContext, nullptr);

    ASSERT_FALSE(result->is_constant());
    ASSERT_FALSE(result->is_numeric());
    ASSERT_TRUE(result->is_nullable());

    {
        auto value = ColumnHelper::cast_to<TYPE_INT>(ColumnHelper::as_column<NullableColumn>(result)->data_column());

        for (int k = 0; k < result->size(); ++k) {
            if (k % 2) {
                ASSERT_TRUE(result->is_null(k));
            } else {
                ASSERT_FALSE(result->is_null(k));
            }
        }

        for (const int& j : value->get_data()) {
            ASSERT_EQ(20, j);
        }
    }

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

TEST_F(VectorizedFunctionCallExprTest, prepareFaileCase) {
    TFunction function;
    TFunctionName functionName;
    functionName.__set_db_name("db");
    functionName.__set_function_name("mode");

    function.__set_name(functionName);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);

    std::vector<TTypeDesc> vec;
    function.__set_arg_types(vec);
    function.__set_has_var_args(false);

    expr_node.__set_fn(function);

    VectorizedFunctionCallExpr expr(expr_node);

    MockVectorizedExpr<TYPE_INT> col1(expr_node, 5, 10);

    expr.add_child(&col1);

    ExprContext exprContext(&expr);
    exprContext._is_clone = true;

    ASSERT_FALSE(expr.prepare(nullptr, &exprContext).ok());
    exprContext.close(nullptr);
}

TEST_F(VectorizedFunctionCallExprTest, prepare_close) {
    TFunction func;
    func.__set_fid(60010); // like
    func.__set_binary_type(TFunctionBinaryType::BUILTIN);

    TExprNode expr_node;
    expr_node.__set_fn(func);
    expr_node.__set_opcode(TExprOpcode::ADD);
    expr_node.__set_child_type(TPrimitiveType::INT);
    expr_node.__set_node_type(TExprNodeType::BINARY_PRED);
    expr_node.__set_num_children(2);
    expr_node.__set_type(gen_type_desc(TPrimitiveType::BOOLEAN));

    VectorizedFunctionCallExpr expr(expr_node);
    ColumnRef col1(TypeDescriptor::create_varbinary_type(10), 1);
    ColumnRef col2(TypeDescriptor::create_varbinary_type(10), 2);
    expr.add_child(&col1);
    expr.add_child(&col2);

    ExprContext expr_context(&expr);
    Status st = expr_context.prepare(&_runtime_state);
    ASSERT_TRUE(st.ok());
    st = expr_context.open(&_runtime_state);
    ASSERT_TRUE(st.ok());
    expr_context.close(&_runtime_state);
}

// ---------------------------------------------------------------------------
// ngram_bloom_filter pushdown helper: validates needle, lowers it via the
// ICU/ASCII path matching the writer, and probes the bloom filter. These
// tests cover the index-side reader path for ngram_bf with UTF-8 needles.
// ---------------------------------------------------------------------------

class NgramBloomFilterPushdownTest : public ::testing::Test {
protected:
    static TExprNode make_typed_node(TPrimitiveType::type t) {
        TExprNode n;
        n.node_type = TExprNodeType::SLOT_REF;
        n.num_children = 0;
        n.type = gen_type_desc(t);
        return n;
    }

    TExprNode build_ngram_call_node() {
        TFunction function;
        TFunctionName functionName;
        functionName.__set_function_name("ngram_search_case_insensitive");
        function.__set_name(functionName);
        function.__set_binary_type(TFunctionBinaryType::BUILTIN);
        function.__set_fid(30441);
        function.__set_has_var_args(false);
        std::vector<TTypeDesc> arg_types = {
                gen_type_desc(TPrimitiveType::VARCHAR),
                gen_type_desc(TPrimitiveType::VARCHAR),
                gen_type_desc(TPrimitiveType::INT),
        };
        function.__set_arg_types(arg_types);
        function.__set_ret_type(gen_type_desc(TPrimitiveType::DOUBLE));

        TExprNode parent;
        parent.node_type = TExprNodeType::FUNCTION_CALL;
        parent.num_children = 3;
        parent.type = gen_type_desc(TPrimitiveType::DOUBLE);
        parent.__set_fn(function);
        return parent;
    }

    std::unique_ptr<BloomFilter> make_bf_with_cyrillic_lowered_trigrams() {
        std::unique_ptr<BloomFilter> bf;
        EXPECT_OK(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
        EXPECT_OK(bf->init(16, 0.05, HashStrategyPB::HASH_MURMUR3_X64_64));
        // Lowercase Cyrillic character trigrams of "привет" (each char is 2 bytes).
        bf->add_bytes("при", 6);
        bf->add_bytes("рив", 6);
        bf->add_bytes("иве", 6);
        bf->add_bytes("вет", 6);
        return bf;
    }

    RuntimeState _runtime_state;
};

TEST_F(NgramBloomFilterPushdownTest, MatchUtf8CaseInsensitiveLowersNeedle) {
    TExprNode varchar_node = make_typed_node(TPrimitiveType::VARCHAR);
    TExprNode int_node = make_typed_node(TPrimitiveType::INT);
    TExprNode parent_node = build_ngram_call_node();

    VectorizedFunctionCallExpr expr(parent_node);
    MockColumnExpr haystack(varchar_node, BinaryColumn::create());
    MockConstVectorizedExpr<TYPE_VARCHAR> needle(varchar_node, "ПРИВЕТ");
    MockConstVectorizedExpr<TYPE_INT> gram_num(int_node, 3);
    expr.add_child(&haystack);
    expr.add_child(&needle);
    expr.add_child(&gram_num);

    ExprContext exprContext(&expr);
    std::vector<ExprContext*> expr_ctxs = {&exprContext};
    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));

    auto bf = make_bf_with_cyrillic_lowered_trigrams();
    NgramBloomFilterReaderOptions opts;
    opts.index_gram_num = 3;
    opts.index_case_sensitive = false;

    // Needle "ПРИВЕТ" lowered via utf8_tolower to "привет"; all 4 trigrams hit
    // the bloom filter, so the helper must report the page as a candidate.
    EXPECT_TRUE(expr.ngram_bloom_filter(&exprContext, bf.get(), opts));

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

TEST_F(NgramBloomFilterPushdownTest, MissUtf8CaseInsensitiveFiltersPage) {
    TExprNode varchar_node = make_typed_node(TPrimitiveType::VARCHAR);
    TExprNode int_node = make_typed_node(TPrimitiveType::INT);
    TExprNode parent_node = build_ngram_call_node();

    VectorizedFunctionCallExpr expr(parent_node);
    MockColumnExpr haystack(varchar_node, BinaryColumn::create());
    MockConstVectorizedExpr<TYPE_VARCHAR> needle(varchar_node, "ПОКА");
    MockConstVectorizedExpr<TYPE_INT> gram_num(int_node, 3);
    expr.add_child(&haystack);
    expr.add_child(&needle);
    expr.add_child(&gram_num);

    ExprContext exprContext(&expr);
    std::vector<ExprContext*> expr_ctxs = {&exprContext};
    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));

    auto bf = make_bf_with_cyrillic_lowered_trigrams();
    NgramBloomFilterReaderOptions opts;
    opts.index_gram_num = 3;
    opts.index_case_sensitive = false;

    // Needle "ПОКА" lowered to "пока" produces trigrams "пок", "ока" — neither
    // present in the bloom filter that was populated for "привет".
    EXPECT_FALSE(expr.ngram_bloom_filter(&exprContext, bf.get(), opts));

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

TEST_F(NgramBloomFilterPushdownTest, InvalidUtf8NeedleDisablesIndex) {
    TExprNode varchar_node = make_typed_node(TPrimitiveType::VARCHAR);
    TExprNode int_node = make_typed_node(TPrimitiveType::INT);
    TExprNode parent_node = build_ngram_call_node();

    VectorizedFunctionCallExpr expr(parent_node);
    MockColumnExpr haystack(varchar_node, BinaryColumn::create());
    // 0xC0 0xC1 are illegal lead bytes in UTF-8.
    MockConstVectorizedExpr<TYPE_VARCHAR> needle(varchar_node, std::string("\xC0\xC1\xFE", 3));
    MockConstVectorizedExpr<TYPE_INT> gram_num(int_node, 3);
    expr.add_child(&haystack);
    expr.add_child(&needle);
    expr.add_child(&gram_num);

    ExprContext exprContext(&expr);
    std::vector<ExprContext*> expr_ctxs = {&exprContext};
    ASSERT_OK(ExprExecutor::prepare(expr_ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(expr_ctxs, &_runtime_state));

    auto bf = make_bf_with_cyrillic_lowered_trigrams();
    NgramBloomFilterReaderOptions opts;
    opts.index_gram_num = 3;
    opts.index_case_sensitive = false;

    // Invalid UTF-8 needle: helper short-circuits with index_useful=false and
    // returns true so the page is not filtered out by the bloom filter.
    EXPECT_TRUE(expr.ngram_bloom_filter(&exprContext, bf.get(), opts));

    ExprExecutor::close(expr_ctxs, &_runtime_state);
}

} // namespace starrocks
