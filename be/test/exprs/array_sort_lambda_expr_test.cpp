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

#include "exprs/array_sort_lambda_expr.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "common/object_pool.h"
#include "exprs/binary_predicate.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "exprs/lambda_function.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks {

class ArraySortLambdaExprTest : public ::testing::Test {
protected:
    static constexpr SlotId kArgX = 100000;
    static constexpr SlotId kArgY = 100001;
    static constexpr size_t kNumRows = 3;

    // An array<int> column with kNumRows rows of [4, 1, 3]. Non-nullable, so evaluation never
    // has to touch the shared column in place.
    Expr* make_array_expr() {
        auto array = ColumnHelper::create_column(TYPE_INT_ARRAY_DESC, false);
        for (size_t i = 0; i < kNumRows; ++i) {
            array->append_datum(DatumArray{Datum((int32_t)4), Datum((int32_t)1), Datum((int32_t)3)});
        }
        TExprNode node;
        node.__set_node_type(TExprNodeType::INT_LITERAL);
        node.__set_num_children(0);
        node.__set_type(TYPE_INT_ARRAY_DESC.to_thrift());
        auto* expr = _pool.add(new FakeConstExpr(node));
        expr->_column = std::move(array);
        return expr;
    }

    ColumnRef* make_arg_ref(SlotId slot_id) {
        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = gen_type_desc(TPrimitiveType::INT);
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = slot_id;
        slot_ref.slot_ref.tuple_id = 0;
        slot_ref.__set_is_nullable(true);
        return _pool.add(new ColumnRef(slot_ref));
    }

    // (x, y) -> x <op> y
    LambdaFunction* make_comparator(TExprOpcode::type op) {
        TExprNode pred_node;
        pred_node.node_type = TExprNodeType::BINARY_PRED;
        pred_node.opcode = op;
        pred_node.child_type = TPrimitiveType::INT;
        pred_node.num_children = 2;
        pred_node.__isset.opcode = true;
        pred_node.__isset.child_type = true;
        pred_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
        Expr* pred = _pool.add(VectorizedBinaryPredicateFactory::from_thrift(pred_node));
        pred->add_child(make_arg_ref(kArgX));
        pred->add_child(make_arg_ref(kArgY));

        TExprNode lambda_node;
        lambda_node.node_type = TExprNodeType::LAMBDA_FUNCTION_EXPR;
        lambda_node.num_children = 3;
        lambda_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
        auto* lambda = _pool.add(new LambdaFunction(lambda_node));
        lambda->add_child(pred);
        lambda->add_child(make_arg_ref(kArgX));
        lambda->add_child(make_arg_ref(kArgY));
        return lambda;
    }

    // array_sort(array, (x, y) -> x <op> y)
    ArraySortLambdaExpr* make_array_sort(TExprOpcode::type op) {
        auto* expr = _pool.add(new ArraySortLambdaExpr(TYPE_INT_ARRAY_DESC));
        expr->add_child(make_array_expr());
        expr->add_child(make_comparator(op));
        return expr;
    }

    // A chunk whose row count matches the array column (the array itself is produced by the
    // fake const expr, so the chunk only needs a column for the row count).
    static std::shared_ptr<Chunk> make_chunk() {
        auto chunk = std::make_shared<Chunk>();
        auto rows = Int32Column::create();
        for (size_t i = 0; i < kNumRows; ++i) {
            rows->append(static_cast<int32_t>(i));
        }
        chunk->append_column(std::move(rows), 1);
        return chunk;
    }

    // Evaluate `expr` from `num_threads` threads at once, each through its own cloned
    // ExprContext, the way the pipeline drivers of one fragment share a single Expr tree.
    template <typename Check>
    void evaluate_concurrently(ArraySortLambdaExpr* expr, int num_threads, int iterations, Check check) {
        ExprContext root_ctx(expr);
        std::vector<ExprContext*> ctxs = {&root_ctx};
        ASSERT_OK(ExprExecutor::prepare(ctxs, &_runtime_state));
        ASSERT_OK(ExprExecutor::open(ctxs, &_runtime_state));

        std::vector<ExprContext*> clones(num_threads, nullptr);
        for (auto& clone : clones) {
            ASSERT_OK(root_ctx.clone(&_runtime_state, &_pool, &clone));
        }

        std::atomic<int> failures{0};
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                auto chunk = make_chunk();
                for (int i = 0; i < iterations; ++i) {
                    // Drop the "already validated" mark so that every iteration re-runs the
                    // validation and re-publishes its verdict while the other threads read it;
                    // otherwise only the first pass of each thread would exercise that window.
                    expr->_comparator_validated = false;
                    if (!check(clones[t]->evaluate(expr, chunk.get()))) {
                        failures.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        ASSERT_EQ(0, failures.load());

        for (auto* clone : clones) {
            ExprExecutor::close(clone, &_runtime_state);
        }
        ExprExecutor::close(ctxs, &_runtime_state);
    }

    RuntimeState _runtime_state;
    ObjectPool _pool;
};

// The comparator validation used to cache its verdict, including an error Status, in members
// of the Expr. The Expr tree is shared by every driver of a fragment, so with an invalid
// comparator one driver assigned the cached Status (freeing its previous state) while another
// copied it out to return it: a heap use-after-free. Every driver must now get the error back
// without the Expr publishing shared mutable state.
TEST_F(ArraySortLambdaExprTest, invalid_comparator_rejected_concurrently) {
    // x <= y is reflexive, which strict weak ordering forbids.
    auto* expr = make_array_sort(TExprOpcode::LE);
    evaluate_concurrently(expr, 8, 200, [](const StatusOr<ColumnPtr>& result) {
        return !result.ok() && result.status().is_invalid_argument() &&
               result.status().message().find("irreflexivity") != std::string::npos;
    });
}

// A valid comparator is validated once and every driver keeps producing sorted arrays.
TEST_F(ArraySortLambdaExprTest, valid_comparator_sorts_concurrently) {
    auto* expr = make_array_sort(TExprOpcode::LT);
    evaluate_concurrently(expr, 8, 200, [](const StatusOr<ColumnPtr>& result) {
        if (!result.ok() || result.value()->size() != kNumRows) {
            return false;
        }
        for (size_t row = 0; row < kNumRows; ++row) {
            auto sorted = result.value()->get(row).get_array();
            if (sorted.size() != 3 || sorted[0].get_int32() != 1 || sorted[1].get_int32() != 3 ||
                sorted[2].get_int32() != 4) {
                return false;
            }
        }
        return true;
    });
}

// A failed validation must not poison the Expr: the same tree still rejects the comparator on
// the next chunk with the same error instead of returning a stale or an OK verdict.
TEST_F(ArraySortLambdaExprTest, invalid_comparator_error_is_not_cached) {
    auto* expr = make_array_sort(TExprOpcode::LE);
    ExprContext ctx(expr);
    std::vector<ExprContext*> ctxs = {&ctx};
    ASSERT_OK(ExprExecutor::prepare(ctxs, &_runtime_state));
    ASSERT_OK(ExprExecutor::open(ctxs, &_runtime_state));
    auto chunk = make_chunk();
    for (int i = 0; i < 3; ++i) {
        auto result = ctx.evaluate(expr, chunk.get());
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().is_invalid_argument()) << result.status();
        ASSERT_NE(std::string::npos, result.status().message().find("irreflexivity")) << result.status();
    }
    ExprExecutor::close(ctxs, &_runtime_state);
}

} // namespace starrocks
