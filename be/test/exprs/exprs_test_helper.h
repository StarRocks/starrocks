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

#pragma once

#include <gtest/gtest.h>

#include <random>

#include "column/chunk.h"
#include "exprs/array_expr.h"
#include "exprs/jit/jit_expr.h"
#include "exprs/mock_vectorized_expr.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "testutil/assert.h"

namespace starrocks {
class ExprsTestHelper {
public:
    static TTypeDesc create_scalar_type_desc(const TPrimitiveType::type t_type) {
        TTypeDesc type;

        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(t_type);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }

        return type;
    }

    static TSlotDescriptor create_slot_desc(const TTypeDesc& type, TupleId tuple_id, SlotId slot_id,
                                            const std::string& col_name) {
        //k1_int
        TSlotDescriptor slot_desc;
        slot_desc.id = slot_id;
        slot_desc.parent = tuple_id;
        slot_desc.slotType = type;
        slot_desc.columnPos = -1;
        slot_desc.byteOffset = 4;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 1;
        slot_desc.colName = col_name;
        slot_desc.slotIdx = 1;
        slot_desc.isMaterialized = true;
        return slot_desc;
    }

    static TDescriptorTable create_table_desc(std::vector<TTupleDescriptor> tuple_descs,
                                              std::vector<TSlotDescriptor> slot_descs) {
        TDescriptorTable t_desc_table;
        for (auto& slot_desc : slot_descs) {
            t_desc_table.slotDescriptors.push_back(slot_desc);
        }
        t_desc_table.__isset.slotDescriptors = true;
        for (auto& tuple_desc : tuple_descs) {
            t_desc_table.tupleDescriptors.push_back(tuple_desc);
        }
        return t_desc_table;
    }

    static TTupleDescriptor create_tuple_desc(int tuple_id) {
        // TTupleDescriptor
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = tuple_id;
        t_tuple_desc.byteSize = 32;
        t_tuple_desc.numNullBytes = 1;
        t_tuple_desc.numNullSlots = 4;
        return t_tuple_desc;
    }

    static std::unique_ptr<Expr> create_array_expr(const TTypeDesc& type) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::ARRAY_EXPR);
        node.__set_is_nullable(true);
        node.__set_type(type);
        node.__set_num_children(0);

        auto* expr = ArrayExprFactory::from_thrift(node);
        return std::unique_ptr<Expr>(expr);
    }

    static std::unique_ptr<Expr> create_array_expr(const TypeDescriptor& type) {
        return create_array_expr(type.to_thrift());
    }

    static TExprNode create_slot_expr_node(TupleId tuple_id, SlotId slot_id, TTypeDesc t_type, bool is_nullable) {
        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = t_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = slot_id;
        slot_ref.slot_ref.tuple_id = tuple_id;
        slot_ref.__set_is_nullable(is_nullable);
        return slot_ref;
    }

    static TExpr create_slot_expr(TExprNode slot_ref) {
        TExpr expr;
        expr.nodes.push_back(slot_ref);
        return expr;
    }

    static TFunction create_builtin_function(const std::string func_name, const std::vector<TTypeDesc>& arg_types,
                                             const TTypeDesc& intermediate_type, const TTypeDesc& ret_type) {
        TFunction fn;
        {
            TFunctionName fn_name;
            fn_name.function_name = func_name;
            fn.name = fn_name;
            fn.binary_type = TFunctionBinaryType::BUILTIN;
            fn.arg_types = arg_types;
            fn.ret_type = ret_type;
            fn.aggregate_fn.intermediate_type = intermediate_type;
            fn.has_var_args = false;
        }
        return fn;
    }

    static TExpr create_aggregate_expr(TFunction fn, const std::vector<TExprNode>& children) {
        TExpr expr;

        TExprNode node;
        node.node_type = TExprNodeType::AGG_EXPR;
        node.has_nullable_child = false;

        TAggregateExpr agg_expr;
        agg_expr.is_merge_agg = false;

        node.agg_expr = agg_expr;
        node.fn = fn;
        node.num_children = children.size();

        expr.nodes.push_back(node);
        for (auto& child : children) {
            expr.nodes.push_back(child);
        }
        return expr;
    }

    static ColumnPtr create_random_column(const TypeDescriptor& type_desc, int num_rows, bool low_card, bool nullable,
                                          size_t min_length = 0) {
        using UniformInt = std::uniform_int_distribution<std::mt19937::result_type>;
        using PoissonInt = std::poisson_distribution<std::mt19937::result_type>;
        MutableColumnPtr column = ColumnHelper::create_column(type_desc, nullable);

        std::random_device dev;
        std::mt19937 rng(dev());
        UniformInt uniform_int;
        if (low_card) {
            uniform_int.param(UniformInt::param_type(1, 100 * std::pow(2, num_rows)));
        } else {
            uniform_int.param(UniformInt::param_type(1, 100'000 * std::pow(2, num_rows)));
        }
        PoissonInt poisson_int(100'000);
        static std::string alphanum =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";

        auto gen_rand_str = [&]() {
            int str_len = uniform_int(rng) % 20 + min_length;
            int str_start = std::min(poisson_int(rng) % alphanum.size(), alphanum.size() - str_len);
            Slice rand_str(alphanum.c_str() + str_start, str_len);
            return rand_str;
        };

        for (int i = 0; i < num_rows; i++) {
            if (nullable) {
                int32_t x = uniform_int(rng);
                if (x % 1000 == 0) {
                    column->append_nulls(1);
                    continue;
                }
            }
            if (type_desc.type == TYPE_INT) {
                int32_t x = uniform_int(rng);
                column->append_datum(Datum(x));
            } else if (type_desc.type == TYPE_VARCHAR) {
                column->append_datum(Datum(gen_rand_str()));
            } else {
                std::cerr << "not supported" << std::endl;
            }
        }
        return column;
    }

    static void verify_with_jit(ColumnPtr ptr, Expr* expr, RuntimeState* runtime_state,
                                const std::function<void(ColumnPtr const&)>& test_func, bool need_jit = true) {
        // Verify the original result.
        test_func(ptr);

        if (!need_jit) {
            return;
        }
        auto jit_engine = JITEngine::get_instance();
        if (!jit_engine->support_jit()) {
            return;
        }
        DCHECK(runtime_state != nullptr);
        runtime_state->set_jit_level(-1);
        ObjectPool pool;
        auto* jit_expr = JITExpr::create(&pool, expr);
        jit_expr->set_uncompilable_children(runtime_state);
        ExprContext exprContext(jit_expr);
        std::vector<ExprContext*> expr_ctxs = {&exprContext};

        ASSERT_OK(Expr::prepare(expr_ctxs, runtime_state));
        ASSERT_OK(Expr::open(expr_ctxs, runtime_state));
        ASSERT_TRUE(jit_expr->is_jit_compiled());

        ptr = jit_expr->evaluate(&exprContext, nullptr);
        // Verify the result after JIT.
        test_func(ptr);

        Expr::close(expr_ctxs, runtime_state);
    }

    static void verify_result_with_jit(const ColumnPtr& ptr, Expr* expr, RuntimeState* runtime_state) {
        auto jit_engine = JITEngine::get_instance();
        if (!jit_engine->support_jit()) {
            return;
        }
        DCHECK(runtime_state != nullptr);
        runtime_state->set_jit_level(-1);
        ObjectPool pool;
        auto* jit_expr = JITExpr::create(&pool, expr);
        jit_expr->set_uncompilable_children(runtime_state);
        ExprContext exprContext(jit_expr);
        std::vector<ExprContext*> expr_ctxs = {&exprContext};

        ASSERT_OK(Expr::prepare(expr_ctxs, runtime_state));
        ASSERT_OK(Expr::open(expr_ctxs, runtime_state));
        ASSERT_TRUE(jit_expr->is_jit_compiled());

        Chunk chunk;
        chunk.append_column(ptr, 0);
        auto jit_ptr = jit_expr->evaluate(&exprContext, &chunk);

        ASSERT_TRUE(jit_ptr->is_constant() == ptr->is_constant());

        // ASSERT_TRUE(jit_ptr->is_nullable() == ptr->is_nullable());
        ASSERT_TRUE(jit_ptr->size() == ptr->size());
        for (int i = 0; i < jit_ptr->size(); ++i) {
            ASSERT_TRUE(jit_ptr->is_null(i) == ptr->is_null(i));
            ASSERT_TRUE(jit_ptr->equals(i, *ptr, i));
        }

        Expr::close(expr_ctxs, runtime_state);
    }
};

} // namespace starrocks
