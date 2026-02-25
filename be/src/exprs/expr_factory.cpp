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

#include "exprs/expr_factory.h"

#include <thrift/protocol/TDebugProtocol.h>

#include <vector>

#include "base/failpoint/fail_point.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/arithmetic_expr.h"
#include "exprs/array_element_expr.h"
#include "exprs/array_expr.h"
#include "exprs/array_map_expr.h"
#include "exprs/array_sort_lambda_expr.h"
#include "exprs/arrow_function_call.h"
#include "exprs/binary_predicate.h"
#include "exprs/case_expr.h"
#include "exprs/cast_expr.h"
#include "exprs/clone_expr.h"
#include "exprs/column_ref.h"
#include "exprs/compound_predicate.h"
#include "exprs/condition_expr.h"
#include "exprs/dict_query_expr.h"
#include "exprs/dictionary_get_expr.h"
#include "exprs/dictmapping_expr.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/function_call_expr.h"
#include "exprs/in_predicate.h"
#include "exprs/info_func.h"
#include "exprs/is_null_predicate.h"
#include "exprs/java_function_call_expr.h"
#include "exprs/lambda_function.h"
#include "exprs/literal.h"
#include "exprs/map_apply_expr.h"
#include "exprs/map_element_expr.h"
#include "exprs/map_expr.h"
#include "exprs/match_expr.h"
#include "exprs/placeholder_ref.h"
#include "exprs/subfield_expr.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "runtime/runtime_state_helper.h"
#ifdef STARROCKS_JIT_ENABLE
#include "exprs/jit/expr_jit_pass.h"
#endif

namespace starrocks {

namespace {

Status create_vectorized_expr(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr, RuntimeState* state) {
    FAIL_POINT_TRIGGER_RETURN_ERROR(random_error);
    switch (texpr_node.node_type) {
    case TExprNodeType::BOOL_LITERAL:
    case TExprNodeType::INT_LITERAL:
    case TExprNodeType::LARGE_INT_LITERAL:
    case TExprNodeType::FLOAT_LITERAL:
    case TExprNodeType::DECIMAL_LITERAL:
    case TExprNodeType::DATE_LITERAL:
    case TExprNodeType::STRING_LITERAL:
    case TExprNodeType::BINARY_LITERAL:
    case TExprNodeType::NULL_LITERAL: {
        *expr = pool->add(new VectorizedLiteral(texpr_node));
        break;
    }
    case TExprNodeType::COMPOUND_PRED: {
        *expr = pool->add(VectorizedCompoundPredicateFactory::from_thrift(texpr_node));
        break;
    }
    case TExprNodeType::BINARY_PRED: {
        *expr = pool->add(VectorizedBinaryPredicateFactory::from_thrift(texpr_node));
        break;
    }
    case TExprNodeType::ARITHMETIC_EXPR: {
        if (texpr_node.opcode != TExprOpcode::INVALID_OPCODE) {
            *expr = pool->add(VectorizedArithmeticExprFactory::from_thrift(texpr_node));
            break;
        } else {
            return Status::InternalError("Vectorized engine not support unknown OP arithmetic expr");
        }
    }
    case TExprNodeType::CAST_EXPR: {
        if (texpr_node.__isset.child_type || texpr_node.__isset.child_type_desc) {
            *expr = pool->add(VectorizedCastExprFactory::from_thrift(
                    pool, texpr_node, (state == nullptr) ? false : state->query_options().allow_throw_exception));
            if (*expr == nullptr) {
                TypeDescriptor to_type = TypeDescriptor::from_thrift(texpr_node.type);
                TypeDescriptor from_type(thrift_to_type(texpr_node.child_type));
                if (texpr_node.__isset.child_type_desc) {
                    from_type = TypeDescriptor::from_thrift(texpr_node.child_type_desc);
                }
                auto err_msg =
                        fmt::format("Not support cast {} to {}.", from_type.debug_string(), to_type.debug_string());
                LOG(WARNING) << err_msg;
                return Status::InternalError(err_msg);
            } else {
                break;
            }
        } else {
            return Status::InternalError("Vectorized engine not support unknown child type cast");
        }
    }
    case TExprNodeType::COMPUTE_FUNCTION_CALL:
    case TExprNodeType::FUNCTION_CALL: {
        if (texpr_node.fn.binary_type == TFunctionBinaryType::SRJAR) {
            *expr = pool->add(new JavaFunctionCallExpr(texpr_node));
        } else if (texpr_node.fn.binary_type == TFunctionBinaryType::PYTHON) {
            *expr = pool->add(new ArrowFunctionCallExpr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "if") {
            *expr = pool->add(VectorizedConditionExprFactory::create_if_expr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "nullif") {
            *expr = pool->add(VectorizedConditionExprFactory::create_null_if_expr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "ifnull") {
            *expr = pool->add(VectorizedConditionExprFactory::create_if_null_expr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "coalesce") {
            *expr = pool->add(VectorizedConditionExprFactory::create_coalesce_expr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "is_null_pred" ||
                   texpr_node.fn.name.function_name == "is_not_null_pred") {
            *expr = pool->add(VectorizedIsNullPredicateFactory::from_thrift(texpr_node));
        } else if (texpr_node.fn.name.function_name == "array_map") {
            *expr = pool->add(new ArrayMapExpr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "array_sort_lambda") {
            *expr = pool->add(new ArraySortLambdaExpr(texpr_node));
        } else if (texpr_node.fn.name.function_name == "map_apply") {
            *expr = pool->add(new MapApplyExpr(texpr_node));
        } else {
            *expr = pool->add(new VectorizedFunctionCallExpr(texpr_node));
        }
        break;
    }
    case TExprNodeType::IN_PRED: {
        *expr = pool->add(VectorizedInPredicateFactory::from_thrift(texpr_node));
        break;
    }
    case TExprNodeType::SLOT_REF: {
        if (!texpr_node.__isset.slot_ref) {
            return Status::InternalError("Slot reference not set in thrift node");
        }
        *expr = pool->add(new ColumnRef(texpr_node));
        break;
    }
    case TExprNodeType::CASE_EXPR: {
        if (!texpr_node.__isset.case_expr) {
            return Status::InternalError("Case expression not set in thrift node");
        }
        *expr = pool->add(VectorizedCaseExprFactory::from_thrift(texpr_node));
        break;
    }
    case TExprNodeType::ARRAY_EXPR:
        *expr = pool->add(ArrayExprFactory::from_thrift(texpr_node));
        break;
    case TExprNodeType::ARRAY_ELEMENT_EXPR:
        *expr = pool->add(ArrayElementExprFactory::from_thrift(texpr_node));
        break;
    case TExprNodeType::MAP_ELEMENT_EXPR:
        *expr = pool->add(MapElementExprFactory::from_thrift(texpr_node));
        break;
    case TExprNodeType::MAP_EXPR:
        *expr = pool->add(MapExprFactory::from_thrift(texpr_node));
        break;
    case TExprNodeType::SUBFIELD_EXPR:
        *expr = pool->add(SubfieldExprFactory::from_thrift(texpr_node));
        break;
    case TExprNodeType::INFO_FUNC:
        *expr = pool->add(new VectorizedInfoFunc(texpr_node));
        break;
    case TExprNodeType::PLACEHOLDER_EXPR:
        *expr = pool->add(new PlaceHolderRef(texpr_node));
        break;
    case TExprNodeType::DICT_EXPR:
        *expr = pool->add(new DictMappingExpr(texpr_node));
        break;
    case TExprNodeType::LAMBDA_FUNCTION_EXPR:
        *expr = pool->add(new LambdaFunction(texpr_node));
        break;
    case TExprNodeType::CLONE_EXPR:
        *expr = pool->add(new CloneExpr(texpr_node));
        break;
    case TExprNodeType::DICT_QUERY_EXPR:
        *expr = pool->add(new DictQueryExpr(texpr_node));
        break;
    case TExprNodeType::DICTIONARY_GET_EXPR:
        *expr = pool->add(new DictionaryGetExpr(texpr_node));
        break;
    case TExprNodeType::MATCH_EXPR:
        *expr = pool->add(new MatchExpr(texpr_node));
        break;
    case TExprNodeType::ARRAY_SLICE_EXPR:
    case TExprNodeType::AGG_EXPR:
    case TExprNodeType::TABLE_FUNCTION_EXPR:
    case TExprNodeType::IS_NULL_PRED:
    case TExprNodeType::LIKE_PRED:
    case TExprNodeType::LITERAL_PRED:
    case TExprNodeType::TUPLE_IS_NULL_PRED:
    case TExprNodeType::RUNTIME_FILTER_MIN_MAX_EXPR:
    case TExprNodeType::JIT_EXPR:
        break;
    }
    if (*expr == nullptr) {
        std::string err_msg =
                fmt::format("Vectorized engine does not support the operator, node_type: {}", texpr_node.node_type);
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }

    return Status::OK();
}

Status create_tree_from_thrift(ObjectPool* pool, const std::vector<TExprNode>& nodes, Expr* parent, int* node_idx,
                               Expr** root_expr, RuntimeState* state) {
    if (*node_idx >= nodes.size()) {
        return Status::InternalError(
                strings::Substitute("Failed to reconstruct expression tree from thrift, node_idx:$0, nodes size:$1.",
                                    *node_idx, nodes.size()));
    }
    int num_children = nodes[*node_idx].num_children;
    Expr* expr = nullptr;
    RETURN_IF_ERROR(create_vectorized_expr(pool, nodes[*node_idx], &expr, state));
    DCHECK(expr != nullptr);
    if (parent != nullptr) {
        parent->add_child(expr);
    }
    for (int i = 0; i < num_children; i++) {
        *node_idx += 1;
        RETURN_IF_ERROR(create_tree_from_thrift(pool, nodes, expr, node_idx, nullptr, state));
        if (*node_idx >= nodes.size()) {
            return Status::InternalError(strings::Substitute(
                    "Failed to reconstruct expression tree from thrift, node_idx:$0, nodes size:$1.", *node_idx,
                    nodes.size()));
        }
    }
    if (parent == nullptr) {
        DCHECK(root_expr != nullptr);
        if (root_expr == nullptr) {
            return Status::InternalError("Failed to reconstruct expression tree from thrift. Invalid root_expr");
        }
        *root_expr = expr;
    }
    return Status::OK();
}

Status create_tree_from_thrift_with_jit(ObjectPool* pool, const std::vector<TExprNode>& nodes, int* node_idx,
                                        Expr** root_expr, RuntimeState* state) {
    Status status = create_tree_from_thrift(pool, nodes, nullptr, node_idx, root_expr, state);
    if (state == nullptr || !status.ok() || !RuntimeStateHelper::is_jit_enabled(state)) {
        return status;
    }

#ifdef STARROCKS_JIT_ENABLE
    RETURN_IF_ERROR(ExprJITPass::rewrite_root(root_expr, pool, state));
#endif

    return status;
}

} // namespace

Status ExprFactory::create_expr_tree(ObjectPool* pool, const TExpr& texpr, Expr** root_expr, RuntimeState* state,
                                     bool can_jit) {
    if (texpr.nodes.empty()) {
        *root_expr = nullptr;
        return Status::OK();
    }
    int node_idx = 0;
    Status status = create_expr_from_thrift_nodes(pool, texpr.nodes, &node_idx, root_expr, state, can_jit);
    if (status.ok() && node_idx + 1 != texpr.nodes.size()) {
        status = Status::InternalError("Expression tree only partially reconstructed. Not all thrift nodes were used.");
    }
    if (!status.ok()) {
        LOG(ERROR) << "Could not construct expr tree.\n"
                   << status.message() << "\n"
                   << apache::thrift::ThriftDebugString(texpr);
    }
    return status;
}

Status ExprFactory::create_expr_trees(ObjectPool* pool, const std::vector<TExpr>& texprs,
                                      std::vector<Expr*>* root_exprs, RuntimeState* state, bool can_jit) {
    root_exprs->clear();
    root_exprs->reserve(texprs.size());
    for (const auto& texpr : texprs) {
        Expr* root_expr = nullptr;
        RETURN_IF_ERROR(create_expr_tree(pool, texpr, &root_expr, state, can_jit));
        root_exprs->push_back(root_expr);
    }
    return Status::OK();
}

Status ExprFactory::create_expr_from_thrift_nodes(ObjectPool* pool, const std::vector<TExprNode>& nodes, int* node_idx,
                                                  Expr** root_expr, RuntimeState* state, bool can_jit) {
    if (can_jit) {
        return create_tree_from_thrift_with_jit(pool, nodes, node_idx, root_expr, state);
    }
    return create_tree_from_thrift(pool, nodes, nullptr, node_idx, root_expr, state);
}

Status ExprFactory::create_expr_tree(ObjectPool* pool, const TExpr& texpr, ExprContext** ctx, RuntimeState* state,
                                     bool can_jit) {
    Expr* root_expr = nullptr;
    RETURN_IF_ERROR(create_expr_tree(pool, texpr, &root_expr, state, can_jit));
    *ctx = root_expr == nullptr ? nullptr : pool->add(new ExprContext(root_expr));
    return Status::OK();
}

Status ExprFactory::create_expr_trees(ObjectPool* pool, const std::vector<TExpr>& texprs,
                                      std::vector<ExprContext*>* ctxs, RuntimeState* state, bool can_jit) {
    std::vector<Expr*> root_exprs;
    RETURN_IF_ERROR(create_expr_trees(pool, texprs, &root_exprs, state, can_jit));
    ctxs->clear();
    ctxs->reserve(root_exprs.size());
    for (Expr* root_expr : root_exprs) {
        ctxs->push_back(root_expr == nullptr ? nullptr : pool->add(new ExprContext(root_expr)));
    }
    return Status::OK();
}

} // namespace starrocks
