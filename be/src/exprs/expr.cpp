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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/expr.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exprs/expr.h"

#include <thrift/protocol/TDebugProtocol.h>

#include <sstream>
#include <utility>
#include <vector>

#include "column/fixed_length_column.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/arithmetic_expr.h"
#include "exprs/array_element_expr.h"
#include "exprs/array_expr.h"
#include "exprs/array_map_expr.h"
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
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "util/failpoint/fail_point.h"

#ifdef STARROCKS_JIT_ENABLE
#include <llvm/IR/Value.h>

#include "exprs/jit/ir_helper.h"
#include "exprs/jit/jit_engine.h"
#include "exprs/jit/jit_expr.h"
#endif

#pragma clang diagnostic push
#pragma ide diagnostic ignored "EndlessLoop"
using std::vector;
namespace starrocks {

// No children here
Expr::Expr(const Expr& expr)
        : _cache_entry(expr._cache_entry),
          _node_type(expr._node_type),
          _opcode(expr._opcode),
          _is_slotref(expr._is_slotref),
          _is_nullable(expr._is_nullable),
          _is_monotonic(expr._is_monotonic),
          _type(expr._type),
          _output_scale(expr._output_scale),
          _fn(expr._fn),
          _fn_context_index(expr._fn_context_index) {}

Expr::Expr(TypeDescriptor type) : Expr(std::move(type), false) {}

Expr::Expr(TypeDescriptor type, bool is_slotref)
        : _opcode(TExprOpcode::INVALID_OPCODE),
          // _vector_opcode(TExprOpcode::INVALID_OPCODE),
          _is_slotref(is_slotref),
          _type(std::move(type)),
          _output_scale(-1),
          _fn_context_index(-1) {
    if (is_slotref) {
        _node_type = (TExprNodeType::SLOT_REF);
    } else {
        switch (_type.type) {
        case TYPE_BOOLEAN:
            _node_type = (TExprNodeType::BOOL_LITERAL);
            break;

        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
            _node_type = (TExprNodeType::INT_LITERAL);
            break;

        case TYPE_LARGEINT:
            _node_type = (TExprNodeType::LARGE_INT_LITERAL);
            break;

        case TYPE_NULL:
            _node_type = (TExprNodeType::NULL_LITERAL);
            break;

        case TYPE_FLOAT:
        case TYPE_DOUBLE:
        case TYPE_TIME:
            _node_type = (TExprNodeType::FLOAT_LITERAL);
            break;

        case TYPE_DECIMAL:
        case TYPE_DECIMALV2:
            _node_type = (TExprNodeType::DECIMAL_LITERAL);
            break;

        case TYPE_DATE:
        case TYPE_DATETIME:
            _node_type = (TExprNodeType::DATE_LITERAL);
            break;

        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_HLL:
        case TYPE_OBJECT:
        case TYPE_PERCENTILE:
            _node_type = (TExprNodeType::STRING_LITERAL);
            break;
        case TYPE_ARRAY:
            _node_type = (TExprNodeType::ARRAY_EXPR);
            break;
        case TYPE_VARBINARY:
            _node_type = (TExprNodeType::BINARY_LITERAL);
            break;
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128:
            _node_type = TExprNodeType::DECIMAL_LITERAL;
            break;
        case TYPE_UNKNOWN:
        case TYPE_STRUCT:
        case TYPE_MAP:
        case TYPE_JSON:
            break;

        default:
            DCHECK(false) << "Invalid type." << _type.type;
        }
    }
}

Expr::Expr(const TExprNode& node) : Expr(node, false) {}

Expr::Expr(const TExprNode& node, bool is_slotref)
        : _node_type(node.node_type),
          _opcode(node.__isset.opcode ? node.opcode : TExprOpcode::INVALID_OPCODE),
          // _vector_opcode(
          // node.__isset.vector_opcode ? node.vector_opcode : TExprOpcode::INVALID_OPCODE),
          _is_slotref(is_slotref),
          _is_nullable(node.is_nullable),
          _type(TypeDescriptor::from_thrift(node.type)),
          _output_scale(node.output_scale),
          _fn_context_index(-1) {
    if (node.__isset.fn) {
        _fn = node.fn;
    }
    if (node.__isset.is_monotonic) {
        _is_monotonic = node.is_monotonic;
    }
    if (node.__isset.is_index_only_filter) {
        _is_index_only_filter = node.is_index_only_filter;
    }
}

Expr::~Expr() = default;

Status Expr::create_expr_tree(ObjectPool* pool, const TExpr& texpr, ExprContext** ctx, RuntimeState* state,
                              bool can_jit) {
    // input is empty
    if (texpr.nodes.empty()) {
        *ctx = nullptr;
        return Status::OK();
    }
    int node_idx = 0;
    Expr* e = nullptr;
    Status status;
    if (can_jit) {
        status = create_tree_from_thrift_with_jit(pool, texpr.nodes, nullptr, &node_idx, &e, ctx, state);
    } else {
        status = create_tree_from_thrift(pool, texpr.nodes, nullptr, &node_idx, &e, ctx, state);
    }
    if (status.ok() && node_idx + 1 != texpr.nodes.size()) {
        status = Status::InternalError("Expression tree only partially reconstructed. Not all thrift nodes were used.");
    }
    if (!status.ok()) {
        LOG(ERROR) << "Could not construct expr tree.\n"
                   << status.message() << "\n"
                   << apache::thrift::ThriftDebugString(texpr);
        return status;
    }

    return status;
}

#ifdef STARROCKS_JIT_ENABLE
Status Expr::prepare_jit_expr(RuntimeState* state, ExprContext* context) {
    if (this->node_type() == TExprNodeType::JIT_EXPR) {
        RETURN_IF_ERROR(((JITExpr*)this)->prepare_impl(state, context));
    }
    for (auto child : _children) {
        RETURN_IF_ERROR(child->prepare_jit_expr(state, context));
    }
    return Status::OK();
}
#endif

Status Expr::create_expr_trees(ObjectPool* pool, const std::vector<TExpr>& texprs, std::vector<ExprContext*>* ctxs,
                               RuntimeState* state, bool can_jit) {
    ctxs->clear();
    for (const auto& texpr : texprs) {
        ExprContext* ctx = nullptr;
        RETURN_IF_ERROR(create_expr_tree(pool, texpr, &ctx, state, can_jit));
        ctxs->push_back(ctx);
    }
    return Status::OK();
}

Status Expr::create_tree_from_thrift_with_jit(ObjectPool* pool, const std::vector<TExprNode>& nodes, Expr* parent,
                                              int* node_idx, Expr** root_expr, ExprContext** ctx, RuntimeState* state) {
    Status status = create_tree_from_thrift(pool, nodes, parent, node_idx, root_expr, ctx, state);
    // Enable JIT based on the "jit_level" parameters.
    if (state == nullptr || !status.ok() || !state->is_jit_enabled()) {
        return status;
    }

#ifdef STARROCKS_JIT_ENABLE
    bool replaced = false;
    status = (*root_expr)->replace_compilable_exprs(root_expr, pool, state, replaced);
    if (!status.ok()) {
        LOG(WARNING) << "Can't replace compilable exprs.\n" << status.message() << "\n" << (*root_expr)->debug_string();
        // Fall back to the non-JIT path.
        return Status::OK();
    }

    if (replaced) {
        // The node was replaced, so we need to update the context.
        *ctx = pool->add(new ExprContext(*root_expr));
    }
#endif

    return status;
}

Status Expr::create_tree_from_thrift(ObjectPool* pool, const std::vector<TExprNode>& nodes, Expr* parent, int* node_idx,
                                     Expr** root_expr, ExprContext** ctx, RuntimeState* state) {
    // propagate error case
    if (*node_idx >= nodes.size()) {
        return Status::InternalError(
                strings::Substitute("Failed to reconstruct expression tree from thrift, "
                                    "node_idx:$0, nodes size:$1.",
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
        RETURN_IF_ERROR(create_tree_from_thrift(pool, nodes, expr, node_idx, nullptr, nullptr, state));
        // we are expecting a child, but have used all nodes
        // this means we have been given a bad tree and must fail
        if (*node_idx >= nodes.size()) {
            return Status::InternalError(
                    strings::Substitute("Failed to reconstruct expression tree from thrift, "
                                        "node_idx:$0, nodes size:$1.",
                                        *node_idx, nodes.size()));
        }
    }
    if (parent == nullptr) {
        DCHECK(root_expr != nullptr);
        DCHECK(ctx != nullptr);
        if (root_expr == nullptr || ctx == nullptr) {
            return Status::InternalError(
                    "Failed to reconstruct expression tree from thrift. Invalid input root_expr or ctx");
        } else {
            *root_expr = expr;
            *ctx = pool->add(new ExprContext(expr));
        }
    }
    return Status::OK();
}

Status Expr::create_vectorized_expr(starrocks::ObjectPool* pool, const starrocks::TExprNode& texpr_node,
                                    starrocks::Expr** expr, RuntimeState* state) {
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
            // @TODO: will call FunctionExpr, implement later
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
                // In cast TExprNode, child_type is used to represent scalar type,
                // and child_type_desc is used to represent complex types, such as struct, map, array
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
            // @TODO: will call FunctionExpr, implement later
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

struct MemLayoutData {
    int expr_idx;
    int byte_size;
    bool variable_length;

    // TODO: sort by type as well?  Any reason to do this?
    bool operator<(const MemLayoutData& rhs) const {
        // variable_len go at end
        if (this->variable_length && !rhs.variable_length) {
            return false;
        }

        if (!this->variable_length && rhs.variable_length) {
            return true;
        }

        return this->byte_size < rhs.byte_size;
    }
};

Status Expr::prepare(const std::vector<ExprContext*>& ctxs, RuntimeState* state) {
    for (auto ctx : ctxs) {
        RETURN_IF_ERROR(ctx->prepare(state));
    }
    return Status::OK();
}

Status Expr::prepare(RuntimeState* state, ExprContext* context) {
    FAIL_POINT_TRIGGER_RETURN_ERROR(randome_error);
    DCHECK(_type.type != TYPE_UNKNOWN);
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->prepare(state, context));
    }
    return Status::OK();
}

Status Expr::open(const std::vector<ExprContext*>& ctxs, RuntimeState* state) {
    for (auto ctx : ctxs) {
        RETURN_IF_ERROR(ctx->open(state));
    }
    return Status::OK();
}

Status Expr::open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    FAIL_POINT_TRIGGER_RETURN_ERROR(random_error);
    DCHECK(_type.type != TYPE_UNKNOWN);
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->open(state, context, scope));
    }
    return Status::OK();
}

void Expr::close(const std::vector<ExprContext*>& ctxs, RuntimeState* state) {
    for (auto ctx : ctxs) {
        if (ctx != nullptr) {
            ctx->close(state);
        }
    }
}

void Expr::close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    for (auto& i : _children) {
        i->close(state, context, scope);
    }
    // TODO(zc)
#if 0
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        // This is the final, non-cloned context to close. Clean up the whole Expr.
        if (cache_entry_ != NULL) {
            LibCache::instance()->DecrementUseCount(cache_entry_);
            cache_entry_ = NULL;
        }
    }
#endif
}

Status Expr::clone_if_not_exists(RuntimeState* state, ObjectPool* pool, const std::vector<ExprContext*>& ctxs,
                                 std::vector<ExprContext*>* new_ctxs) {
    DCHECK(new_ctxs != nullptr);
    if (!new_ctxs->empty()) {
        // 'ctxs' was already cloned into '*new_ctxs', nothing to do.
        DCHECK_EQ(new_ctxs->size(), ctxs.size());
        for (auto& new_ctx : *new_ctxs) {
            DCHECK(new_ctx->_is_clone);
        }
        return Status::OK();
    }

    new_ctxs->resize(ctxs.size());
    for (int i = 0; i < ctxs.size(); ++i) {
        RETURN_IF_ERROR(ctxs[i]->clone(state, pool, &(*new_ctxs)[i]));
    }
    return Status::OK();
}

std::string Expr::debug_string() const {
    // TODO: implement partial debug string for member vars
    std::stringstream out;
    out << " type=" << _type.debug_string();

    if (_opcode != TExprOpcode::INVALID_OPCODE) {
        out << " opcode=" << _opcode;
    }
    out << " node-type=" << to_string(_node_type);
    out << " codegen=false";

    if (!_children.empty()) {
        out << " children=" << debug_string(_children);
    }

    return out.str();
}

std::string Expr::debug_string(const std::vector<Expr*>& exprs) {
    std::stringstream out;
    out << "[";

    for (int i = 0; i < exprs.size(); ++i) {
        out << (i == 0 ? "" : " ") << exprs[i]->debug_string();
    }

    out << "]";
    return out.str();
}

std::string Expr::debug_string(const std::vector<ExprContext*>& ctxs) {
    std::vector<Expr*> exprs;
    exprs.reserve(ctxs.size());
    for (auto ctx : ctxs) {
        exprs.push_back(ctx->root());
    }
    return debug_string(exprs);
}

bool Expr::is_constant() const {
    for (auto i : _children) {
        if (!i->is_constant()) {
            return false;
        }
    }

    return true;
}

TExprNodeType::type Expr::type_without_cast(const Expr* expr) {
    if (expr->_opcode == TExprOpcode::CAST) {
        return type_without_cast(expr->_children[0]);
    }
    return expr->_node_type;
}

const Expr* Expr::expr_without_cast(const Expr* expr) {
    if (expr->_opcode == TExprOpcode::CAST) {
        return expr_without_cast(expr->_children[0]);
    }
    return expr;
}

bool Expr::is_bound(const std::vector<TupleId>& tuple_ids) const {
    for (auto i : _children) {
        if (!i->is_bound(tuple_ids)) {
            return false;
        }
    }

    return true;
}

int Expr::get_slot_ids(std::vector<SlotId>* slot_ids) const {
    int n = 0;

    for (auto i : _children) {
        n += i->get_slot_ids(slot_ids);
    }

    return n;
}

void Expr::for_each_slot_id(const std::function<void(SlotId)>& cb) const {
    for (auto child : _children) {
        child->for_each_slot_id(cb);
    }
}

int Expr::get_subfields(std::vector<std::vector<std::string>>* subfields) const {
    int n = 0;

    for (auto i : _children) {
        n += i->get_subfields(subfields);
    }

    return n;
}

Expr* Expr::copy(ObjectPool* pool, Expr* old_expr) {
    auto new_expr = old_expr->clone(pool);
    for (auto child : old_expr->_children) {
        auto new_child = copy(pool, child);
        new_expr->_children.push_back(new_child);
    }
    return new_expr;
}

// TODO chenhao
void Expr::close() {
    for (Expr* child : _children) child->close();
    /*if (_cache_entry != nullptr) {
      LibCache::instance()->decrement_use_count(_cache_entry);
      _cache_entry = nullptr;
      }*/
    _cache_entry.reset();
}

void Expr::close(const std::vector<Expr*>& exprs) {
    for (Expr* expr : exprs) expr->close();
}

StatusOr<ColumnPtr> Expr::evaluate_const(ExprContext* context) {
    if (!is_constant()) {
        return nullptr;
    }

    if (_constant_column.ok() && _constant_column.value()) {
        return _constant_column;
    }

    // prevent _constant_column from being assigned by multiple threads in pipeline engine.
    std::call_once(_constant_column_evaluate_once,
                   [this, context] { this->_constant_column = context->evaluate(this, nullptr); });
    return _constant_column;
}

StatusOr<ColumnPtr> Expr::evaluate_with_filter(ExprContext* context, Chunk* ptr, uint8_t* filter) {
    return evaluate_checked(context, ptr);
}

ColumnRef* Expr::get_column_ref() {
    if (this->is_slotref()) {
        return down_cast<ColumnRef*>(this);
    }
    for (auto child : this->children()) {
        ColumnRef* ref = nullptr;
        if ((ref = child->get_column_ref()) != nullptr) {
            return ref;
        }
    }
    return nullptr;
}

#ifdef STARROCKS_JIT_ENABLE
StatusOr<LLVMDatum> Expr::generate_ir(ExprContext* context, JITContext* jit_ctx) {
    if (this->is_compilable(context->_runtime_state)) {
        return this->generate_ir_impl(context, jit_ctx);
    } else {
        return Expr::generate_ir_impl(context, jit_ctx);
    }
}

StatusOr<LLVMDatum> Expr::generate_ir_impl(ExprContext* context, JITContext* jit_ctx) {
    if (is_compilable(context->_runtime_state)) {
#if BE_TEST
        throw std::runtime_error("[JIT] compilable expressions must not be here : " + debug_string());
#else
        return Status::NotSupported("[JIT] compilable expressions must override generate_ir_impl()");
#endif
    }
    if (jit_ctx->input_index >= jit_ctx->columns.size() - 1) {
#if BE_TEST
        throw std::runtime_error("[JIT] vector overflow for expr :" + debug_string());
#else
        return Status::RuntimeError("[JIT] vector overflow for uncompilable expr");
#endif
    }
    LLVMDatum datum(jit_ctx->builder);
    datum.value = jit_ctx->builder.CreateLoad(
            jit_ctx->columns[jit_ctx->input_index].value_type,
            jit_ctx->builder.CreateInBoundsGEP(jit_ctx->columns[jit_ctx->input_index].value_type,
                                               jit_ctx->columns[jit_ctx->input_index].values, jit_ctx->index_phi));
    if (is_nullable()) {
        datum.null_flag = jit_ctx->builder.CreateLoad(
                jit_ctx->builder.getInt8Ty(),
                jit_ctx->builder.CreateInBoundsGEP(jit_ctx->builder.getInt8Ty(),
                                                   jit_ctx->columns[jit_ctx->input_index].null_flags,
                                                   jit_ctx->index_phi));
    }
    jit_ctx->input_index++;
    return datum;
}

void Expr::get_uncompilable_exprs(std::vector<Expr*>& exprs, RuntimeState* state) {
    if (!this->is_compilable(state)) {
        exprs.emplace_back(this);
        return;
    }
    for (auto child : this->children()) {
        child->get_uncompilable_exprs(exprs, state);
    }
}

std::string Expr::jit_func_name(RuntimeState* state) const {
    if (this->is_compilable(state)) {
        return this->jit_func_name_impl(state);
    } else {
        return Expr::jit_func_name_impl(state);
    }
}

std::string Expr::jit_func_name_impl(RuntimeState* state) const {
    DCHECK(!is_compilable(state));
    // uncompilable inputs, reducing string size.
    return std::string("col[") + (is_constant() ? "c:" : "") + (is_nullable() ? "n:" : "") + type().debug_string() +
           "]";
}

// This method attempts to traverse the entire expression tree from the current expression downwards, seeking to replace expressions with JITExprs.
// This method searches from top to bottom for compilable expressions.
// Once a compilable expression is found, it skips over its compilable subexpressions and continues the search downwards.
Status Expr::replace_compilable_exprs(Expr** expr, ObjectPool* pool, RuntimeState* state, bool& replaced) {
    if (_node_type == TExprNodeType::DICT_EXPR || _node_type == TExprNodeType::DICT_QUERY_EXPR ||
        _node_type == TExprNodeType::DICTIONARY_GET_EXPR || _node_type == TExprNodeType::PLACEHOLDER_EXPR ||
        _node_type == TExprNodeType::MATCH_EXPR) {
        return Status::OK();
    }
    DCHECK(JITEngine::get_instance()->support_jit());
    if ((*expr)->should_compile(state)) {
        // If the current expression is compilable, we will replace it with a JITExpr.
        // This expression and its compilable subexpressions will be compiled into a single function.
        auto* jit_expr = JITExpr::create(pool, *expr);
        jit_expr->set_uncompilable_children(state);
        *expr = jit_expr;
        replaced = true;
    }

    for (auto& child : (*expr)->_children) {
        RETURN_IF_ERROR(child->replace_compilable_exprs(&child, pool, state, replaced));
    }
    return Status::OK();
}

JitScore Expr::compute_jit_score(RuntimeState* state) const {
    JitScore jit_score = {0, 0};
    if (!is_compilable(state)) {
        return jit_score;
    }
    for (auto child : _children) {
        auto tmp = child->compute_jit_score(state);
        jit_score.score += tmp.score;
        jit_score.num += tmp.num;
    }
    jit_score.num++;
    jit_score.score++; // helpful by default.
    return jit_score;
}

bool Expr::should_compile(RuntimeState* state) const {
    if (!is_compilable(state) || _children.empty() || is_constant()) {
        return false;
    }

    if (state->is_adaptive_jit()) {
        auto score = compute_jit_score(state);
        auto valid = (score.score > score.num * IRHelper::jit_score_ratio && score.num > 2);
        VLOG_QUERY << "JIT score expr: score = " << score.score << " / " << score.num << " = "
                   << score.score * 1.0 / score.num << " valid = " << valid << "  " << jit_func_name(state);
        if (!valid) {
            return false;
        }
    }
    return true;
}
#endif

bool Expr::support_ngram_bloom_filter(ExprContext* context) const {
    bool support = false;
    for (auto& child : _children) {
        if (child->support_ngram_bloom_filter(context)) {
            return true;
        }
    }
    return support;
}

bool Expr::ngram_bloom_filter(ExprContext* context, const BloomFilter* bf,
                              const NgramBloomFilterReaderOptions& reader_options) const {
    bool no_need_to_filt = true;
    for (auto& child : _children) {
        if (!child->ngram_bloom_filter(context, bf, reader_options)) {
            return false;
        }
    }
    return no_need_to_filt;
}

bool Expr::is_index_only_filter() const {
    bool is_index_only_filter = _is_index_only_filter;
    for (auto& child : _children) {
        if (child->is_index_only_filter()) {
            return true;
        }
    }
    return is_index_only_filter;
}

SlotId Expr::max_used_slot_id() const {
    SlotId max_slot_id = 0;
    for_each_slot_id([&max_slot_id](SlotId slot_id) { max_slot_id = std::max(max_slot_id, slot_id); });
    return max_slot_id;
}

} // namespace starrocks

#pragma clang diagnostic pop
