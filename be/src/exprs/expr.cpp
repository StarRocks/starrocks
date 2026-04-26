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

#include <algorithm>
#include <map>
#include <mutex>
#include <sstream>
#include <utility>
#include <vector>

#include "base/failpoint/fail_point.h"
#include "exprs/expr_context.h"

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
          _fn(expr._fn),
          _fn_context_index(expr._fn_context_index) {}

Expr::Expr(TypeDescriptor type) : Expr(std::move(type), false) {}

Expr::Expr(TypeDescriptor type, bool is_slotref)
        : _opcode(TExprOpcode::INVALID_OPCODE),
          // _vector_opcode(TExprOpcode::INVALID_OPCODE),
          _is_slotref(is_slotref),
          _type(std::move(type)),
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
        case TYPE_DECIMAL256:
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
    if (node.__isset.is_nondeterministic) {
        _is_nondeterministic = node.is_nondeterministic;
    }
}

Expr::~Expr() = default;

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

Status Expr::prepare(RuntimeState* state, ExprContext* context) {
    FAIL_POINT_TRIGGER_RETURN_ERROR(randome_error);
    DCHECK(_type.type != TYPE_UNKNOWN);
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->prepare(state, context));
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

#ifdef STARROCKS_JIT_ENABLE
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

Status Expr::do_for_each_child(const std::function<Status(Expr*)>& callback) {
    for (auto& child : _children) {
        RETURN_IF_ERROR(callback(child));
    }
    return Status::OK();
}

} // namespace starrocks

#pragma clang diagnostic pop
