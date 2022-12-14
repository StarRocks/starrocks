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
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/sort_exec_exprs.cpp

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

#include "exec/sort_exec_exprs.h"

#include <fmt/format.h>

namespace starrocks {

Status SortExecExprs::init(const TSortInfo& sort_info, ObjectPool* pool, RuntimeState* state) {
    return init(sort_info.ordering_exprs,
                sort_info.__isset.sort_tuple_slot_exprs ? &sort_info.sort_tuple_slot_exprs : nullptr, pool, state);
}

Status SortExecExprs::init(const std::vector<TExpr>& ordering_exprs, const std::vector<TExpr>* sort_tuple_slot_exprs,
                           ObjectPool* pool, RuntimeState* state) {
    _pool = pool;
    RETURN_IF_ERROR(Expr::create_expr_trees(pool, ordering_exprs, &_lhs_ordering_expr_ctxs, state));
    for (auto& expr : _lhs_ordering_expr_ctxs) {
        auto& type_desc = expr->root()->type();
        if (!type_desc.support_orderby()) {
            return Status::NotSupported(fmt::format("order by type {} is not supported", type_desc.debug_string()));
        }
    }

    if (sort_tuple_slot_exprs != nullptr) {
        _materialize_tuple = true;
        RETURN_IF_ERROR(Expr::create_expr_trees(pool, *sort_tuple_slot_exprs, &_sort_tuple_slot_expr_ctxs, state));
    } else {
        _materialize_tuple = false;
    }
    return Status::OK();
}

Status SortExecExprs::init(const std::vector<ExprContext*>& lhs_ordering_expr_ctxs,
                           const std::vector<ExprContext*>& rhs_ordering_expr_ctxs) {
    _lhs_ordering_expr_ctxs = lhs_ordering_expr_ctxs;
    _rhs_ordering_expr_ctxs = rhs_ordering_expr_ctxs;
    return Status::OK();
}

Status SortExecExprs::prepare(RuntimeState* state, const RowDescriptor& child_row_desc,
                              const RowDescriptor& output_row_desc) {
    _runtime_state = state;
    if (_materialize_tuple) {
        RETURN_IF_ERROR(Expr::prepare(_sort_tuple_slot_expr_ctxs, state));
    }
    RETURN_IF_ERROR(Expr::prepare(_lhs_ordering_expr_ctxs, state));
    return Status::OK();
}

Status SortExecExprs::open(RuntimeState* state) {
    if (_materialize_tuple) {
        RETURN_IF_ERROR(Expr::open(_sort_tuple_slot_expr_ctxs, state));
    }
    RETURN_IF_ERROR(Expr::open(_lhs_ordering_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::clone_if_not_exists(state, _pool, _lhs_ordering_expr_ctxs, &_rhs_ordering_expr_ctxs));
    return Status::OK();
}

void SortExecExprs::close(RuntimeState* state) {
    if (_is_closed) {
        return;
    }
    _is_closed = true;
    if (_materialize_tuple) {
        Expr::close(_sort_tuple_slot_expr_ctxs, state);
    }
    Expr::close(_lhs_ordering_expr_ctxs, state);
    Expr::close(_rhs_ordering_expr_ctxs, state);
}

SortExecExprs::~SortExecExprs() {
    if (_runtime_state != nullptr) {
        close(_runtime_state);
    }
}

} //namespace starrocks
