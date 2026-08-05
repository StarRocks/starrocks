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
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/expr_context.cpp

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

#include "exprs/expr_context.h"

#include <fmt/format.h>

#include <memory>
#include <stdexcept>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {

ChunkPtr create_dummy_chunk() {
    auto dummy_chunk = std::make_shared<Chunk>();
    auto column = ColumnHelper::create_const_column<TYPE_INT>(1, 1);
    dummy_chunk->append_column(std::move(column), 0);
    return dummy_chunk;
}

} // namespace

ExprContext::ExprContext(Expr* root) : _root(root) {}

ExprContext::~ExprContext() {
    // nothing to do
    if (_runtime_state == nullptr) return;

    close(_runtime_state);
    for (auto& _fn_context : _fn_contexts) {
        delete _fn_context;
    }
}

Status ExprContext::prepare(RuntimeState* state) {
    if (_prepared) {
        return Status::OK();
    }
    _prepared = true;
    _runtime_state = state;
    return _root->prepare(state, this);
}

Status ExprContext::open(RuntimeState* state) {
    DCHECK(_prepared);
    if (_opened) {
        return Status::OK();
    }
    _opened = true;
    // Clones inherit the original's fragment-local state (copied in clone()) and per-thread
    // state now lives in the FunctionContext thread-state registry, so open is a run-once
    // fragment-local operation.
    try {
        return _root->open(state, this, FunctionContext::FRAGMENT_LOCAL);
    } catch (std::runtime_error& e) {
        return Status::RuntimeError(fmt::format("Expr evaluate meet error: {}", e.what()));
    }
}

void ExprContext::close(RuntimeState* state) {
    if (!_prepared) {
        return;
    }
    bool expected = false;
    if (!_closed.compare_exchange_strong(expected, true)) {
        return;
    }
    // Only the original owns the shared fragment-local state; clones must not free it again.
    // A clone's per-thread state lives in the FunctionContext thread-state registry and is
    // released when the (cloned) FunctionContext is destroyed.
    if (!_is_clone) {
        _root->close(state, this, FunctionContext::FRAGMENT_LOCAL);
    }
}

int ExprContext::register_func(RuntimeState* state, const FunctionContext::TypeDesc& return_type,
                               const std::vector<FunctionContext::TypeDesc>& arg_types) {
    // Scalar-function and UDF FunctionContexts never allocate from their MemPool (only
    // aggregate functions use FunctionContext::mem_pool(), and those contexts are created by
    // the aggregator/analytor with its own pool), so no backing pool is needed here.
    _fn_contexts.push_back(FunctionContext::create_context(state, nullptr, return_type, arg_types));
    return _fn_contexts.size() - 1;
}

Status ExprContext::clone(RuntimeState* state, ObjectPool* pool, ExprContext** new_ctx) {
    DCHECK(_prepared);
    DCHECK(_opened);
    DCHECK(*new_ctx == nullptr);

    *new_ctx = pool->add(new ExprContext(_root));
    for (auto& _fn_context : _fn_contexts) {
        (*new_ctx)->_fn_contexts.push_back(_fn_context->clone());
    }

    (*new_ctx)->_is_clone = true;
    (*new_ctx)->_prepared = true;
    (*new_ctx)->_opened = true;
    (*new_ctx)->_runtime_state = state;

    // The clone shares the original's fragment-local state (copied above via
    // FunctionContext::clone); per-thread state is obtained lazily during evaluation from the
    // FunctionContext thread-state registry, so there is no per-clone open work to do.
    return Status::OK();
}

Status ExprContext::get_udf_error() {
    for (int idx = 0; idx < _fn_contexts.size(); ++idx) {
        DCHECK_LT(idx, _fn_contexts.size());
        FunctionContext* fn_ctx = _fn_contexts[idx];
        if (fn_ctx->is_udf() && fn_ctx->has_error()) {
            return Status::InternalError(fn_ctx->error_msg());
        }
    }
    return Status::OK();
}

std::string ExprContext::get_error_msg() const {
    for (auto fn_ctx : _fn_contexts) {
        if (fn_ctx->has_error()) {
            return {fn_ctx->error_msg()};
        }
    }
    return "";
}

StatusOr<ColumnPtr> ExprContext::evaluate(Chunk* chunk, uint8_t* filter) {
    return evaluate(_root, chunk, filter);
}

StatusOr<ColumnPtr> ExprContext::evaluate(Expr* e, Chunk* chunk, uint8_t* filter) {
    DCHECK(_prepared);
    DCHECK(_opened);
    DCHECK(!_closed);
    ChunkPtr dummy_chunk;
    // this may happen if expr is constant, which means it doesn't need any input chunk
    // but some expr can not handle situation that input chunk is nullptr or empty correctly
    // so we create chunk with one column and one raw
    if (chunk == nullptr) {
        dummy_chunk = create_dummy_chunk();
        chunk = dummy_chunk.get();
    }
#ifndef NDEBUG
    if (chunk != nullptr) {
        chunk->check_or_die();
        CHECK(!chunk->is_empty());
    }
#endif
    try {
        ColumnPtr ptr = nullptr;
        if (filter == nullptr) {
            ASSIGN_OR_RETURN(ptr, e->evaluate_checked(this, chunk));
        } else {
            ASSIGN_OR_RETURN(ptr, e->evaluate_with_filter(this, chunk, filter));
        }
        DCHECK(ptr != nullptr);
        if (chunk != nullptr && 0 != chunk->num_columns() && ptr->is_constant() && (dummy_chunk.get() == nullptr)) {
            ptr->as_mutable_raw_ptr()->resize(chunk->num_rows());
        }
        return ptr;
    } catch (std::runtime_error& e) {
        return Status::RuntimeError(fmt::format("Expr evaluate meet error: {}", e.what()));
    }
}

bool ExprContext::ngram_bloom_filter(const BloomFilter* bf, const NgramBloomFilterReaderOptions& reader_options) {
    return _root->ngram_bloom_filter(this, bf, reader_options);
}

bool ExprContext::support_ngram_bloom_filter() {
    return _root->support_ngram_bloom_filter(this);
}

bool ExprContext::is_index_only_filter() const {
    return _root->is_index_only_filter();
}

bool ExprContext::error_if_overflow() const {
    return _runtime_state != nullptr && _runtime_state->error_if_overflow();
}

bool ExprContext::error_for_division_by_zero() const {
    return _runtime_state != nullptr && _runtime_state->error_for_division_by_zero();
}
} // namespace starrocks
