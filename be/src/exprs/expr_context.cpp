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
#include <storage/chunk_helper.h>

#include <memory>
#include <sstream>
#include <stdexcept>

#include "column/chunk.h"
#include "common/statusor.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"

namespace starrocks {

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
    DCHECK(_pool.get() == nullptr);
    _prepared = true;
    _runtime_state = state;
    _pool = std::make_unique<MemPool>();
    return _root->prepare(state, this);
}

Status ExprContext::open(RuntimeState* state) {
    DCHECK(_prepared);
    if (_opened) {
        return Status::OK();
    }
    _opened = true;
    // Fragment-local state is only initialized for original contexts. Clones inherit the
    // original's fragment state and only need to have thread-local state initialized.
    FunctionContext::FunctionStateScope scope =
            _is_clone ? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
    return _root->open(state, this, scope);
}

Status ExprContext::open(std::vector<ExprContext*> evals, RuntimeState* state) {
    for (auto& eval : evals) {
        RETURN_IF_ERROR(eval->open(state));
    }
    return Status::OK();
}

void ExprContext::close(RuntimeState* state) {
    if (!_prepared) {
        return;
    }
    bool expected = false;
    if (!_closed.compare_exchange_strong(expected, true)) {
        return;
    }
    FunctionContext::FunctionStateScope scope =
            _is_clone ? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
    _root->close(state, this, scope);
    // _pool can be nullptr if Prepare() was never called
    if (_pool != nullptr) {
        _pool->free_all();
    }
    _pool.reset();
}

int ExprContext::register_func(RuntimeState* state, const FunctionContext::TypeDesc& return_type,
                               const std::vector<FunctionContext::TypeDesc>& arg_types) {
    _fn_contexts.push_back(FunctionContext::create_context(state, _pool.get(), return_type, arg_types));
    return _fn_contexts.size() - 1;
}

Status ExprContext::clone(RuntimeState* state, ObjectPool* pool, ExprContext** new_ctx) {
    DCHECK(_prepared);
    DCHECK(_opened);
    DCHECK(*new_ctx == nullptr);

    *new_ctx = pool->add(new ExprContext(_root));
    (*new_ctx)->_pool = std::make_unique<MemPool>();
    for (auto& _fn_context : _fn_contexts) {
        (*new_ctx)->_fn_contexts.push_back(_fn_context->clone((*new_ctx)->_pool.get()));
    }

    (*new_ctx)->_is_clone = true;
    (*new_ctx)->_prepared = true;
    (*new_ctx)->_opened = true;
    (*new_ctx)->_runtime_state = state;

    return _root->open(state, *new_ctx, FunctionContext::THREAD_LOCAL);
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
        dummy_chunk = ChunkHelper::createDummyChunk();
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
            ptr->resize(chunk->num_rows());
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

Status ExprContext::rewrite_jit_expr(ObjectPool* pool) {
    if (_runtime_state == nullptr || !_runtime_state->is_jit_enabled()) {
        return Status::OK();
    }
    bool replaced = false;
    auto st = _root->replace_compilable_exprs(&_root, pool, _runtime_state, replaced);
    if (!st.ok()) {
        LOG(WARNING) << "Can't replace compilable exprs.\n" << st.message() << "\n" << (root())->debug_string();
        // Fall back to the non-JIT path.
        return Status::OK();
    }
    if (replaced) { // only prepare jit_expr
        WARN_IF_ERROR(_root->prepare_jit_expr(_runtime_state, this), "prepare rewritten expr failed");
    }
    return Status::OK();
}

} // namespace starrocks
