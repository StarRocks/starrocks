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
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/expr_context.h

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

#pragma once

#include <atomic>
#include <memory>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exprs/function_context.h"
// Only include column/vectorized_fwd.h in this file, you need include what you need
// in the source files. Please NOT add unnecessary includes in this file.

namespace starrocks {

class OlapScanNode;
class Chunk;

class Expr;
class MemPool;
class MemTracker;
class RuntimeState;
class ObjectPool;
class TColumnValue;
class BloomFilter;
struct NgramBloomFilterReaderOptions;

/// An ExprContext contains the state for the execution of a tree of Exprs, in particular
/// the FunctionContexts necessary for the expr tree. This allows for multi-threaded
/// expression evaluation, as a given tree can be evaluated using multiple ExprContexts
/// concurrently. A single ExprContext is not thread-safe.

class ExprContext {
public:
    ExprContext(Expr* root);
    ~ExprContext();

    /// Prepare expr tree for evaluation.
    /// Allocations from this context will be counted against 'tracker'.
    Status prepare(RuntimeState* state);

    /// Must be called after calling Prepare(). Does not need to be called on clones.
    /// Idempotent (this allows exprs to be opened multiple times in subplans without
    /// reinitializing function state).
    Status open(RuntimeState* state);

    static Status open(std::vector<ExprContext*> input_evals, RuntimeState* state);

    /// Creates a copy of this ExprContext. Open() must be called first. The copy contains
    /// clones of each FunctionContext, which share the fragment-local state of the
    /// originals but have their own MemPool and thread-local state. Clone() should be used
    /// to create an ExprContext for each execution thread that needs to evaluate
    /// 'root'. Note that clones are already opened. '*new_context' must be initialized by
    /// the caller to NULL.
    Status clone(RuntimeState* state, ObjectPool* pool, ExprContext** new_context);

    /// Closes all FunctionContexts. Must be called on every ExprContext, including clones.
    void close(RuntimeState* state);

    /// Creates a FunctionContext, and returns the index that's passed to fn_context() to
    /// retrieve the created context. Exprs that need a FunctionContext should call this in
    /// Prepare() and save the returned index. 'varargs_buffer_size', if specified, is the
    /// size of the varargs buffer in the created FunctionContext (see udf-internal.h).
    int register_func(RuntimeState* state, const FunctionContext::TypeDesc& return_type,
                      const std::vector<FunctionContext::TypeDesc>& arg_types);

    /// Retrieves a registered FunctionContext. 'i' is the index returned by the call to
    /// register_func(). This should only be called by Exprs.
    FunctionContext* fn_context(int i) {
        DCHECK_GE(i, 0);
        DCHECK_LT(i, _fn_contexts.size());
        return _fn_contexts[i];
    }

    Expr* root() { return _root; }

    bool closed() { return _closed; }

    bool opened() { return _opened; }

    Status get_udf_error();

    std::string get_error_msg() const;

    StatusOr<ColumnPtr> evaluate(Chunk* chunk, uint8_t* filter = nullptr);
    StatusOr<ColumnPtr> evaluate(Expr* expr, Chunk* chunk, uint8_t* filter = nullptr);
    bool ngram_bloom_filter(const BloomFilter* bf, const NgramBloomFilterReaderOptions& reader_options);
    bool support_ngram_bloom_filter();
    bool is_index_only_filter() const;

    bool error_if_overflow() const;

    Status rewrite_jit_expr(ObjectPool* pool);

    void set_build_from_only_in_filter(bool build_from_only_in_filter) {
        _build_from_only_in_filter = build_from_only_in_filter;
    }
    bool build_from_only_in_filter() const { return _build_from_only_in_filter; }

private:
    friend class Expr;
    friend class OlapScanNode;
    friend class OlapScanNode;
    friend class EsPredicate;

    /// FunctionContexts for each registered expression. The FunctionContexts are created
    /// and owned by this ExprContext.
    std::vector<FunctionContext*> _fn_contexts;

    /// Pool backing fn_contexts_. Counts against the runtime state's UDF mem tracker.
    std::unique_ptr<MemPool> _pool;

    RuntimeState* _runtime_state = nullptr;
    /// The expr tree this context is for.
    Expr* _root;

    /// True if this context came from a Clone() call. Used to manage FunctionStateScope.
    bool _is_clone{false};
    /// Variables keeping track of current state.
    bool _prepared{false};
    bool _opened{false};
    // Indicates that this expr is built from only in runtime in filter
    // For hash join, it will build both IN filter and bloom filter. This variable is false.
    // For cross join, it will only build Runtime IN filter, and this value is false.
    bool _build_from_only_in_filter{false};
    // In operator, the ExprContext::close method will be called concurrently
    std::atomic<bool> _closed{false};
};

#define RETURN_IF_HAS_ERROR(expr_ctxs)             \
    do {                                           \
        for (auto* ctx : expr_ctxs) {              \
            RETURN_IF_ERROR(ctx->get_udf_error()); \
        }                                          \
    } while (false)

#define EVALUATE_NULL_IF_ERROR(ctx, expr, chunk)                                                      \
    [](ExprContext* c, Expr* e, Chunk* ptr) {                                                         \
        auto st = c->evaluate(e, ptr);                                                                \
        if (st.ok()) {                                                                                \
            return st.value();                                                                        \
        }                                                                                             \
        ColumnPtr res = ColumnHelper::create_const_null_column(ptr == nullptr ? 1 : ptr->num_rows()); \
        return res;                                                                                   \
    }(ctx, expr, chunk)

} // namespace starrocks
