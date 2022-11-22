// This file is made available under Elastic License 2.0.
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

#include <memory>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "udf/udf.h"
#include "udf/udf_internal.h" // for ArrayVal
// Only include column/vectorized_fwd.h in this file, you need include what you need
// in the source files. Please NOT add unnecessary includes in this file.

#undef USING_STARROCKS_UDF
#define USING_STARROCKS_UDF using namespace starrocks_udf

USING_STARROCKS_UDF;

namespace starrocks {

namespace vectorized {
class OlapScanNode;
class Chunk;
} // namespace vectorized

class Expr;
class MemPool;
class MemTracker;
class RuntimeState;
class ObjectPool;
class TColumnValue;

using vectorized::ColumnPtr;

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

    Status clone(RuntimeState* state, ObjectPool* pool, ExprContext** new_ctx, Expr* root);

    /// Closes all FunctionContexts. Must be called on every ExprContext, including clones.
    void close(RuntimeState* state);

    /// Creates a FunctionContext, and returns the index that's passed to fn_context() to
    /// retrieve the created context. Exprs that need a FunctionContext should call this in
    /// Prepare() and save the returned index. 'varargs_buffer_size', if specified, is the
    /// size of the varargs buffer in the created FunctionContext (see udf-internal.h).
    int register_func(RuntimeState* state, const FunctionContext::TypeDesc& return_type,
                      const std::vector<FunctionContext::TypeDesc>& arg_types, int varargs_buffer_size);

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

    /// Returns an error status if there was any error in evaluating the expression
    /// or its sub-expressions. 'start_idx' and 'end_idx' correspond to the range
    /// within the vector of FunctionContext for the sub-expressions of interest.
    /// The default parameters correspond to the entire expr 'root_'.
    Status get_error(int start_idx, int end_idx) const;

    Status get_udf_error();

    std::string get_error_msg() const;

    StatusOr<ColumnPtr> evaluate(vectorized::Chunk* chunk, uint8_t* filter = nullptr);
    StatusOr<ColumnPtr> evaluate(Expr* expr, vectorized::Chunk* chunk, uint8_t* filter = nullptr);

private:
    friend class Expr;
    friend class ScalarFnCall;
    friend class InPredicate;
    friend class OlapScanNode;
    friend class vectorized::OlapScanNode;
    friend class EsScanNode;
    friend class EsPredicate;

    /// FunctionContexts for each registered expression. The FunctionContexts are created
    /// and owned by this ExprContext.
    std::vector<FunctionContext*> _fn_contexts;

    /// Array access to fn_contexts_. Used by ScalarFnCall's codegen'd compute function
    /// to access the correct FunctionContext.
    /// TODO: revisit this
    FunctionContext** _fn_contexts_ptr;

    /// Pool backing fn_contexts_. Counts against the runtime state's UDF mem tracker.
    std::unique_ptr<MemPool> _pool;

    /// The expr tree this context is for.
    Expr* _root;

    /// True if this context came from a Clone() call. Used to manage FunctionStateScope.
    bool _is_clone;

    /// Variables keeping track of current state.
    bool _prepared;
    bool _opened;
    RuntimeState* _runtime_state = nullptr;
    // In operator, the ExprContext::close method will be called concurrently
    std::atomic<bool> _closed;
};

#define RETURN_IF_HAS_ERROR(expr_ctxs)             \
    do {                                           \
        for (auto* ctx : expr_ctxs) {              \
            RETURN_IF_ERROR(ctx->get_udf_error()); \
        }                                          \
    } while (false)

#define EVALUATE_NULL_IF_ERROR(ctx, expr, chunk)                                                         \
    [](ExprContext* c, Expr* e, vectorized::Chunk* ptr) {                                                \
        auto st = c->evaluate(e, ptr);                                                                   \
        if (st.ok()) {                                                                                   \
            return st.value();                                                                           \
        }                                                                                                \
        return vectorized::ColumnHelper::create_const_null_column(ptr == nullptr ? 1 : ptr->num_rows()); \
    }(ctx, expr, chunk)

} // namespace starrocks
