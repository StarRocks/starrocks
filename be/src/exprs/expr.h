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
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/expr.h

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
#include <string>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/expr_context.h"
#include "exprs/function_context.h"
#include "gen_cpp/Opcodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"

namespace starrocks {

class BloomFilter;
class Expr;
class ObjectPool;
class RuntimeState;
class TColumnValue;
class TExpr;
class TExprNode;
class Literal;
struct UserFunctionCacheEntry;

class Chunk;
class ColumnRef;
class ColumnPredicateRewriter;
class JITContext;
class JITExpr;
struct JitScore;
struct LLVMDatum;
class LambdaFunction;

// This is the superclass of all expr evaluation nodes.
class Expr {
public:
    // Empty virtual destructor
    virtual ~Expr();

    Expr(const Expr& expr);

    virtual Expr* clone(ObjectPool* pool) const = 0;

    bool is_null_scalar_function(std::string& str) const {
        // name and function_name both are required
        if (_fn.name.function_name.compare("is_null_pred") == 0) {
            str.assign("null");
            return true;
        } else if (_fn.name.function_name.compare("is_not_null_pred") == 0) {
            str.assign("not null");
            return true;
        } else {
            return false;
        }
    }

    // Get the number of digits after the decimal that should be displayed for this
    // value. Returns -1 if no scale has been specified (currently the scale is only set for
    // doubles set by RoundUpTo). get_value() must have already been called.
    // TODO: this will be unnecessary once we support the DECIMAL(precision, scale) type
    int output_scale() const { return _output_scale; }

    void add_child(Expr* expr) { _children.push_back(expr); }

    // only the expr after clone can call this function
    // clear children
    void clear_children() { _children.clear(); }
    Expr* get_child(int i) const { return _children[i]; }
    int get_num_children() const { return _children.size(); }

    const TypeDescriptor& type() const { return _type; }
    const std::vector<Expr*>& children() const { return _children; }

    TExprOpcode::type op() const { return _opcode; }

    TExprNodeType::type node_type() const { return _node_type; }

    const TFunction& fn() const { return _fn; }

    bool is_slotref() const { return _is_slotref; }

    bool is_nullable() const { return _is_nullable; }

    bool is_monotonic() const { return _is_monotonic; }
    bool is_cast_expr() const { return _node_type == TExprNodeType::CAST_EXPR; }
    virtual bool is_lambda_function() const { return false; }
    virtual bool is_literal() const { return false; }
    virtual bool is_dictmapping_expr() const { return false; }

    // In most time, this field is passed from FE
    // Sometimes we want to construct expr on BE implicitly and we have knowledge about `monotonicity`
    void set_monotonic(bool v) { _is_monotonic = v; }

    /// Returns true if this expr uses a FunctionContext to track its runtime state.
    /// Overridden by exprs which use FunctionContext.
    virtual bool has_fn_ctx() const { return false; }

    static TExprNodeType::type type_without_cast(const Expr* expr);

    static const Expr* expr_without_cast(const Expr* expr);

    // Returns true if expr doesn't contain slotrefs, ie, can be evaluated
    // with get_value(NULL). The default implementation returns true if all of
    // the children are constant.
    virtual bool is_constant() const;

    // Returns true if expr bound
    virtual bool is_bound(const std::vector<TupleId>& tuple_ids) const;

    // Returns the slots that are referenced by this expr tree in 'slot_ids'.
    // Returns the number of slots added to the vector
    virtual int get_slot_ids(std::vector<SlotId>* slot_ids) const;

    virtual int get_subfields(std::vector<std::vector<std::string>>* subfields) const;

    virtual void for_each_slot_id(const std::function<void(SlotId)>& cb) const;

    /// Create expression tree from the list of nodes contained in texpr within 'pool'.
    /// Returns the root of expression tree in 'expr' and the corresponding ExprContext in
    /// 'ctx'.
    static Status create_expr_tree(ObjectPool* pool, const TExpr& texpr, ExprContext** ctx, RuntimeState* state,
                                   bool can_jit = false);

    /// Creates vector of ExprContexts containing exprs from the given vector of
    /// TExprs within 'pool'.  Returns an error if any of the individual conversions caused
    /// an error, otherwise OK.
    static Status create_expr_trees(ObjectPool* pool, const std::vector<TExpr>& texprs, std::vector<ExprContext*>* ctxs,
                                    RuntimeState* state, bool can_jit = false);

    /// Creates an expr tree for the node rooted at 'node_idx' via depth-first traversal.
    /// parameters
    ///   nodes: vector of thrift expression nodes to be translated
    ///   parent: parent of node at node_idx (or NULL for node_idx == 0)
    ///   node_idx:
    ///     in: root of TExprNode tree
    ///     out: next node in 'nodes' that isn't part of tree
    ///   root_expr: out: root of constructed expr tree
    ///   ctx: out: context of constructed expr tree
    /// return
    ///   Status.ok() if successful
    ///   !Status.ok() if tree is inconsistent or corrupt
    static Status create_tree_from_thrift(ObjectPool* pool, const std::vector<TExprNode>& nodes, Expr* parent,
                                          int* node_idx, Expr** root_expr, ExprContext** ctx, RuntimeState* state);

    static Status create_tree_from_thrift_with_jit(ObjectPool* pool, const std::vector<TExprNode>& nodes, Expr* parent,
                                                   int* node_idx, Expr** root_expr, ExprContext** ctx,
                                                   RuntimeState* state);

    /// Convenience function for preparing multiple expr trees.
    static Status prepare(const std::vector<ExprContext*>& ctxs, RuntimeState* state);

    /// Convenience function for opening multiple expr trees.
    static Status open(const std::vector<ExprContext*>& ctxs, RuntimeState* state);

    /// Clones each ExprContext for multiple expr trees. 'new_ctxs' must be non-NULL.
    /// Idempotent: if '*new_ctxs' is empty, a clone of each context in 'ctxs' will be added
    /// to it, and if non-empty, it is assumed CloneIfNotExists() was already called and the
    /// call is a no-op. The new ExprContexts are created in provided object pool.
    static Status clone_if_not_exists(RuntimeState* state, ObjectPool* pool, const std::vector<ExprContext*>& ctxs,
                                      std::vector<ExprContext*>* new_ctxs);

    /// Convenience function for closing multiple expr trees.
    static void close(const std::vector<ExprContext*>& ctxs, RuntimeState* state);

    /// Convenience functions for closing a list of ScalarExpr.
    static void close(const std::vector<Expr*>& exprs);

    virtual std::string debug_string() const;

    static std::string debug_string(const std::vector<Expr*>& exprs);
    static std::string debug_string(const std::vector<ExprContext*>& ctxs);

    static Expr* copy(ObjectPool* pool, Expr* old_expr);

    // for vector query engine
    virtual StatusOr<ColumnPtr> evaluate_const(ExprContext* context);

    // TODO: check error in expression and return error Status, instead of return null column
    virtual StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) = 0;
    virtual StatusOr<ColumnPtr> evaluate_with_filter(ExprContext* context, Chunk* ptr, uint8_t* filter);

    // TODO:(murphy) remove this unchecked evaluate
    ColumnPtr evaluate(ExprContext* context, Chunk* ptr) { return evaluate_checked(context, ptr).value(); }

    // Get the first column ref in expr.
    ColumnRef* get_column_ref();

#ifdef STARROCKS_JIT_ENABLE
    StatusOr<LLVMDatum> generate_ir(ExprContext* context, JITContext* jit_ctx);

    virtual StatusOr<LLVMDatum> generate_ir_impl(ExprContext* context, JITContext* jit_ctx);

    // Return true if this expression supports JIT compilation.
    virtual bool is_compilable(RuntimeState* state) const { return false; }

    std::string jit_func_name(RuntimeState* state) const;

    virtual std::string jit_func_name_impl(RuntimeState* state) const;

    std::string jit_func_name() const;

    // This function will collect all uncompiled expressions in this expression tree.
    // The uncompiled expressions are those expressions which are not supported by JIT, it will become the input of JIT function.
    void get_uncompilable_exprs(std::vector<Expr*>& exprs, RuntimeState* state);

    // This method attempts to traverse the entire expression tree from the current expression downwards, seeking to replace expressions with JITExprs.
    // This method searches from top to bottom for compilable expressions.
    // Once a compilable expression is found, it skips over its compilable subexpressions and continues the search downwards.
    // TODO(Yueyang): The algorithm is imperfect and may further be optimized in the future.
    Status replace_compilable_exprs(Expr** expr, ObjectPool* pool, RuntimeState* state, bool& replaced);

    // Establishes whether the current expression should undergo compilation.
    // if adaptive, the valuable expressions should take the majority, i.e., `jit_score_ratio` of all expressions,
    // but case_when expr is especial, refer to its `compute_jit_score()`.
    bool should_compile(RuntimeState* state) const;

    // The valuable expressions get 1 score per expression, others get 0 score per expression, including
    // comparison expr, logical expr, branch expr, div and mod.
    virtual JitScore compute_jit_score(RuntimeState* state) const;
#endif

    // Return true if this expr or any of its children support ngram bloom filter, otherwise return flase
    virtual bool support_ngram_bloom_filter(ExprContext* context) const;

    // Return false to filter out a data page.
    virtual bool ngram_bloom_filter(ExprContext* context, const BloomFilter* bf,
                                    const NgramBloomFilterReaderOptions& reader_options) const;

    // Return true if this expr or any of its children is index only filter, otherwise return false
    bool is_index_only_filter() const;
#if BE_TEST
    void set_type(TypeDescriptor t) { _type = t; }
#endif
    SlotId max_used_slot_id() const;

protected:
    friend class MathFunctions;
    friend class StringFunctions;
    friend class ExecNode;
    friend class JsonFunctions;
    friend class Literal;
    friend class ExprContext;
    friend class ColumnPredicateRewriter;
    friend class LambdaFunction;
    friend class ArrayMapExpr;

    explicit Expr(TypeDescriptor type);
    explicit Expr(const TExprNode& node);
    Expr(TypeDescriptor type, bool is_slotref);
    Expr(const TExprNode& node, bool is_slotref);

    /// Initializes this expr instance for execution. This does not include initializing
    /// state in the ExprContext; 'context' should only be used to register a
    /// FunctionContext via RegisterFunctionContext(). Any IR functions must be generated
    /// here.
    ///
    /// Subclasses overriding this function should call Expr::Prepare() to recursively call
    /// Prepare() on the expr tree.
    virtual Status prepare(RuntimeState* state, ExprContext* context);

    /// Initializes 'context' for execution. If scope if FRAGMENT_LOCAL, both fragment- and
    /// thread-local state should be initialized. Otherwise, if scope is THREAD_LOCAL, only
    /// thread-local state should be initialized.
    //
    /// Subclasses overriding this function should call Expr::Open() to recursively call
    /// Open() on the expr tree.
    Status open(RuntimeState* state, ExprContext* context) {
        return open(state, context, FunctionContext::FRAGMENT_LOCAL);
    }

    virtual Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope);

    /// Subclasses overriding this function should call Expr::Close().
    //
    /// If scope if FRAGMENT_LOCAL, both fragment- and thread-local state should be torn
    /// down. Otherwise, if scope is THREAD_LOCAL, only thread-local state should be torn
    /// down.
    void close(RuntimeState* state, ExprContext* context) { close(state, context, FunctionContext::FRAGMENT_LOCAL); }

    virtual void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope);

    /// Releases cache entries to LibCache in all nodes of the Expr tree.
    virtual void close();

    // ------------------------------------------------------------------------------------
    // Data Members:
    // **NOTE** that when adding a new data member, please check whether it need to be added into `Expr::Expr(const Expr&)`.
    // ------------------------------------------------------------------------------------

    /// Cache entry for the library implementing this function.
    std::shared_ptr<UserFunctionCacheEntry> _cache_entry = nullptr;

    // function opcode

    TExprNodeType::type _node_type;

    // Used to check what opcode
    TExprOpcode::type _opcode;

    // recognize if this node is a slotref in order to speed up get_value()
    const bool _is_slotref;

    // The result for this expr is whether nullable, This info is passed from FE
    bool _is_nullable = true;

    // Is this expr monotnoic or not. This info is passed from FE
    bool _is_monotonic = false;

    // In storage engine, Is this expr only used for index filter(so expr filter phase will skip this expr). This info is passed from FE
    bool _is_index_only_filter = false;

    // analysis is done, types are fixed at this point
    TypeDescriptor _type;
    std::vector<Expr*> _children = std::vector<Expr*>();
    int _output_scale;

    /// Function description.
    TFunction _fn;

    /// Index to pass to ExprContext::fn_context() to retrieve this expr's FunctionContext.
    /// Set in RegisterFunctionContext(). -1 if this expr does not need a FunctionContext and
    /// doesn't call RegisterFunctionContext().
    int _fn_context_index;

    std::once_flag _constant_column_evaluate_once{};
    // set if this expr is constant, used to avoid redundant computation
    StatusOr<ColumnPtr> _constant_column = Status::OK();

    /// Simple debug string that provides no expr subclass-specific information
    std::string debug_string(const std::string& expr_name) const {
        std::stringstream out;
        out << expr_name << "(" << Expr::debug_string() << ")";
        return out.str();
    }
#ifdef STARROCKS_JIT_ENABLE
    Status prepare_jit_expr(RuntimeState* state, ExprContext* context);
#endif

private:
    // Create a new vectorized expr
    static Status create_vectorized_expr(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr,
                                         RuntimeState* state);
};

} // namespace starrocks
