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

#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "types/logical_type.h"

namespace starrocks {

class MemPool;
class RuntimeState;

class Column;
struct JavaUDAFContext;
using ColumnPtr = std::shared_ptr<Column>;

class FunctionContext {
public:
    struct TypeDesc {
        ::starrocks::LogicalType type = ::starrocks::TYPE_NULL;

        /// Only valid if type == TYPE_DECIMAL
        int precision = 0;
        int scale = 0;

        /// Only valid if type == TYPE_FIXED_BUFFER || type == TYPE_VARCHAR
        int len = 0;

        // only valid if type is nested type
        // array's element: children[0].
        // map's key: children[0]; map's value: children[1].
        // struct's types: keep order with field_names.
        std::vector<TypeDesc> children;
        std::vector<std::string> field_names;
    };

    enum FunctionStateScope {
        /// Indicates that the function state for this FunctionContext's UDF is shared across
        /// the plan fragment (a query is divided into multiple plan fragments, each of which
        /// is responsible for a part of the query execution). Within the plan fragment, there
        /// may be multiple instances of the UDF executing concurrently with multiple
        /// FunctionContexts sharing this state, meaning that the state must be
        /// thread-safe. The Prepare() function for the UDF may be called with this scope
        /// concurrently on a single host if the UDF will be evaluated in multiple plan
        /// fragments on that host. In general, read-only state that doesn't need to be
        /// recomputed for every UDF call should be fragment-local.
        /// TODO: not yet implemented
        FRAGMENT_LOCAL,

        /// Indicates that the function state is local to the execution thread. This state
        /// does not need to be thread-safe. However, this state will be initialized (via the
        /// Prepare() function) once for every execution thread, so fragment-local state
        /// should be used when possible for better performance. In general, inexpensive
        /// shared state that is written to by the UDF (e.g. scratch space) should be
        /// thread-local.
        THREAD_LOCAL,
    };

    /// Create a FunctionContext for a UDF. Caller is responsible for deleting it.
    static FunctionContext* create_context(RuntimeState* state, MemPool* pool,
                                           const FunctionContext::TypeDesc& return_type,
                                           const std::vector<FunctionContext::TypeDesc>& arg_types);

    static FunctionContext* create_context(RuntimeState* state, MemPool* pool,
                                           const FunctionContext::TypeDesc& return_type,
                                           const std::vector<FunctionContext::TypeDesc>& arg_types,
                                           const std::vector<bool>& isAscOrder, const std::vector<bool>& nullsFirst);

    ~FunctionContext();
    FunctionContext();

    // Sets an error for this UDF. If this is called, this will trigger the
    // query to fail.
    // Note: when you set error for the UDFs used in Data Load, you should
    // ensure the function return value is null.
    void set_error(const char* error_msg, const bool is_udf = true);

    // Adds a warning that is returned to the user. This can include things like
    // overflow or other recoverable error conditions.
    // Warnings are capped at a maximum number. Returns true if the warning was
    // added and false if it was ignored due to the cap.
    bool add_warning(const char* warning_msg);

    /// Methods for maintaining state across UDF/UDA function calls. SetFunctionState() can
    /// be used to store a pointer that can then be retrieved via GetFunctionState(). If
    /// GetFunctionState() is called when no pointer is set, it will return
    /// NULL. SetFunctionState() does not take ownership of 'ptr'; it is up to the UDF/UDA
    /// to clean up any function state if necessary.
    void set_function_state(FunctionStateScope scope, void* ptr);

    void* get_function_state(FunctionStateScope scope) const;

    // Returns the return type information of this function. For UDAs, this is the final
    // return type of the UDA (e.g., the type returned by the finalize function).
    const TypeDesc& get_return_type() const;

    // Returns the number of arguments to this function (not including the FunctionContext*
    // argument).
    int get_num_args() const;

    std::vector<bool> get_is_asc_order() { return _is_asc_order; }
    std::vector<bool> get_nulls_first() { return _nulls_first; }
    // for tests
    void set_is_asc_order(const std::vector<bool>& order) { _is_asc_order = order; }
    void set_nulls_first(const std::vector<bool>& nulls) { _nulls_first = nulls; }
    void set_runtime_state(RuntimeState* const state) { _state = state; }

    // Returns _constant_columns size
    int get_num_constant_columns() const;

    // Returns the type information for the arg_idx-th argument (0-indexed, not including
    // the FunctionContext* argument). Returns NULL if arg_idx is invalid.
    const TypeDesc* get_arg_type(int arg_idx) const;

    bool is_constant_column(int arg_idx) const;

    // Return true if it's constant and not null
    bool is_notnull_constant_column(int i) const;

    std::shared_ptr<starrocks::Column> get_constant_column(int arg_idx) const;

    bool is_udf() { return _is_udf; }
    void set_is_udf(bool is_udf) { this->_is_udf = is_udf; }

    ColumnPtr create_column(const TypeDesc& type_desc, bool nullable);

    // Create a test FunctionContext object. The caller is responsible for calling delete
    // on it. This context has additional debugging validation enabled.
    static FunctionContext* create_test_context();
    static FunctionContext* create_test_context(std::vector<TypeDesc>&& arg_types, const TypeDesc& return_type);

    /// Returns a new FunctionContext with the same constant args, fragment-local state, and
    /// debug flag as this FunctionContext. The caller is responsible for calling delete on
    /// it.
    FunctionContext* clone(MemPool* pool);

    void set_constant_columns(std::vector<ColumnPtr> columns) { _constant_columns = std::move(columns); }

    MemPool* mem_pool() { return _mem_pool; }
    size_t mem_usage() { return _mem_usage; }
    void add_mem_usage(size_t size) { _mem_usage += size; }

    RuntimeState* state() { return _state; }
    bool has_error() const;
    const char* error_msg() const;

    JavaUDAFContext* udaf_ctxs() { return _jvm_udaf_ctxs.get(); }

private:
    friend class ExprContext;

    MemPool* _mem_pool = nullptr;

    // We use the query's runtime state to report errors and warnings. NULL for test
    // contexts.
    RuntimeState* _state{nullptr};

    // Empty if there's no error
    mutable std::mutex _error_msg_mutex;
    std::string _error_msg;

    // The number of warnings reported.
    int64_t _num_warnings{0};

    /// The function state accessed via FunctionContext::Get/SetFunctionState()
    void* _thread_local_fn_state{nullptr};
    void* _fragment_local_fn_state{nullptr};

    // Type descriptor for the return type of the function.
    FunctionContext::TypeDesc _return_type;

    // Type descriptors for each argument of the function.
    // TODO: support complex type
    std::vector<FunctionContext::TypeDesc> _arg_types;

    std::vector<ColumnPtr> _constant_columns;

    // Indicates whether this context has been closed. Used for verification/debugging.
    bool _is_udf = false;

    // this is used for count memory usage of aggregate state
    size_t _mem_usage = 0;

    // UDAF Context
    std::unique_ptr<JavaUDAFContext> _jvm_udaf_ctxs;

    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
};

} // namespace starrocks
