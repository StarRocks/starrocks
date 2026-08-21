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

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "column/column.h"
#include "types/type_descriptor.h"

namespace starrocks {

class MemPool;
class RuntimeState;

struct NgramBloomFilterState;

// Base class for per-execution-thread function state (e.g. a Hyperscan scratch space) held by
// a shared FunctionContext via get_or_create_thread_state(). Derive from this and put the
// thread-private, mutable resources in the derived type.
class FunctionThreadState {
public:
    virtual ~FunctionThreadState() = default;
};

// A process-stable id for the current execution thread, assigned lazily on first use. Used to
// key per-thread state without relying on a per-thread FunctionContext clone. The pipeline
// runs on a bounded thread pool, so the id space stays small in practice.
int current_worker_id();

class FunctionContext {
public:
    using TypeDesc = TypeDescriptor;

    enum FunctionStateScope {
        /// The function's prepared state is shared across the whole plan fragment: it is built
        /// once and read concurrently by every execution thread, so it must be read-only /
        /// thread-safe after preparation. Genuinely per-thread, mutable resources (e.g. a
        /// Hyperscan scratch) must NOT live here — obtain them lazily during evaluation via
        /// FunctionContext::get_or_create_thread_state<T>() instead.
        FRAGMENT_LOCAL,
    };

    /// Create a FunctionContext for a UDF. Caller is responsible for deleting it.
    static FunctionContext* create_context(RuntimeState* state, MemPool* pool,
                                           const FunctionContext::TypeDesc& return_type,
                                           const std::vector<FunctionContext::TypeDesc>& arg_types);

    static FunctionContext* create_context(RuntimeState* state, MemPool* pool,
                                           const FunctionContext::TypeDesc& return_type,
                                           const std::vector<FunctionContext::TypeDesc>& arg_types, bool is_distinct,
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
    bool get_is_distinct() { return _is_distinct; }
    // for tests
    void set_is_asc_order(const std::vector<bool>& order) { _is_asc_order = order; }
    void set_nulls_first(const std::vector<bool>& nulls) { _nulls_first = nulls; }
    void set_runtime_state(RuntimeState* const state) { _state = state; }
    void set_is_distinct(bool is_distinct) { _is_distinct = is_distinct; }

    // Returns _constant_columns size
    int get_num_constant_columns() const;

    // Returns the type information for the arg_idx-th argument (0-indexed, not including
    // the FunctionContext* argument). Returns NULL if arg_idx is invalid.
    const TypeDesc* get_arg_type(int arg_idx) const;

    const std::vector<FunctionContext::TypeDesc>& get_arg_types() const { return _arg_types; }

    bool is_constant_column(int arg_idx) const;

    // Return true if it's constant and not null
    bool is_notnull_constant_column(int i) const;

    ColumnPtr get_constant_column(int arg_idx) const;

    bool is_udf() { return _is_udf; }
    void set_is_udf(bool is_udf) { this->_is_udf = is_udf; }

    // Create a test FunctionContext object. The caller is responsible for calling delete
    // on it. This context has additional debugging validation enabled.
    static FunctionContext* create_test_context();
    static FunctionContext* create_test_context(std::vector<TypeDesc>&& arg_types, const TypeDesc& return_type);

    /// Returns a new FunctionContext with the same constant args, fragment-local state, and
    /// debug flag as this FunctionContext. The caller is responsible for calling delete on
    /// it.
    FunctionContext* clone();

    void set_constant_columns(Columns columns) { _constant_columns = std::move(columns); }

    MemPool* mem_pool() { return _mem_pool; }

    void set_mem_usage_counter(int64_t* mem_usage_counter) { _mem_usage_counter = mem_usage_counter; }

    int64_t mem_usage() const {
        DCHECK(_mem_usage_counter);
        return *_mem_usage_counter;
    }
    void add_mem_usage(int64_t delta) {
        DCHECK(_mem_usage_counter);
        *_mem_usage_counter += delta;
    }

    RuntimeState* state() { return _state; }
    bool has_error() const;
    const char* error_msg() const;

    ssize_t get_group_concat_max_len() { return group_concat_max_len; }
    // min value is 4, default is 1024
    void set_group_concat_max_len(ssize_t len) { group_concat_max_len = len < 4 ? 4 : len; }

    // Max number of elements in an array produced by an array function; <= 0 means unlimited.
    ssize_t get_max_array_size() const { return _max_array_size; }
    void set_max_array_size(ssize_t size) { _max_array_size = size < 0 ? 0 : size; }

    bool error_if_overflow() const;

    bool allow_throw_exception() const;

    std::unique_ptr<NgramBloomFilterState>& get_ngram_state() { return _ngramState; }

    // Returns this worker thread's instance of T (which must derive from FunctionThreadState),
    // creating it via factory() on first access for this (FunctionContext, worker) pair.
    // factory() must return std::unique_ptr<T>. Thread-safe: multiple worker threads may call
    // concurrently on the same shared FunctionContext. The returned states are owned by this
    // FunctionContext and freed when it is destroyed (i.e. with the fragment), so a function's
    // eval can obtain per-thread scratch without a per-thread FunctionContext clone. The lock
    // is only taken to look up / create the slot; callers must not hold references across the
    // lifetime of the FunctionContext.
    template <class T, class Factory>
    T* get_or_create_thread_state(Factory&& factory) {
        static_assert(std::is_base_of<FunctionThreadState, T>::value, "T must derive from FunctionThreadState");
        int w = current_worker_id();
        std::lock_guard<std::mutex> l(_thread_state_mu);
        auto it = _thread_states.find(w);
        if (it == _thread_states.end()) {
            it = _thread_states.emplace(w, std::forward<Factory>(factory)()).first;
        }
        return static_cast<T*>(it->second.get());
    }

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

    Columns _constant_columns;

    // Indicates whether this context has been closed. Used for verification/debugging.
    bool _is_udf = false;

    int64_t _mem_usage = 0;
    // This is used to count the memory usage of the agg state.
    // In Aggregator, multiple FunctionContexts can share the same counter.
    // If it is not explicitly set externally (e.g. AggFuncBasedValueAggregator),
    // it will point to the internal _mem_usage
    int64_t* _mem_usage_counter = &_mem_usage;

    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
    bool _is_distinct = false;
    ssize_t group_concat_max_len = 1024;
    ssize_t _max_array_size = 0;

    // used for ngram bloom filter to speed up some function
    std::unique_ptr<NgramBloomFilterState> _ngramState;

    // Per-worker thread-state registry (see get_or_create_thread_state). Owned here, so the
    // states live as long as this FunctionContext (the fragment) and are freed with it.
    std::mutex _thread_state_mu;
    std::unordered_map<int, std::unique_ptr<FunctionThreadState>> _thread_states;
};

} // namespace starrocks
