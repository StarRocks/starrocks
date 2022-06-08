// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/udf/udf.h

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

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "runtime/primitive_type.h"
#include "runtime/types.h"

// This is the only StarRocks header required to develop UDFs and UDAs. This header
// contains the types that need to be used and the FunctionContext object. The context
// object serves as the interface object between the UDF/UDA and the starrocks process.
namespace starrocks {
class FunctionContextImpl;
}

namespace starrocks {
namespace vectorized {
class Column;
} // namespace vectorized
} // namespace starrocks

namespace starrocks_udf {

// The FunctionContext is passed to every UDF/UDA and is the interface for the UDF to the
// rest of the system. It contains APIs to examine the system state, report errors
// and manage memory.
class FunctionContext {
public:
    enum StarRocksVersion {
        V2_0,
    };

    // keep the order with PrimitiveType

    struct TypeDesc {
        ::starrocks::PrimitiveType type = ::starrocks::TYPE_NULL;

        /// Only valid if type == TYPE_DECIMAL
        int precision = 0;
        int scale = 0;

        /// Only valid if type == TYPE_FIXED_BUFFER || type == TYPE_VARCHAR
        int len = 0;
    };

    struct UniqueId {
        int64_t hi = 0;
        int64_t lo = 0;
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

    // Returns the version of StarRocks that's currently running.
    StarRocksVersion version() const;

    // Returns the user that is running the query. Returns NULL if it is not
    // available.
    const char* user() const;

    // Returns the query_id for the current query.
    UniqueId query_id() const;

    // Sets an error for this UDF. If this is called, this will trigger the
    // query to fail.
    // Note: when you set error for the UDFs used in Data Load, you should
    // ensure the function return value is null.
    void set_error(const char* error_msg);

    // when you reused this FunctionContext, you maybe need clear the error status and message.
    void clear_error_msg();

    // Adds a warning that is returned to the user. This can include things like
    // overflow or other recoverable error conditions.
    // Warnings are capped at a maximum number. Returns true if the warning was
    // added and false if it was ignored due to the cap.
    bool add_warning(const char* warning_msg);

    // Returns true if there's been an error set.
    bool has_error() const;

    // Returns the current error message. Returns NULL if there is no error.
    const char* error_msg() const;

    // Allocates memory for UDAs. All UDA allocations should use this if possible instead of
    // malloc/new. The UDA is responsible for calling Free() on all buffers returned
    // by Allocate().
    // If this Allocate causes the memory limit to be exceeded, the error will be set
    // in this object causing the query to fail.
    uint8_t* allocate(int byte_size);

    // Reallocates 'ptr' to the new byte_size. If the currently underlying allocation
    // is big enough, the original ptr will be returned. If the allocation needs to
    // grow, a new allocation is made that is at least 'byte_size' and the contents
    // of 'ptr' will be copied into it.
    // This should be used for buffers that constantly get appended to.
    uint8_t* reallocate(uint8_t* ptr, int byte_size);

    // Frees a buffer returned from Allocate() or Reallocate()
    void free(uint8_t* buffer);

    // For allocations that cannot use the Allocate() API provided by this
    // object, TrackAllocation()/Free() can be used to just keep count of the
    // byte sizes. For each call to TrackAllocation(), the UDF/UDA must call
    // the corresponding Free().
    void track_allocation(int64_t byte_size);
    void free(int64_t byte_size);

    // TODO: Do we need to add arbitrary key/value metadata. This would be plumbed
    // through the query. E.g. "select UDA(col, 'sample=true') from tbl".
    // const char* GetMetadata(const char*) const;

    // TODO: Add mechanism for UDAs to update stats similar to runtime profile counters

    // TODO: Add mechanism to query for table/column stats

    // Returns the underlying opaque implementation object. The UDF/UDA should not
    // use this. This is used internally.
    starrocks::FunctionContextImpl* impl() { return _impl; }

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

    // Returns the intermediate type for UDAs, i.e., the one returned by
    // update and merge functions. Returns INVALID_TYPE for UDFs.
    const TypeDesc& get_intermediate_type() const;

    // Returns the number of arguments to this function (not including the FunctionContext*
    // argument).
    int get_num_args() const;

    // Returns _constant_columns size
    int get_num_constant_columns() const;

    // Returns the type information for the arg_idx-th argument (0-indexed, not including
    // the FunctionContext* argument). Returns NULL if arg_idx is invalid.
    const TypeDesc* get_arg_type(int arg_idx) const;

    bool is_constant_column(int arg_idx) const;

    // Return true if it's constant and not null
    bool is_notnull_constant_column(int i) const;

    std::shared_ptr<starrocks::vectorized::Column> get_constant_column(int arg_idx) const;

    bool is_udf() { return _is_udf; }
    void set_is_udf(bool is_udf) { this->_is_udf = is_udf; }

    // Create a test FunctionContext object. The caller is responsible for calling delete
    // on it. This context has additional debugging validation enabled.
    static FunctionContext* create_test_context();
    static FunctionContext* create_test_context(std::vector<TypeDesc>&& arg_types, const TypeDesc& return_type);

    ~FunctionContext();

private:
    friend class starrocks::FunctionContextImpl;
    FunctionContext();

    // Disable copy ctor and assignment operator
    FunctionContext(const FunctionContext& other);
    FunctionContext& operator=(const FunctionContext& other);

    bool _is_udf = false;

    // Owned by this object.
    starrocks::FunctionContextImpl* _impl;
};

//----------------------------------------------------------------------------
//------------------------------- UDFs ---------------------------------------
//----------------------------------------------------------------------------
// The UDF function must implement this function prototype. This is not
// a typedef as the actual UDF's signature varies from UDF to UDF.
//    typedef <*Val> Evaluate(FunctionContext* context, <const Val& arg>);
//
// The UDF must return one of the *Val structs. The UDF must accept a pointer
// to a FunctionContext object and then a const reference for each of the input arguments.
// NULL input arguments will have NULL passed in.
// Examples of valid Udf signatures are:
//  1) DoubleVal Example1(FunctionContext* context);
//  2) IntVal Example2(FunctionContext* context, const IntVal& a1, const DoubleVal& a2);
//
// UDFs can be variadic. The variable arguments must all come at the end and must be
// the same type. A example signature is:
//  StringVal Concat(FunctionContext* context, const StringVal& separator,
//    int num_var_args, const StringVal* args);
// In this case args[0] is the first variable argument and args[num_var_args - 1] is
// the last.
//
// The UDF should not maintain any state across calls since there is no guarantee
// on how the execution is multithreaded or distributed. Conceptually, the UDF
// should only read the input arguments and return the result, using only the
// FunctionContext as an external object.
//
// Memory Managment: the UDF can assume that memory from input arguments will have
// the same lifetime as results for the UDF. In other words, the UDF can return
// memory from input arguments without making copies. For example, a function like
// substring will not need to allocate and copy the smaller string. For cases where
// the UDF needs a buffer, it should use the StringValue(FunctionContext, len) c'tor.
//
// The UDF can optionally specify a Prepare function. The prepare function is called
// once before any calls to the Udf to evaluate values. This is the appropriate time for
// the Udf to validate versions and things like that.
// If there is an error, this function should call FunctionContext::set_error()/
// FunctionContext::add_warning().
typedef void (*UdfPrepareFn)(FunctionContext* context);

/// --- Prepare / Close Functions ---
/// ---------------------------------
/// The UDF can optionally include a prepare function, specified in the "CREATE FUNCTION"
/// statement using "prepare_fn=<prepare function symbol>". The prepare function is called
/// before any calls to the UDF to evaluate values. This is the appropriate time for the
/// UDF to initialize any shared data structures, validate versions, etc. If there is an
/// error, this function should call FunctionContext::SetError()/
/// FunctionContext::AddWarning().
//
/// The prepare function is called multiple times with different FunctionStateScopes. It
/// will be called once per fragment with 'scope' set to FRAGMENT_LOCAL, and once per
/// execution thread with 'scope' set to THREAD_LOCAL.
typedef void (*UdfPrepare)(FunctionContext* context, FunctionContext::FunctionStateScope scope);

/// The UDF can also optionally include a close function, specified in the "CREATE
/// FUNCTION" statement using "close_fn=<close function symbol>". The close function is
/// called after all calls to the UDF have completed. This is the appropriate time for the
/// UDF to deallocate any shared data structures that are not needed to maintain the
/// results. If there is an error, this function should call FunctionContext::SetError()/
/// FunctionContext::AddWarning().
//
/// The close function is called multiple times with different FunctionStateScopes. It
/// will be called once per fragment with 'scope' set to FRAGMENT_LOCAL, and once per
/// execution thread with 'scope' set to THREAD_LOCAL.
typedef void (*UdfClose)(FunctionContext* context, FunctionContext::FunctionStateScope scope);

typedef uint8_t* BufferVal;
} // namespace starrocks_udf

using starrocks_udf::FunctionContext;
