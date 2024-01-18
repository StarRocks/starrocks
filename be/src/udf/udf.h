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

#include "column/vectorized_fwd.h"
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

    void set_constant_columns(std::vector<starrocks::vectorized::ColumnPtr> columns);

    bool is_udf() { return _is_udf; }
    void set_is_udf(bool is_udf) { this->_is_udf = is_udf; }

    // Create a test FunctionContext object. The caller is responsible for calling delete
    // on it. This context has additional debugging validation enabled.
    static FunctionContext* create_test_context();
    static FunctionContext* create_test_context(std::vector<TypeDesc>&& arg_types, const TypeDesc& return_type);

    ~FunctionContext();

    bool error_if_overflow() const;

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
} // namespace starrocks_udf

using starrocks_udf::FunctionContext;
