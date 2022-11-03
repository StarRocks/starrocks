// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/udf/udf_internal.h

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
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "udf/udf.h"

namespace starrocks {

class FreePool;
class MemPool;
class RuntimeState;

namespace vectorized {
class Column;
struct JavaUDAFContext;
using ColumnPtr = std::shared_ptr<Column>;
} // namespace vectorized

// This class actually implements the interface of FunctionContext. This is split to
// hide the details from the external header.
// Note: The actual user code does not include this file.
class FunctionContextImpl {
public:
    /// Create a FunctionContext for a UDF. Caller is responsible for deleting it.
    static starrocks_udf::FunctionContext* create_context(
            RuntimeState* state, MemPool* pool, const starrocks_udf::FunctionContext::TypeDesc& return_type,
            const std::vector<starrocks_udf::FunctionContext::TypeDesc>& arg_types, int varargs_buffer_size,
            bool debug);

    /// Create a FunctionContext for a UDA. Identical to the UDF version except for the
    /// intermediate type. Caller is responsible for deleting it.
    static starrocks_udf::FunctionContext* create_context(
            RuntimeState* state, MemPool* pool, const starrocks_udf::FunctionContext::TypeDesc& intermediate_type,
            const starrocks_udf::FunctionContext::TypeDesc& return_type,
            const std::vector<starrocks_udf::FunctionContext::TypeDesc>& arg_types, int varargs_buffer_size,
            bool debug);

    ~FunctionContextImpl();

    FunctionContextImpl(starrocks_udf::FunctionContext* parent);

    void close();

    /// Returns a new FunctionContext with the same constant args, fragment-local state, and
    /// debug flag as this FunctionContext. The caller is responsible for calling delete on
    /// it.
    starrocks_udf::FunctionContext* clone(MemPool* pool);

    void set_constant_columns(std::vector<vectorized::ColumnPtr> columns) { _constant_columns = std::move(columns); }

    uint8_t* varargs_buffer() { return _varargs_buffer; }

    bool closed() const { return _closed; }

    int64_t num_updates() const { return _num_updates; }
    int64_t num_removes() const { return _num_removes; }
    void set_num_updates(int64_t n) { _num_updates = n; }
    void set_num_removes(int64_t n) { _num_removes = n; }
    void increment_num_updates(int64_t n) { _num_updates += n; }
    void increment_num_updates() { _num_updates += 1; }
    void increment_num_removes(int64_t n) { _num_removes += n; }
    void increment_num_removes() { _num_removes += 1; }

    MemPool* mem_pool() { return _mem_pool; }
    size_t mem_usage() { return _mem_usage; }
    void add_mem_usage(size_t size) { _mem_usage += size; }

    // Allocates a buffer of 'byte_size' with "local" memory management. These
    // allocations are not freed one by one but freed as a pool by FreeLocalAllocations()
    // This is used where the lifetime of the allocation is clear.
    // For UDFs, the allocations can be freed at the row level.
    // TODO: free them at the batch level and save some copies?
    uint8_t* allocate_local(int64_t byte_size);

    // Returns true if there are no outstanding allocations.
    bool check_allocations_empty();

    // Returns true if there are no outstanding local allocations.
    bool check_local_allocations_empty();

    RuntimeState* state() { return _state; }
    void set_error(const char* error_msg);
    bool has_error();
    const char* error_msg();

    vectorized::JavaUDAFContext* udaf_ctxs() { return _jvm_udaf_ctxs.get(); }

    std::string& string_result() { return _string_result; }

private:
    friend class starrocks_udf::FunctionContext;
    friend class ExprContext;

    /// Preallocated buffer for storing varargs (if the function has any). Allocated and
    /// owned by this object, but populated by an Expr function.
    //
    /// This is the first field in the class so it's easy to access in codegen'd functions.
    /// Don't move it or add fields above unless you know what you're doing.
    uint8_t* _varargs_buffer;
    int _varargs_buffer_size;

    // The number of calls to Update()/Remove().
    int64_t _num_updates;
    int64_t _num_removes;

    // Parent context object. Not owned
    starrocks_udf::FunctionContext* _context;

    MemPool* _mem_pool = nullptr;

    // Pool to service allocations from.
    FreePool* _pool;

    // We use the query's runtime state to report errors and warnings. NULL for test
    // contexts.
    RuntimeState* _state;

    // If true, indicates this is a debug context which will do additional validation.
    bool _debug;

    starrocks_udf::FunctionContext::StarRocksVersion _version;

    // Empty if there's no error
    std::mutex _error_msg_mutex;
    std::string _error_msg;

    // The number of warnings reported.
    int64_t _num_warnings;

    // Allocations made and still owned by the user function.
    std::map<uint8_t*, int> _allocations;
    std::vector<uint8_t*> _local_allocations;

    /// The function state accessed via FunctionContext::Get/SetFunctionState()
    void* _thread_local_fn_state;
    void* _fragment_local_fn_state;

    // The number of bytes allocated externally by the user function. In some cases,
    // it is too inconvenient to use the Allocate()/Free() APIs in the FunctionContext,
    // particularly for existing codebases (e.g. they use std::vector). Instead, they'll
    // have to track those allocations manually.
    int64_t _external_bytes_tracked;

    // Type descriptor for the intermediate type of a UDA. Set to INVALID_TYPE for UDFs.
    starrocks_udf::FunctionContext::TypeDesc _intermediate_type;

    // Type descriptor for the return type of the function.
    starrocks_udf::FunctionContext::TypeDesc _return_type;

    // Type descriptors for each argument of the function.
    std::vector<starrocks_udf::FunctionContext::TypeDesc> _arg_types;

    std::vector<vectorized::ColumnPtr> _constant_columns;

    // Indicates whether this context has been closed. Used for verification/debugging.
    bool _closed;

    std::string _string_result;

    // this is used for count memory usage of aggregate state
    size_t _mem_usage = 0;

    // UDAF Context
    std::unique_ptr<vectorized::JavaUDAFContext> _jvm_udaf_ctxs;
};

} // namespace starrocks
