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

    static FunctionContext* create_context(RuntimeState* state, MemPool* pool,
                                           const FunctionContext::TypeDesc& return_type,
                                           const std::vector<FunctionContext::TypeDesc>& arg_types,
                                           int varargs_buffer_size, bool debug, bool is_distinct,
                                           const std::vector<bool>& is_asc_order, const std::vector<bool>& nulls_first);
    ~FunctionContextImpl();

    FunctionContextImpl(starrocks_udf::FunctionContext* parent);

    void close();

    /// Returns a new FunctionContext with the same constant args, fragment-local state, and
    /// debug flag as this FunctionContext. The caller is responsible for calling delete on
    /// it.
    starrocks_udf::FunctionContext* clone(MemPool* pool);

    void set_constant_columns(std::vector<vectorized::ColumnPtr> columns) { _constant_columns = std::move(columns); }

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

    RuntimeState* state() { return _state; }
    void set_runtime_state(RuntimeState* state) { _state = state; }
    void set_error(const char* error_msg, bool is_udf = true);
    bool has_error();
    const char* error_msg();

    vectorized::JavaUDAFContext* udaf_ctxs() { return _jvm_udaf_ctxs.get(); }

    std::vector<bool> get_is_asc_order() { return _is_asc_order; }
    std::vector<bool> get_nulls_first() { return _nulls_first; }
    bool get_is_distinct() { return _is_distinct; }
    // for tests
    void set_is_asc_order(const std::vector<bool>& order) { _is_asc_order = order; }
    void set_nulls_first(const std::vector<bool>& nulls) { _nulls_first = nulls; }
    void set_is_distinct(bool is_distinct) { _is_distinct = is_distinct; }

    ssize_t get_group_concat_max_len() const { return group_concat_max_len; }
    // min value is 4, default is 1024
    void set_group_concat_max_len(ssize_t len) { group_concat_max_len = len < 4 ? 4 : len; }

private:
    friend class starrocks_udf::FunctionContext;
    friend class ExprContext;

    // The number of calls to Update()/Remove().
    int64_t _num_updates;
    int64_t _num_removes;

    // Parent context object. Not owned
    starrocks_udf::FunctionContext* _context;

    MemPool* _mem_pool = nullptr;

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

    /// The function state accessed via FunctionContext::Get/SetFunctionState()
    void* _thread_local_fn_state;
    void* _fragment_local_fn_state;

    // Type descriptor for the return type of the function.
    starrocks_udf::FunctionContext::TypeDesc _return_type;

    // Type descriptors for each argument of the function.
    std::vector<starrocks_udf::FunctionContext::TypeDesc> _arg_types;

    std::vector<vectorized::ColumnPtr> _constant_columns;

    // Indicates whether this context has been closed. Used for verification/debugging.
    bool _closed;

    // this is used for count memory usage of aggregate state
    size_t _mem_usage = 0;

    // UDAF Context
    std::unique_ptr<vectorized::JavaUDAFContext> _jvm_udaf_ctxs;

    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
    bool _is_distinct = false;
    ssize_t group_concat_max_len = 1024;
};

} // namespace starrocks
