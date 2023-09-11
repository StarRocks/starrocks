// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/udf/udf.cpp

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

#include "udf/udf.h"

#include <cassert>
#include <iostream>

#include "common/logging.h"
#include "exprs/agg/java_udaf_function.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "types/hll.h"
#include "udf/udf_internal.h"

namespace starrocks {

FunctionContextImpl::FunctionContextImpl(starrocks_udf::FunctionContext* parent)
        : _num_updates(0),
          _num_removes(0),
          _context(parent),
          _state(nullptr),
          _debug(false),
          _version(starrocks_udf::FunctionContext::V2_0),
          _num_warnings(0),
          _thread_local_fn_state(nullptr),
          _fragment_local_fn_state(nullptr),
          _closed(false) {}

FunctionContextImpl::~FunctionContextImpl() = default;

void FunctionContextImpl::close() {
    if (_closed) {
        return;
    }

    _closed = true;
}

void FunctionContextImpl::set_error(const char* error_msg) {
    std::lock_guard<std::mutex> lock(_error_msg_mutex);
    if (_error_msg.empty()) {
        _error_msg = error_msg;
        std::stringstream ss;
        ss << "UDF ERROR: " << error_msg;
        if (_state != nullptr) {
            _state->set_process_status(ss.str());
        }
    }
}

bool FunctionContextImpl::has_error() {
    std::lock_guard<std::mutex> lock(_error_msg_mutex);
    return !_error_msg.empty();
}

const char* FunctionContextImpl::error_msg() {
    std::lock_guard<std::mutex> lock(_error_msg_mutex);
    if (!_error_msg.empty()) {
        return _error_msg.c_str();
    } else {
        return nullptr;
    }
}

starrocks_udf::FunctionContext* FunctionContextImpl::create_context(
        RuntimeState* state, MemPool* pool, const starrocks_udf::FunctionContext::TypeDesc& return_type,
        const std::vector<starrocks_udf::FunctionContext::TypeDesc>& arg_types, int varargs_buffer_size, bool debug) {
    auto* ctx = new starrocks_udf::FunctionContext();
    ctx->_impl->_state = state;
    ctx->_impl->_mem_pool = pool;
    ctx->_impl->_return_type = return_type;
    ctx->_impl->_arg_types = arg_types;
    ctx->_impl->_debug = debug;
    ctx->_impl->_jvm_udaf_ctxs = std::make_unique<vectorized::JavaUDAFContext>();
    return ctx;
}

FunctionContext* FunctionContextImpl::clone(MemPool* pool) {
    starrocks_udf::FunctionContext* new_context = create_context(_state, pool, _return_type, _arg_types, 0, _debug);

    new_context->_impl->_constant_columns = _constant_columns;
    new_context->_impl->_fragment_local_fn_state = _fragment_local_fn_state;
    return new_context;
}

} // namespace starrocks

namespace starrocks_udf {
static const int MAX_WARNINGS = 1000;

FunctionContext* FunctionContext::create_test_context() {
    auto* context = new FunctionContext();
    context->impl()->_debug = true;
    context->impl()->_state = nullptr;
    return context;
}

FunctionContext* FunctionContext::create_test_context(std::vector<TypeDesc>&& arg_types, const TypeDesc& return_type) {
    FunctionContext* context = FunctionContext::create_test_context();
    context->impl()->_arg_types = std::move(arg_types);
    context->impl()->_return_type = return_type;
    return context;
}

bool FunctionContext::error_if_overflow() const {
    return _impl != nullptr && _impl->_state != nullptr && _impl->_state->error_if_overflow();
}

FunctionContext::FunctionContext() : _impl(new starrocks::FunctionContextImpl(this)) {}

FunctionContext::~FunctionContext() {
    delete _impl;
}

FunctionContext::StarRocksVersion FunctionContext::version() const {
    return _impl->_version;
}

const char* FunctionContext::user() const {
    if (_impl->_state == nullptr) {
        return nullptr;
    }

    return _impl->_state->user().c_str();
}

FunctionContext::UniqueId FunctionContext::query_id() const {
    UniqueId id;
    id.hi = _impl->_state->query_id().hi;
    id.lo = _impl->_state->query_id().lo;
    return id;
}

bool FunctionContext::has_error() const {
    return _impl->has_error();
}

const char* FunctionContext::error_msg() const {
    if (has_error()) {
        return _impl->_error_msg.c_str();
    }

    return nullptr;
}

void FunctionContext::set_function_state(FunctionStateScope scope, void* ptr) {
    assert(!_impl->_closed);
    switch (scope) {
    case THREAD_LOCAL:
        _impl->_thread_local_fn_state = ptr;
        break;
    case FRAGMENT_LOCAL:
        _impl->_fragment_local_fn_state = ptr;
        break;
    default:
        std::stringstream ss;
        ss << "Unknown FunctionStateScope: " << scope;
        set_error(ss.str().c_str());
    }
}

void FunctionContext::set_error(const char* error_msg) {
    _impl->set_error(error_msg);
}

void FunctionContext::set_constant_columns(std::vector<starrocks::vectorized::ColumnPtr> columns) {
    _impl->set_constant_columns(std::move(columns));
}

bool FunctionContext::add_warning(const char* warning_msg) {
    if (_impl->_num_warnings++ >= MAX_WARNINGS) {
        return false;
    }

    std::stringstream ss;
    ss << "UDF WARNING: " << warning_msg;

    if (_impl->_state != nullptr) {
        return _impl->_state->log_error(ss.str());
    } else {
        std::cerr << ss.str() << std::endl;
        return true;
    }
}

const FunctionContext::TypeDesc* FunctionContext::get_arg_type(int arg_idx) const {
    if (arg_idx < 0 || arg_idx >= _impl->_arg_types.size()) {
        return nullptr;
    }
    return &_impl->_arg_types[arg_idx];
}

} // namespace starrocks_udf
