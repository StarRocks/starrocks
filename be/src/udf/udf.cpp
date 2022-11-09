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
#include "runtime/types.h"
#include "types/hll.h"
#include "udf/udf_internal.h"

#if STARROCKS_UDF_SDK_BUILD
// For the SDK build, we are building the .lib that the developers would use to
// write UDFs. They want to link against this to run their UDFs in a test environment.
// Pulling in free-pool is very undesirable since it pulls in many other libraries.
// Instead, we'll implement a dummy version that is not used.
// When they build their library to a .so, they'd use the version of FunctionContext
// in the main binary, which does include FreePool.
namespace starrocks {
class FreePool {
public:
    FreePool(MemPool*) {}

    uint8_t* allocate(int byte_size) { return reinterpret_cast<uint8_t*>(malloc(byte_size)); }

    uint8_t* reallocate(uint8_t* ptr, int byte_size) { return reinterpret_cast<uint8_t*>(realloc(ptr, byte_size)); }

    void free(uint8_t* ptr) { ::free(ptr); }
};

class RuntimeState {
public:
    void set_process_status(const std::string& error_msg) { assert(false); }

    bool log_error(const std::string& error) {
        assert(false);
        return false;
    }

    const std::string& user() const { return _user; }

private:
    std::string _user;
};
} // namespace starrocks
#else
#include "runtime/free_pool.hpp"
#include "runtime/runtime_state.h"
#endif

namespace starrocks {

FunctionContextImpl::FunctionContextImpl(starrocks_udf::FunctionContext* parent)
        : _varargs_buffer(nullptr),
          _varargs_buffer_size(0),
          _num_updates(0),
          _num_removes(0),
          _context(parent),
          _pool(nullptr),
          _state(nullptr),
          _debug(false),
          _version(starrocks_udf::FunctionContext::V2_0),
          _num_warnings(0),
          _thread_local_fn_state(nullptr),
          _fragment_local_fn_state(nullptr),
          _external_bytes_tracked(0),
          _closed(false) {}

FunctionContextImpl::~FunctionContextImpl() = default;

void FunctionContextImpl::close() {
    if (_closed) {
        return;
    }

    if (_external_bytes_tracked > 0) {
        // This isn't ideal because the memory is still leaked, but don't track it so our
        // accounting stays sane.
        // TODO: we need to modify the memtrackers to allow leaked user-allocated memory.
        _context->free(_external_bytes_tracked);
    }

    free(_varargs_buffer);
    _varargs_buffer = nullptr;

    _closed = true;
}

uint8_t* FunctionContextImpl::allocate_local(int64_t byte_size) {
    uint8_t* buffer = _pool->allocate(byte_size);
    _local_allocations.push_back(buffer);
    return buffer;
}

bool FunctionContextImpl::check_allocations_empty() {
    if (_allocations.empty() && _external_bytes_tracked == 0) {
        return true;
    }

    // TODO: fix this
    //if (_debug) _context->set_error("Leaked allocations.");
    return false;
}

bool FunctionContextImpl::check_local_allocations_empty() {
    if (_local_allocations.empty()) {
        return true;
    }

    // TODO: fix this
    //if (_debug) _context->set_error("Leaked local allocations.");
    return false;
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
    starrocks_udf::FunctionContext::TypeDesc invalid_type;
    invalid_type.type = INVALID_TYPE;
    invalid_type.precision = 0;
    invalid_type.scale = 0;
    return FunctionContextImpl::create_context(state, pool, invalid_type, return_type, arg_types, varargs_buffer_size,
                                               debug);
}

starrocks_udf::FunctionContext* FunctionContextImpl::create_context(
        RuntimeState* state, MemPool* pool, const starrocks_udf::FunctionContext::TypeDesc& intermediate_type,
        const starrocks_udf::FunctionContext::TypeDesc& return_type,
        const std::vector<starrocks_udf::FunctionContext::TypeDesc>& arg_types, int varargs_buffer_size, bool debug) {
    auto* ctx = new starrocks_udf::FunctionContext();
    ctx->_impl->_state = state;
    ctx->_impl->_mem_pool = pool;
    ctx->_impl->_pool = new FreePool(pool);
    ctx->_impl->_intermediate_type = intermediate_type;
    ctx->_impl->_return_type = return_type;
    ctx->_impl->_arg_types = arg_types;
    // UDFs may manipulate DecimalVal arguments via SIMD instructions such as 'movaps'
    // that require 16-byte memory alignment.
    // ctx->_impl->_varargs_buffer =
    //     reinterpret_cast<uint8_t*>(aligned_malloc(varargs_buffer_size, 16));
    ctx->_impl->_varargs_buffer = reinterpret_cast<uint8_t*>(malloc(varargs_buffer_size));
    ctx->_impl->_varargs_buffer_size = varargs_buffer_size;
    ctx->_impl->_debug = debug;
    ctx->_impl->_jvm_udaf_ctxs = std::make_unique<vectorized::JavaUDAFContext>();
    VLOG_ROW << "Created FunctionContext: " << ctx << " with pool " << ctx->_impl->_pool;
    return ctx;
}

FunctionContext* FunctionContextImpl::clone(MemPool* pool) {
    starrocks_udf::FunctionContext* new_context =
            create_context(_state, pool, _intermediate_type, _return_type, _arg_types, _varargs_buffer_size, _debug);

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
    context->impl()->_pool = new starrocks::FreePool(nullptr);
    return context;
}

FunctionContext* FunctionContext::create_test_context(std::vector<TypeDesc>&& arg_types, const TypeDesc& return_type) {
    FunctionContext* context = FunctionContext::create_test_context();
    context->impl()->_arg_types = std::move(arg_types);
    context->impl()->_return_type = return_type;
    return context;
}

FunctionContext::FunctionContext() : _impl(new starrocks::FunctionContextImpl(this)) {}

FunctionContext::~FunctionContext() {
    // TODO: this needs to free local allocations but there's a mem issue
    // in the uda harness now.
    _impl->check_local_allocations_empty();
    _impl->check_allocations_empty();
    delete _impl->_pool;
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
#if STARROCKS_UDF_SDK_BUILD
    id.hi = id.lo = 0;
#else
    id.hi = _impl->_state->query_id().hi;
    id.lo = _impl->_state->query_id().lo;
#endif
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

uint8_t* FunctionContext::allocate(int byte_size) {
    uint8_t* buffer = _impl->_pool->allocate(byte_size);
    _impl->_allocations[buffer] = byte_size;

    if (_impl->_debug) {
        memset(buffer, 0xff, byte_size);
    }

    return buffer;
}

uint8_t* FunctionContext::reallocate(uint8_t* ptr, int byte_size) {
    _impl->_allocations.erase(ptr);
    ptr = _impl->_pool->reallocate(ptr, byte_size);
    _impl->_allocations[ptr] = byte_size;
    return ptr;
}

void FunctionContext::free(uint8_t* buffer) {
    if (buffer == nullptr) {
        return;
    }

    if (_impl->_debug) {
        auto it = _impl->_allocations.find(buffer);

        if (it != _impl->_allocations.end()) {
            // fill in garbage value into the buffer to increase the chance of detecting misuse
            memset(buffer, 0xff, it->second);
            _impl->_allocations.erase(it);
            _impl->_pool->free(buffer);
        } else {
            set_error("FunctionContext::free() on buffer that is not freed or was not allocated.");
        }
    } else {
        _impl->_allocations.erase(buffer);
        _impl->_pool->free(buffer);
    }
}

void FunctionContext::track_allocation(int64_t bytes) {
    _impl->_external_bytes_tracked += bytes;
}

void FunctionContext::free(int64_t bytes) {
    _impl->_external_bytes_tracked -= bytes;
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
