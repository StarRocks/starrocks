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

#include "exprs/function_context.h"

#include <iostream>
#include <random>

#include "agent/master_info.h"
#include "column/array_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "exprs/agg/java_udaf_function.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "storage/rowset/bloom_filter.h"
#include "types/logical_type_infra.h"
#include "udf/java/java_udf.h"

namespace starrocks {

static const int MAX_WARNINGS = 1000;

FunctionContext* FunctionContext::create_context(RuntimeState* state, MemPool* pool,
                                                 const FunctionContext::TypeDesc& return_type,
                                                 const std::vector<FunctionContext::TypeDesc>& arg_types) {
    auto* ctx = new FunctionContext();
    ctx->_state = state;
    ctx->_mem_pool = pool;
    ctx->_return_type = return_type;
    ctx->_arg_types = arg_types;
    ctx->_jvm_udaf_ctxs = std::make_unique<JavaUDAFContext>();
    return ctx;
}

FunctionContext* FunctionContext::create_context(RuntimeState* state, MemPool* pool,
                                                 const FunctionContext::TypeDesc& return_type,
                                                 const std::vector<FunctionContext::TypeDesc>& arg_types,
                                                 bool is_distinct, const std::vector<bool>& is_asc_order,
                                                 const std::vector<bool>& nulls_first) {
    auto* ctx = new FunctionContext();
    ctx->_state = state;
    ctx->_mem_pool = pool;
    ctx->_return_type = return_type;
    ctx->_arg_types = arg_types;
    ctx->_jvm_udaf_ctxs = std::make_unique<JavaUDAFContext>();
    ctx->_is_distinct = is_distinct;
    ctx->_is_asc_order = is_asc_order;
    ctx->_nulls_first = nulls_first;
    return ctx;
}

FunctionContext* FunctionContext::create_test_context() {
    auto* context = new FunctionContext();
    context->_state = nullptr;
    return context;
}

FunctionContext* FunctionContext::create_test_context(std::vector<TypeDesc>&& arg_types, const TypeDesc& return_type) {
    FunctionContext* context = FunctionContext::create_test_context();
    context->_arg_types = std::move(arg_types);
    context->_return_type = return_type;
    return context;
}

FunctionContext::FunctionContext() = default;
FunctionContext::~FunctionContext() = default;

FunctionContext* FunctionContext::clone(MemPool* pool) {
    FunctionContext* new_context = create_context(_state, pool, _return_type, _arg_types);

    new_context->_constant_columns = _constant_columns;
    new_context->_fragment_local_fn_state = _fragment_local_fn_state;
    return new_context;
}

int FunctionContext::get_num_args() const {
    return _arg_types.size();
}

int FunctionContext::get_num_constant_columns() const {
    return _constant_columns.size();
}

bool FunctionContext::is_constant_column(int i) const {
    if (i < 0 || i >= _constant_columns.size()) {
        return false;
    }

    return _constant_columns[i] && _constant_columns[i]->is_constant();
}

bool FunctionContext::is_notnull_constant_column(int i) const {
    if (i < 0 || i >= _constant_columns.size()) {
        return false;
    }

    auto& col = _constant_columns[i];
    return col && col->is_constant() && !col->is_null(0);
}

ColumnPtr FunctionContext::get_constant_column(int i) const {
    if (i < 0 || i >= _constant_columns.size()) {
        return nullptr;
    }

    return _constant_columns[i];
}

const FunctionContext::TypeDesc& FunctionContext::get_return_type() const {
    return _return_type;
}

void* FunctionContext::get_function_state(FunctionStateScope scope) const {
    switch (scope) {
    case THREAD_LOCAL:
        return _thread_local_fn_state;
    case FRAGMENT_LOCAL:
        return _fragment_local_fn_state;
    default:
        // TODO: signal error somehow
        return nullptr;
    }
}

void FunctionContext::release_mems() {
    if (_jvm_udaf_ctxs != nullptr && _jvm_udaf_ctxs->states) {
        auto env = JVMFunctionHelper::getInstance().getEnv();
        _jvm_udaf_ctxs->states->clear(this, env);
    }
}

void FunctionContext::set_error(const char* error_msg, const bool is_udf) {
    std::lock_guard<std::mutex> lock(_error_msg_mutex);
    if (_error_msg.empty()) {
        _error_msg = error_msg;
        std::stringstream ss;
        ss << (is_udf ? "UDF ERROR: " : "") << error_msg;
        if (_state != nullptr) {
            _state->set_process_status(ss.str());
        }
    }
}

bool FunctionContext::has_error() const {
    std::lock_guard<std::mutex> lock(_error_msg_mutex);
    return !_error_msg.empty();
}

const char* FunctionContext::error_msg() const {
    std::lock_guard<std::mutex> lock(_error_msg_mutex);
    if (!_error_msg.empty()) {
        return _error_msg.c_str();
    } else {
        return nullptr;
    }
}

bool FunctionContext::error_if_overflow() const {
    return _state != nullptr && _state->error_if_overflow();
}

bool FunctionContext::allow_throw_exception() const {
    return _state != nullptr && _state->query_options().allow_throw_exception;
}

void FunctionContext::set_function_state(FunctionStateScope scope, void* ptr) {
    switch (scope) {
    case THREAD_LOCAL:
        _thread_local_fn_state = ptr;
        break;
    case FRAGMENT_LOCAL:
        _fragment_local_fn_state = ptr;
        break;
    default:
        std::stringstream ss;
        ss << "Unknown FunctionStateScope: " << scope;
        set_error(ss.str().c_str());
    }
}

std::mt19937_64* FunctionContext::driver_local_random_generator() {
    int32_t driver_id = CurrentThread::current().get_driver_id();

    std::lock_guard<std::mutex> lock(_rnd_mu);
    std::mt19937_64* res;
    auto iter = _driver_local_generators.find(driver_id);
    if (iter == _driver_local_generators.end()) {
        if (_global_seed == 0) {
            // init global seed
            std::random_device rnd;
            _global_seed = rnd();
            _default_generator.seed(_global_seed);
        }
        // use default generator to generate seed for driver-local generators
        // this way can make sure all seeds and generated number are deterministic
        std::uniform_int_distribution<> dist;
        auto gen = std::make_unique<std::mt19937_64>(dist(_default_generator));
        res = gen.get();
        _driver_local_generators.emplace(driver_id, std::move(gen));
    } else {
        res = iter->second.get();
    }

    return res;
}

void FunctionContext::reseed_random_number(int64_t seed) {
    // Use (seed + be_id) to guarantee every backend will generate different random number even if user provide the seed
    // why seq? be_id is unique but not deterministic when deploying a new cluster, so use a sequence instead of be_id
    // the basic assumption, the biggest cluster is 1024
    auto be_id = get_backend_id();
    if (be_id.has_value()) {
        int64_t seq = std::hash<int64_t>()(be_id.value()) % 1024;
        seed += seq;
    }
    std::lock_guard<std::mutex> lock(_rnd_mu);

    _global_seed = seed;
    _default_generator.seed(seed);
    _driver_local_generators.clear();
    VLOG(2) << "reseed_random_number to " << seed;
}

bool FunctionContext::add_warning(const char* warning_msg) {
    if (_num_warnings++ >= MAX_WARNINGS) {
        return false;
    }

    std::stringstream ss;
    ss << "UDF WARNING: " << warning_msg;

    std::cerr << ss.str() << std::endl;
    return true;
}

const FunctionContext::TypeDesc* FunctionContext::get_arg_type(int arg_idx) const {
    if (arg_idx < 0 || arg_idx >= _arg_types.size()) {
        return nullptr;
    }
    return &_arg_types[arg_idx];
}

struct ColumnBuilder {
    template <LogicalType Type>
    ColumnPtr operator()(const FunctionContext::TypeDesc& type_desc) {
        if constexpr (lt_is_decimal<Type>) {
            return RunTimeColumnType<Type>::create(type_desc.precision, type_desc.scale);
        } else if constexpr (lt_is_collection<Type>) {
            throw std::runtime_error(fmt::format("Unsupported collection type {}", Type));
            return nullptr;
        } else if constexpr (Type == TYPE_UNKNOWN || Type == TYPE_BINARY || Type == TYPE_DECIMAL) {
            throw std::runtime_error(fmt::format("Unsupported column type {}", Type));
            return nullptr;
        } else {
            return RunTimeColumnType<Type>::create();
        }
    }
};

ColumnPtr FunctionContext::create_column(const FunctionContext::TypeDesc& type_desc, bool nullable) {
    const auto type = type_desc.type;
    ColumnPtr p = nullptr;

    if (type == TYPE_STRUCT) {
        size_t field_size = type_desc.children.size();
        DCHECK_EQ(field_size, type_desc.field_names.size());
        Columns columns;
        for (size_t i = 0; i < field_size; i++) {
            ColumnPtr field_column = create_column(type_desc.children[i], true);
            columns.emplace_back(field_column);
        }
        p = StructColumn::create(columns, type_desc.field_names);
    } else if (type == TYPE_ARRAY) {
        auto offsets = UInt32Column::create();
        auto data = create_column(type_desc.children[0], true);
        p = ArrayColumn::create(std::move(data), std::move(offsets));
    } else if (type == TYPE_MAP) {
        auto offsets = UInt32Column ::create();
        ColumnPtr keys = nullptr;
        ColumnPtr values = nullptr;
        if (type_desc.children[0].type == TYPE_UNKNOWN) {
            FunctionContext::TypeDesc desc;
            desc.type = TYPE_NULL;
            keys = create_column(desc, true);
        } else {
            keys = create_column(type_desc.children[0], true);
        }
        if (type_desc.children[1].type == TYPE_UNKNOWN) {
            FunctionContext::TypeDesc desc;
            desc.type = TYPE_NULL;
            values = create_column(desc, true);
        } else {
            values = create_column(type_desc.children[1], true);
        }
        p = MapColumn::create(std::move(keys), std::move(values), std::move(offsets));
    } else {
        p = type_dispatch_column(type, ColumnBuilder(), type_desc);
    }

    if (nullable && p != nullptr) {
        return NullableColumn::create(p, NullColumn::create());
    }
    return p;
}

} // namespace starrocks
