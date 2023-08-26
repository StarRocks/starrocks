// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/udf/udf_ir.cpp

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

#include "column/column.h"
#include "udf/udf_internal.h"

namespace starrocks_udf {

int FunctionContext::get_num_args() const {
    return _impl->_arg_types.size();
}

int FunctionContext::get_num_constant_columns() const {
    return _impl->_constant_columns.size();
}

bool FunctionContext::is_constant_column(int i) const {
    if (i < 0 || i >= _impl->_constant_columns.size()) {
        return false;
    }

    return !!_impl->_constant_columns[i] && _impl->_constant_columns[i]->is_constant();
}

bool FunctionContext::is_notnull_constant_column(int i) const {
    if (i < 0 || i >= _impl->_constant_columns.size()) {
        return false;
    }

    auto& col = _impl->_constant_columns[i];
    return !!col && col->is_constant() && !col->is_null(0);
}

starrocks::vectorized::ColumnPtr FunctionContext::get_constant_column(int i) const {
    if (i < 0 || i >= _impl->_constant_columns.size()) {
        return nullptr;
    }

    return _impl->_constant_columns[i];
}

const FunctionContext::TypeDesc& FunctionContext::get_return_type() const {
    return _impl->_return_type;
}

void* FunctionContext::get_function_state(FunctionStateScope scope) const {
    // assert(!_impl->_closed);
    switch (scope) {
    case THREAD_LOCAL:
        return _impl->_thread_local_fn_state;
    case FRAGMENT_LOCAL:
        return _impl->_fragment_local_fn_state;
    default:
        // TODO: signal error somehow
        return nullptr;
    }
}

void FunctionContext::set_runtime_state(starrocks::RuntimeState* state) {
    _impl->set_runtime_state(state);
}
starrocks::RuntimeState* FunctionContext::state() {
    return _impl->state();
}
std::vector<bool> FunctionContext::get_is_asc_order() {
    return _impl->get_is_asc_order();
}
std::vector<bool> FunctionContext::get_nulls_first() {
    return _impl->get_nulls_first();
}
bool FunctionContext::get_is_distinct() {
    return _impl->_is_distinct;
}
// for tests
void FunctionContext::set_is_asc_order(const std::vector<bool>& order) {
    _impl->set_is_asc_order(order);
}
void FunctionContext::set_nulls_first(const std::vector<bool>& nulls) {
    _impl->set_nulls_first(nulls);
}
void FunctionContext::set_is_distinct(bool is_distinct) {
    _impl->set_is_distinct(is_distinct);
}

ssize_t FunctionContext::get_group_concat_max_len() const {
    return _impl->get_group_concat_max_len();
}
// min value is 4, default is 1024
void FunctionContext::set_group_concat_max_len(ssize_t len) {
    _impl->set_group_concat_max_len(len);
}

} // namespace starrocks_udf
