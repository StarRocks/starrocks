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

#include "exprs/dict_functions.h"

#include <cstdint>
#include <string>

#include "column/column_helper.h"
#include "common/status.h"
#include "common/statusor.h"
#include "runtime/global_dict/config.h"
#include "runtime/global_dict/parser.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

namespace {
struct EncodeDictState {
    bool is_null = false;
    DictId code = 0;
};
} // namespace

Status DictFunctions::dict_encode_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }
    auto* state = new EncodeDictState();
    context->set_function_state(scope, state);

    if (!context->is_notnull_constant_column(1)) {
        return Status::InternalError("dict_encode: dict_slot_id must be a non-null constant");
    }
    const int32_t slot_id = ColumnHelper::get_const_value<TYPE_INT>(context->get_constant_column(1));

    if (!context->is_constant_column(0)) {
        return Status::InternalError("dict_encode: value must be a constant");
    }
    if (!context->is_notnull_constant_column(0)) {
        state->is_null = true;
        return Status::OK();
    }

    RuntimeState* runtime_state = context->state();
    if (runtime_state == nullptr) {
        return Status::InternalError("dict_encode: null runtime state for slot " + std::to_string(slot_id));
    }

    const Slice value = ColumnHelper::get_const_value<TYPE_VARCHAR>(context->get_constant_column(0));
    ASSIGN_OR_RETURN(state->code, runtime_state->mutable_dict_optimize_parser()->lookup_dict_code(slot_id, value));
    return Status::OK();
}

Status DictFunctions::dict_encode_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        delete reinterpret_cast<EncodeDictState*>(context->get_function_state(scope));
    }
    return Status::OK();
}

StatusOr<ColumnPtr> DictFunctions::dict_encode(FunctionContext* context, const Columns& columns) {
    auto* state = reinterpret_cast<EncodeDictState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state == nullptr) {
        return Status::InternalError("dict_encode: function state not initialized");
    }
    const size_t num_rows = columns[0]->size();
    if (state->is_null) {
        return ColumnHelper::create_const_null_column(num_rows);
    }
    return ColumnHelper::create_const_column<TYPE_INT>(state->code, num_rows);
}

} // namespace starrocks
