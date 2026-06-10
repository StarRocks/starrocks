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

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "common/status.h"
#include "common/statusor.h"
#include "gutil/casts.h"
#include "runtime/global_dict/fragment_dict_state.h"
#include "runtime/global_dict/parser.h"
#include "runtime/global_dict/types.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"
#include "types/type_descriptor.h"

namespace starrocks {

namespace {
// Resolved once in the FRAGMENT_LOCAL prepare. Both overloads share it: the scalar overload sets
// `code`, the array overload sets `array_value` (a single-row ARRAY<INT>); `is_null` means the
// whole result is NULL. Which one applies is decided by arg0's type.
struct EncodeDictState {
    bool is_null = false;
    int32_t code = 0;
    ColumnPtr array_value;
};

// Resolve the forward (string -> code) dictionary for `slot_id`. A dict-encoded operand must have a
// dictionary, so a missing one is an inconsistent plan rather than a fall-back case.
StatusOr<const GlobalDictMap*> resolve_forward_dict(FunctionContext* context, int32_t slot_id) {
    RuntimeState* runtime_state = context->state();
    FragmentDictState* dict_state = runtime_state != nullptr ? runtime_state->fragment_dict_state() : nullptr;
    if (dict_state == nullptr) {
        return Status::InternalError("dict_encode: missing dict state for slot " + std::to_string(slot_id));
    }
    const GlobalDictMaps& dicts = dict_state->query_global_dicts();
    auto it = dicts.find(static_cast<uint32_t>(slot_id));
    if (it == dicts.end()) {
        // Derived (expression-backed) dictionaries are materialized lazily; evaluate this slot's
        // registered global-dict expr (as DictDecodeOperator does) and re-find before giving up.
        (void)dict_state->mutable_dict_optimize_parser()->eval_dict_expr(runtime_state, slot_id);
        it = dicts.find(static_cast<uint32_t>(slot_id));
    }
    if (it == dicts.end()) {
        return Status::InternalError("dict_encode: global dictionary not found for slot " + std::to_string(slot_id));
    }
    return &it->second.first;
}

// A code guaranteed absent from the encoded column. Global dict codes are a contiguous range
// 1..N (N = dict size) -- see ColumnDict.merge -- so N+1 is larger than every code. A value absent
// from the (complete) dictionary cannot occur in the column, so encoding it to this "no match" code
// makes comparisons against the column behave correctly (col = absent -> false, etc.). Every absent
// value maps to this SAME code, so absent values are not distinguishable from one another -- which
// is fine because the code is only ever compared against the dict column.
int32_t no_match_code(const GlobalDictMap& forward) {
    return static_cast<int32_t>(forward.size() + 1);
}
} // namespace

Status DictFunctions::dict_encode_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    // Resolve once per fragment; the global-dict map is stable here. Serves both the scalar
    // (value VARCHAR) and array (value ARRAY<VARCHAR>) overloads, dispatched on arg0's type.
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }
    auto* state = new EncodeDictState();
    context->set_function_state(scope, state);

    // dict_slot_id must be a non-null constant.
    if (!context->is_notnull_constant_column(1)) {
        return Status::InternalError("dict_encode: dict_slot_id must be a non-null constant");
    }
    const int32_t slot_id = ColumnHelper::get_const_value<TYPE_INT>(context->get_constant_column(1));

    // value must be a constant; a non-constant value cannot be folded to a single code/array.
    if (!context->is_constant_column(0)) {
        return Status::InternalError("dict_encode: value must be a constant");
    }
    // A NULL constant value (scalar or whole array) encodes to NULL.
    if (!context->is_notnull_constant_column(0)) {
        state->is_null = true;
        return Status::OK();
    }

    ASSIGN_OR_RETURN(const GlobalDictMap* forward, resolve_forward_dict(context, slot_id));

    if (!context->get_arg_type(0)->is_array_type()) {
        // Scalar: encode the single constant string to its code, or the no-match code if absent.
        const Slice value = ColumnHelper::get_const_value<TYPE_VARCHAR>(context->get_constant_column(0));
        if (auto f = forward->find(value); f != forward->end()) {
            state->code = static_cast<int32_t>(f->second);
        } else {
            state->code = no_match_code(*forward);
        }
        return Status::OK();
    }

    // Array: encode each element of the constant ARRAY<VARCHAR> into a constant ARRAY<INT>. The
    // const column wraps a single-row ArrayColumn whose elements_column() holds the array's elements.
    const ColumnPtr& arg0 = context->get_constant_column(0);
    const auto* array = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(arg0.get()));
    ColumnViewer<TYPE_VARCHAR> elements(array->elements_column());
    const size_t num_elements = array->elements_column()->size();

    const int32_t absent = no_match_code(*forward); // shared by all absent elements
    DatumArray encoded;
    encoded.reserve(num_elements);
    for (size_t i = 0; i < num_elements; ++i) {
        if (elements.is_null(i)) {
            encoded.emplace_back(); // NULL element stays NULL
            continue;
        }
        const Slice element = elements.value(i);
        if (auto f = forward->find(element); f != forward->end()) {
            encoded.emplace_back(static_cast<int32_t>(f->second));
        } else {
            encoded.emplace_back(absent);
        }
    }
    MutableColumnPtr result = ColumnHelper::create_column(TYPE_INT_ARRAY_DESC, false);
    Datum array_datum;
    array_datum.set_array(encoded);
    result->append_datum(array_datum); // a single array row
    state->array_value = std::move(result);
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
    const size_t num_rows = columns[0]->size();
    if (state == nullptr || state->is_null) {
        return ColumnHelper::create_const_null_column(num_rows);
    }
    if (state->array_value != nullptr) {
        return ConstColumn::create(state->array_value, num_rows);
    }
    return ColumnHelper::create_const_column<TYPE_INT>(state->code, num_rows);
}

} // namespace starrocks

#include "gen_cpp/opcode/DictFunctions.inc"
