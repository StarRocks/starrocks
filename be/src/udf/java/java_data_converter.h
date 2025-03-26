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

#pragma once

#include "column/binary_column.h"
#include "column/column_visitor.h"
#include "column/column_visitor_adapter.h"
#include "column/fixed_length_column.h"
#include "common/status.h"
#include "common/statusor.h"
#include "runtime/types.h"
#include "types/logical_type.h"
#include "udf/java/java_udf.h"

namespace starrocks {
struct JavaUDAFState {
    JavaUDAFState(int handle_) : handle(std::move(handle_)) {}
    ~JavaUDAFState() = default;
    // UDAF State
    int handle;
};

class JavaDataTypeConverter {
public:
    static jobject convert_to_states(FunctionContext* ctx, uint8_t** data, size_t offset, int num_rows);
    static jobject convert_to_states_with_filter(FunctionContext* ctx, uint8_t** data, size_t offset,
                                                 const uint8_t* filter, int num_rows);

    static Status convert_to_boxed_array(FunctionContext* ctx, const Column** columns, int num_cols, int num_rows,
                                         std::vector<jobject>* res);
};

#define SET_FUNCTION_CONTEXT_ERR(status, context)       \
    if (UNLIKELY(!status.ok())) {                       \
        context->set_error(status.to_string().c_str()); \
    }

StatusOr<jvalue> cast_to_jvalue(const TypeDescriptor& type_desc, bool is_boxed, const Column* col, int row_num);

void release_jvalue(bool is_boxed, jvalue val);
Status append_jvalue(const TypeDescriptor& type_desc, bool is_box, Column* col, jvalue val);
Status check_type_matched(const TypeDescriptor& type_desc, jobject val);
} // namespace starrocks
