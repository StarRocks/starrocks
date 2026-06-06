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
#include "types/logical_type.h"
#include "types/type_descriptor.h"
#include "udf/java/java_udf.h"

namespace starrocks {
struct JavaUDAFState {
    JavaUDAFState(int handle_) : handle(handle_) {}
    ~JavaUDAFState() = default;
    // UDAF State
    int handle;
};

class JavaDataTypeConverter {
public:
    static jobject convert_to_states(FunctionContext* ctx, uint8_t** data, size_t offset, int num_rows);
    static jobject convert_to_states_with_filter(FunctionContext* ctx, uint8_t** data, size_t offset,
                                                 const uint8_t* filter, int num_rows);

    // `arg_type_descs` is an optional per-argument com.starrocks.udf.UdfTypeDesc
    // jobject (mirroring the SQL TypeDescriptor of each argument and capturing the
    // formal Java record class at every STRUCT slot). Populated by the UDF caller
    // for any signature that contains a STRUCT in any position. When the caller has
    // no STRUCT in any arg subtree, the slot may be null jobject and the boxer
    // falls back to JavaArrayConverter.
    static Status convert_to_boxed_array(FunctionContext* ctx, const Column** columns, int num_cols, int num_rows,
                                         std::vector<jobject>* res,
                                         const std::vector<jobject>* arg_type_descs = nullptr);
};

// Build a com.starrocks.udf.UdfTypeDesc jobject mirroring the SQL TypeDescriptor,
// walking the formal Java reflective Type in lockstep so the formal record class
// is captured at every STRUCT slot. Returned reference is a local ref; caller
// promotes to a JavaGlobalRef for storage on the UDF context.
StatusOr<jobject> build_udf_type_desc(JNIEnv* env, const TypeDescriptor& td, jobject formal_type);

// Per-arg / per-return UdfTypeDesc tree built from a Java reflective Method object.
// Slots whose SQL type subtree contains no STRUCT are stored as null-handle JavaGlobalRef
// so callers can pass the vector verbatim to the boxer / writer (which fall back to the
// non-STRUCT path when a slot is null).
struct JavaUdfMethodTypeDescs {
    std::vector<JavaGlobalRef> args;
    JavaGlobalRef ret = JavaGlobalRef(nullptr);
};

// Walk Method.getGenericParameterTypes() / getGenericReturnType() in lockstep with the SQL
// type tree, and produce one UdfTypeDesc Java object per arg / return slot whose subtree
// contains a STRUCT. Slots with no STRUCT remain null-handle in the result vector.
//
// `state_offset` is the number of Java-method parameters that come BEFORE the SQL args.
// Scalar UDF and UDTF use 0; UDAF `update` has `State` as parameter 0, so state_offset = 1.
// Varargs handling unwraps the trailing array layer once per overflowing SQL arg.
//
// `unwrap_return_array_layer` strips one array layer off the Java reflective return type
// before pairing it with `sql_return_type`. UDTF needs this because the SQL return type
// is the per-row element while the Java method returns `T[]` (zero or more rows per call).
// Scalar UDF and UDAF return types match 1:1 and pass false.
//
// The result entries are global refs owned by the caller; null-handle entries are normal
// (built via JavaGlobalRef(nullptr)) and consume no JVM resources.
StatusOr<JavaUdfMethodTypeDescs> build_method_udf_type_descs(JNIEnv* env, jobject method_obj,
                                                             const std::vector<TypeDescriptor>& sql_arg_types,
                                                             const TypeDescriptor& sql_return_type, int state_offset,
                                                             bool unwrap_return_array_layer = false);

#define SET_FUNCTION_CONTEXT_ERR(status, context)       \
    if (UNLIKELY(!status.ok())) {                       \
        context->set_error(status.to_string().c_str()); \
    }

// Build a jvalue for a single-row cell of `col` at `row_num` according to `type_desc`.
// `type_desc_obj` is an optional UdfTypeDesc Java object (held by caller as a non-owning
// jobject local/global handle). It is required when `type_desc` contains STRUCT in any
// position (the formal record class lives in there); when the subtree has no STRUCT it
// may be null and the existing scalar/array/map paths are used unchanged.
StatusOr<jvalue> cast_to_jvalue(const TypeDescriptor& type_desc, bool is_boxed, const Column* col, int row_num,
                                jobject type_desc_obj = nullptr);

void release_jvalue(bool is_boxed, jvalue val);

// Append a Java-side jvalue to `col`, converting DECIMAL via the column's declared
// precision/scale. When the value overflows, `error_if_overflow == true` (default)
// returns an error Status (REPORT_ERROR), `false` appends NULL (OUTPUT_NULL).
//
// `type_desc_obj` is an optional UdfTypeDesc Java object; same contract as cast_to_jvalue.
Status append_jvalue(const TypeDescriptor& type_desc, bool is_box, Column* col, jvalue val,
                     bool error_if_overflow = true, jobject type_desc_obj = nullptr);
Status check_type_matched(const TypeDescriptor& type_desc, jobject val, jobject type_desc_obj = nullptr);
} // namespace starrocks
