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

#include <arrow/array.h>
#include <arrow/status.h>

#include "column/array_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/arrow_type_traits.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/substitute.h"
#include "runtime/types.h"
#include "types/logical_type.h"
#include "util/meta_macro.h"

namespace starrocks {
class RuntimeState;
class SlotDescriptor;
struct ConvertFuncTree;

struct ArrowConvertContext {
    class RuntimeState* state;
    class SlotDescriptor* current_slot;
    std::string current_file;
    int error_message_counter = 0;
    void report_error_message(const std::string& reason, const std::string& raw_data);
};

// fill null_column's range [column_start_idx, column_start_idx + num_elements) with
// array's range [array_start_idx, array_start_idx + num_elements).
size_t fill_null_column(const arrow::Array* array, size_t array_start_idx, size_t num_elements, NullColumn*,
                        size_t column_start_idx);

// fill filter's range [column_start_idx, column_start_idx + num_elements) with
// array's range [array_start, array_start_idx + num_elements).
void fill_filter(const arrow::Array* array, size_t array_start_idx, size_t num_elements, Filter* filter,
                 size_t column_start_idx, ArrowConvertContext* ctx);

// ConvertFunc is used to fill column's range [column_start_idx, column_start_idx + num_elements)
// with array's range [array_start_idx, array_start_idx + num_elements). A slot is null if
// null_data[i - column_start_idx] == DATUM_NULL, null slots are skipped.
// filter_data[i - column_start_idx] == DATUM_NULL, slot marked as 1 is filtered out
typedef Status (*ConvertFunc)(const arrow::Array* array, size_t array_start_idx, size_t num_elements, Column* column,
                              size_t column_start_idx, uint8_t* null_data, Filter* chunk_filter,
                              ArrowConvertContext* ctx, ConvertFuncTree* conv_func);

// invoked when a arrow type fail to convert to StarRocks type.
Status illegal_converting_error(const std::string& arrow_type_name, const std::string& type_name);

// A triple [at, lt, is_nullable] can determine a optimized converter from converting at -> lt,
// is_nullable means original data has null slots.
ConvertFunc get_arrow_converter(ArrowTypeId at, LogicalType lt, bool is_nullable, bool is_strict);

// if there is no converter for a triple [at, lt, is_nullable], then call get_strict_type to obtain
// strict lt0, and converting is decomposed into two phases:
// phase1: convert at->lt0 by converter determined by the triple [at, lt0, is_nullable]
// phase2: convert lt0->lt by VectorCastExpr.
LogicalType get_strict_type(ArrowTypeId at);

struct ConvertFuncTree {
    ConvertFuncTree(ConvertFunc f) : func(f){};
    ConvertFuncTree() = default;
    ConvertFunc func = nullptr;
    std::vector<std::string> field_names; // used in struct
    std::vector<std::unique_ptr<ConvertFuncTree>> children;
};

} // namespace starrocks
