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

} // namespace starrocks
namespace starrocks {

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
                 size_t column_start_idx);

// ConvertFunc is used to fill column's range [column_start_idx, column_start_idx + num_elements)
// with array's range [array_start_idx, array_start_idx + num_elements). A slot is null if
// null_data[i - column_start_idx] == DATUM_NULL, null slots are skipped.
// filter_data[i - column_start_idx] == DATUM_NULL, slot marked as 1 is filtered out
typedef Status (*ConvertFunc)(const arrow::Array* array, size_t array_start_idx, size_t num_elements, Column* column,
                              size_t column_start_idx, uint8_t* null_data, uint8_t* filter_data,
                              ArrowConvertContext* ctx);

// ListConvertFunc resembles to ConvertFunc, except that:
// 1. depth_limit is used to prevent a ListArray has too many nested layers.
// 2. type_desc is used to guide ArrayColumn to unfold its layers and we can utilize directly copying
// or simd optimization to speed up construction of each layer.
typedef Status (*ListConvertFunc)(const arrow::Array* array, size_t array_start_idx, size_t num_elements,
                                  Column* column, size_t column_start_idx, [[maybe_unused]] uint8_t* null_data,
                                  Filter* column_filter, ArrowConvertContext* ctx, const TypeDescriptor* type_desc);

typedef Status (*ListCheckDepthFunc)(const arrow::Array* array, size_t expected_depth);

// invoked when a arrow type fail to convert to StarRocks type.
Status illegal_converting_error(const std::string& arrow_type_name, const std::string& type_name);

// A triple [at, lt, is_nullable] can determine a optimized converter from converting at -> lt,
// is_nullable means original data has null slots.
ConvertFunc get_arrow_converter(ArrowTypeId at, LogicalType lt, bool is_nullable, bool is_strict);

// get list converter, it is used to convert a ListArray in arrow to ArrayColumn in StarRocks.
ListConvertFunc get_arrow_list_converter();

ListCheckDepthFunc get_arrow_list_check_depth();

// if there is no converter for a triple [at, lt, is_nullable], then call get_strict_type to obtain
// strict lt0, and converting is decomposed into two phases:
// phase1: convert at->lt0 by converter determined by the triple [at, lt0, is_nullable]
// phase2: convert lt0->lt by VectorCastExpr.
LogicalType get_strict_type(ArrowTypeId at);

} // namespace starrocks
