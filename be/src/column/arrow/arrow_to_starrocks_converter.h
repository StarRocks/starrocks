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

#include <functional>
#include <string>
#include <string_view>

#include "base/utility/meta_macro.h"
#include "column/array_column.h"
#include "column/arrow/arrow_type_traits.h"
#include "column/nullable_column.h"
#include "column/runtime_type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/substitute.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks {
struct ConvertFuncTree;

struct ArrowConvertContext {
    // `row_offset_in_array`:
    //   -1   -> no row offset available; downstream consumers (e.g. the
    //           Parquet rejected-records anchor) omit `row_in_file`.
    //   >= 0 -> row index relative to the RecordBatch start. The Parquet
    //           scanner adds `current_batch_first_row_in_file` to derive
    //           an absolute row index in the source file.
    using ErrorReporter =
            std::function<void(const std::string& reason, const std::string& raw_data, int64_t row_offset_in_array)>;

    std::string timezone;
    std::string current_file;
    std::string current_column_name;
    size_t current_type_length = 0;
    int error_message_counter = 0;
    ErrorReporter report_error_message = nullptr;

    // --- Parquet rejected-record anchor fields ---
    //
    // These let `_statistics_.rejected_records.source_info` carry a stable
    // pointer back to the offending row in the source Parquet file, so a
    // future `parquet_read_rows()` TVF can rehydrate the full row on
    // demand. They are scoped to the Broker-Load Parquet scanner (the
    // only path that populates them); Arrow Flight and other users leave
    // them at -1 and the reporter omits them from the emitted anchor.
    //
    // current_batch_first_row_in_file: absolute row number in the
    //     Parquet file where the RecordBatch currently being converted
    //     starts. ParquetScanner updates this before pulling each batch.
    // file_size / file_mtime_ms: snapshotted when the scanner opens the
    //     file. Included in the anchor so the TVF can fail-closed when
    //     the file has been modified since rejection was recorded.
    int64_t current_batch_first_row_in_file = -1;
    int64_t file_size = -1;
    int64_t file_mtime_ms = -1;

    void set_current_column(std::string_view column_name, const TypeDescriptor& type) {
        current_column_name = std::string(column_name);
        current_type_length = type.len > 0 ? static_cast<size_t>(type.len) : 0;
    }
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

Status convert_arrow_array_to_column(ConvertFuncTree* conv_func, size_t num_elements, const arrow::Array* array,
                                     Column* column, size_t batch_start_idx, size_t chunk_start_idx,
                                     Filter* chunk_filter, ArrowConvertContext* conv_ctx);

struct ConvertFuncTree {
    ConvertFuncTree(ConvertFunc f) : func(f){};
    ConvertFuncTree() = default;
    ConvertFunc func = nullptr;
    std::vector<std::string> field_names; // used in struct
    std::vector<std::unique_ptr<ConvertFuncTree>> children;
};

Status build_arrow_column_convert_plan(const arrow::DataType* arrow_type, const TypeDescriptor* type_desc,
                                       bool is_nullable, TypeDescriptor* raw_type_desc, ConvertFuncTree* conv_func,
                                       bool& need_cast, bool strict_mode);

MutableColumnPtr create_arrow_column_convert_dest(const TypeDescriptor& type_desc, const TypeDescriptor& raw_type_desc,
                                                  bool need_cast, bool is_nullable);

} // namespace starrocks
