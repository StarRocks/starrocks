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

#include "exprs/variant_functions.h"

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "runtime/runtime_state.h"
#include "util/variant_converter.h"
#include "variant_path_parser.h"

namespace starrocks {

StatusOr<ColumnPtr> VariantFunctions::variant_query(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_VARIANT>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_string(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_VARCHAR>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_int(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_BIGINT>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_bool(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_BOOLEAN>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_double(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_DOUBLE>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_date(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_DATE>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_datetime(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_DATETIME>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_time(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_TIME>(context, columns);
}

Status VariantFunctions::variant_segments_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    // Don't parse if the path is not constant
    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    const auto path_col = context->get_constant_column(1);
    const Slice variant_path = ColumnHelper::get_const_value<TYPE_VARCHAR>(path_col);

    std::string path_string = variant_path.to_string();
    auto variant_path_status = VariantPathParser::parse(path_string);
    RETURN_IF(!variant_path_status.ok(), variant_path_status.status());
    auto* path_state = new VariantState();
    path_state->variant_path.reset(std::move(variant_path_status.value()));
    context->set_function_state(scope, path_state);
    VLOG(10) << "Preloaded variant path: " << path_string;

    return Status::OK();
}

static StatusOr<VariantPath*> get_or_parse_variant_segments(FunctionContext* context, const Slice path_slice,
                                                            VariantPath* variant_path) {
    auto* cached = reinterpret_cast<VariantState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    if (cached != nullptr) {
        // If we already have parsed segments, return them
        return &cached->variant_path;
    }

    std::string path_string = path_slice.to_string();
    auto path_status = VariantPathParser::parse(path_string);
    RETURN_IF(!path_status.ok(), path_status.status());
    variant_path->reset(std::move(path_status.value()));

    return variant_path;
}

Status VariantFunctions::variant_segments_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* variant_state = reinterpret_cast<VariantState*>(context->get_function_state(scope));
        delete variant_state;
    }

    return Status::OK();
}

<<<<<<< HEAD
=======
// Bulk fast-path: const path + exact typed-type match, no suffix.
// Copies the typed column data in bulk (SIMD for fixed-length, bulk for strings)
// and merges null masks from the outer variant column and the typed column.
template <LogicalType ResultType>
static ColumnPtr _build_typed_bulk_result(const Column* typed_col, const Column* variant_col, size_t num_rows) {
    // Unwrap nullable typed column
    const Column* typed_data = typed_col;
    const uint8_t* typed_null_data = nullptr;
    if (typed_col->is_nullable()) {
        const auto* tn = down_cast<const NullableColumn*>(typed_col);
        typed_data = tn->data_column().get();
        typed_null_data = tn->null_column()->get_data().data();
    }

    // Bulk-copy data column (SIMD memcpy for fixed-length, bulk string copy for varchar)
    auto result_data = RunTimeColumnType<ResultType>::create();
    result_data->append(*typed_data, 0, num_rows);

    // Build merged null mask: variant_null | typed_null
    auto result_null = NullColumn::create(num_rows, 0);
    uint8_t* rn = result_null->get_data().data();

    if (variant_col->is_nullable()) {
        const uint8_t* vn = down_cast<const NullableColumn*>(variant_col)->null_column()->get_data().data();
        for (size_t i = 0; i < num_rows; ++i) rn[i] = vn[i];
    }
    if (typed_null_data != nullptr) {
        for (size_t i = 0; i < num_rows; ++i) rn[i] |= typed_null_data[i];
    }

    bool has_null = SIMD::contain_nonzero(result_null->get_data());
    auto col = NullableColumn::create(std::move(result_data), std::move(result_null));
    col->set_has_null(has_null);
    return col;
}

// Row-level typed cast helper: encode each typed datum to VariantRowValue and
// reuse VariantRowConverter for query cast semantics.
template <LogicalType ResultType>
static ColumnPtr _build_typed_cast_result(FunctionContext* context, const Column* typed_col,
                                          const TypeDescriptor& typed_type_desc, const Column* variant_col,
                                          size_t num_rows) {
    if constexpr (ResultType == TYPE_BIGINT || ResultType == TYPE_DOUBLE) {
        TypeDescriptor result_type_desc = TypeDescriptor::from_logical_type(ResultType);
        cctz::time_zone zone =
                (context->state() == nullptr) ? cctz::local_time_zone() : context->state()->timezone_obj();
        auto casted = VariantColumnMerger::cast_typed_column(*typed_col, typed_type_desc, result_type_desc, zone);
        if (casted.ok()) {
            return _build_typed_bulk_result<ResultType>(casted.value().get(), variant_col, num_rows);
        }
    }

    const RuntimeState* state = context->state();
    cctz::time_zone zone = (state == nullptr) ? cctz::local_time_zone() : state->timezone_obj();
    TzOffsetCache offset_cache;
    offset_cache.bind(zone);

    ColumnBuilder<ResultType> result(num_rows);

    std::vector<uint8_t> null_mask(num_rows, 0);
    if (variant_col->is_nullable()) {
        const uint8_t* vn = down_cast<const NullableColumn*>(variant_col)->null_column()->get_data().data();
        for (size_t i = 0; i < num_rows; ++i) null_mask[i] = vn[i];
    }
    if (typed_col->is_nullable()) {
        const uint8_t* tn = down_cast<const NullableColumn*>(typed_col)->null_column()->get_data().data();
        for (size_t i = 0; i < num_rows; ++i) null_mask[i] |= tn[i];
    }

    for (size_t i = 0; i < num_rows; ++i) {
        if (null_mask[i]) {
            result.append_null();
            continue;
        }
        const size_t typed_row = typed_col->is_constant() ? 0 : i;
        auto encode_result = VariantColumn::encode_typed_row_as_variant(typed_col, typed_row, typed_type_desc);
        if (!encode_result.ok()) {
            result.append_null();
            continue;
        }
        auto encoded = std::move(encode_result).value();
        if (encoded.state == VariantColumn::EncodedVariantState::kNull) {
            result.append_null();
            continue;
        }
        Status cast_status =
                VariantRowConverter::cast_to<ResultType, true>(encoded.value.as_ref(), zone, result, &offset_cache);
        if (!cast_status.ok()) {
            result.append_null();
        }
    }

    return result.build(false);
}

>>>>>>> a4b68c1 ([Enhancement][RB-913] Cast VARIANT struct fields and get_variant_* results without copying the variant metadata (#82344))
template <LogicalType ResultType>
StatusOr<ColumnPtr> VariantFunctions::_do_variant_query(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    if (columns.size() != 2) {
        return Status::InvalidArgument("Variant query functions requires 2 arguments");
    }

    size_t num_rows = columns[0]->size();

    auto variant_viewer = ColumnViewer<TYPE_VARIANT>(columns[0]);
    auto json_path_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    ColumnBuilder<ResultType> result(num_rows);
<<<<<<< HEAD
    VariantPath stored_path;
=======
    VariantPath row_path;
    VariantPathReader row_reader;
    const RuntimeState* state = context->state();
    const cctz::time_zone zone = (state == nullptr) ? cctz::local_time_zone() : state->timezone_obj();
    TzOffsetCache offset_cache;
    offset_cache.bind(zone);
    // Backing storage for a field view when the row has to be materialised; reused across rows.
    VariantRowValue row_buffer;

>>>>>>> a4b68c1 ([Enhancement][RB-913] Cast VARIANT struct fields and get_variant_* results without copying the variant metadata (#82344))
    for (size_t row = 0; row < num_rows; ++row) {
        if (variant_viewer.is_null(row) || json_path_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto path_slice = json_path_viewer.value(row);
        auto variant_segments_status = get_or_parse_variant_segments(context, path_slice, &stored_path);
        if (!variant_segments_status.ok()) {
            result.append_null();
            continue;
        }

        const VariantRowValue* variant = variant_viewer.value(row);
        if (variant == nullptr) {
            result.append_null();
            continue;
        }

        auto field = VariantPath::seek(variant, variant_segments_status.value());
        if (!field.ok()) {
            // If seek fails (e.g., path not found), append null
            result.append_null();
            continue;
        }

        if constexpr (ResultType == TYPE_VARIANT) {
            result.append(std::move(field.value()));
        } else {
            const RuntimeState* state = context->state();
            cctz::time_zone zone = (state == nullptr) ? cctz::local_time_zone() : context->state()->timezone_obj();
            Status casted = cast_variant_value_to<ResultType, true>(field.value(), zone, result);
            // Append null if casting fails
            if (!casted.ok()) {
                result.append_null();
            }
        }
<<<<<<< HEAD
=======

        const size_t variant_row = columns[0]->is_constant() ? 0 : row;

        if constexpr (ResultType == TYPE_VARIANT) {
            VariantReadResult read = cur_reader->read_row(variant_row);
            if (read.state != VariantReadState::kValue) {
                result.append_null();
                continue;
            }
            result.append(std::move(read.value));
        } else {
            // A scalar result is cast straight from a view of the field; no owned copy of the row's metadata.
            VariantRowRef field;
            if (cur_reader->read_row_view(variant_row, &field, &row_buffer) != VariantReadState::kValue) {
                result.append_null();
                continue;
            }
            Status casted = VariantRowConverter::cast_to<ResultType, true>(field, zone, result, &offset_cache);
            if (!casted.ok()) {
                result.append_null();
            }
        }
>>>>>>> a4b68c1 ([Enhancement][RB-913] Cast VARIANT struct fields and get_variant_* results without copying the variant metadata (#82344))
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> VariantFunctions::variant_typeof(FunctionContext* context, const Columns& columns) {
    const auto& variant_column = columns[0];
    auto variant_viewer = ColumnViewer<TYPE_VARIANT>(variant_column);
    size_t num_rows = variant_column->size();

    ColumnBuilder<TYPE_VARCHAR> result(num_rows);
    for (size_t row = 0; row < num_rows; ++row) {
        if (variant_viewer.is_null(row)) {
            result.append_null();
            continue;
        }
        const VariantRowValue* variant = variant_viewer.value(row);
        if (variant == nullptr) {
            result.append_null();
            continue;
        }
        result.append(VariantUtil::variant_type_to_string(variant->get_value().type()));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks

#include "gen_cpp/opcode/VariantFunctions.inc"
