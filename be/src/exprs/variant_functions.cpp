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

#include "base/simd/simd.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/variant_column.h"
#include "column/variant_converter.h"
#include "column/variant_path_parser.h"
#include "exprs/variant_path_reader.h"
#include "runtime/runtime_state.h"

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
    context->set_function_state(scope, new VariantPath(std::move(variant_path_status.value())));
    VLOG(10) << "Preloaded variant path: " << path_string;

    return Status::OK();
}

Status VariantFunctions::variant_segments_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        delete reinterpret_cast<VariantPath*>(context->get_function_state(scope));
    }
    return Status::OK();
}

template <LogicalType ResultType>
static void append_variant_field_to_result(FunctionContext* context, VariantRowValue&& field,
                                           ColumnBuilder<ResultType>* result) {
    if constexpr (ResultType == TYPE_VARIANT) {
        result->append(std::move(field));
    } else {
        const RuntimeState* state = context->state();
        cctz::time_zone zone = (state == nullptr) ? cctz::local_time_zone() : context->state()->timezone_obj();
        Status casted = VariantConverter::cast_to<ResultType, true>(field.as_ref(), zone, *result);
        if (!casted.ok()) {
            result->append_null();
        }
    }
}

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

// Columnar cast fast-path: const path + typed match (no suffix) + typed_type != ResultType.
// Encodes each typed datum to a VariantRowValue and then casts to ResultType.
// This avoids the full row-by-row encode→VariantRowValue→VariantConverter pipeline.
// The typed null / variant null masks are merged into the result.
template <LogicalType ResultType>
static ColumnPtr _build_typed_cast_result(FunctionContext* context, const Column* typed_col,
                                          const TypeDescriptor& typed_type_desc, const Column* variant_col,
                                          size_t num_rows) {
    const RuntimeState* state = context->state();
    cctz::time_zone zone = (state == nullptr) ? cctz::local_time_zone() : state->timezone_obj();

    ColumnBuilder<ResultType> result(num_rows);

    // Compute the merged null mask upfront: variant_null | typed_null.
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
        // Encode the typed datum as a VariantRowValue, then cast to ResultType.
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
        Status cast_status = VariantConverter::cast_to<ResultType, true>(encoded.value.as_ref(), zone, result);
        if (!cast_status.ok()) {
            result.append_null();
        }
    }

    return result.build(false);
}

template <LogicalType ResultType>
StatusOr<ColumnPtr> VariantFunctions::_do_variant_query(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    if (columns.size() != 2) {
        return Status::InvalidArgument("Variant query functions requires 2 arguments");
    }

    const size_t num_rows = columns[0]->size();

    auto variant_viewer = ColumnViewer<TYPE_VARIANT>(columns[0]);
    auto path_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    const auto* variant_data_column = down_cast<const VariantColumn*>(ColumnHelper::get_data_column(columns[0].get()));

    // For const paths, the parsed VariantPath is cached in FunctionContext.
    // Prepare a local reader once per batch; for non-const paths, re-prepare per row.
    auto* cached_path = reinterpret_cast<VariantPath*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    VariantPathReader reader;
    if (cached_path != nullptr) {
        reader.prepare(variant_data_column, cached_path);
    }

    // BATCH / COLUMNAR FAST-PATH: const path + typed match + no suffix + non-const variant column.
    // kTypedNoSuffix means the path maps directly to a typed column with no additional seek.
    // - typed_type == ResultType: bulk Column::append() (zero-copy for fixed-length)
    // - typed_type != ResultType: columnar cast (per-datum, avoids VariantRowValue round-trip)
    // ConstColumn typed columns are excluded because FixedLengthColumn::append() doesn't handle them.
    if constexpr (ResultType != TYPE_VARIANT) {
        if (cached_path != nullptr && !columns[0]->is_constant() && reader.is_typed_exact() &&
            !reader.typed_column()->is_constant()) {
            if (reader.typed_type() == ResultType) {
                return _build_typed_bulk_result<ResultType>(reader.typed_column(), columns[0].get(), num_rows);
            } else {
                return _build_typed_cast_result<ResultType>(context, reader.typed_column(), reader.typed_type_desc(),
                                                            columns[0].get(), num_rows);
            }
        }
    }

    // Row-by-row fallback.
    ColumnBuilder<ResultType> result(num_rows);
    VariantPath row_path;
    VariantPathReader row_reader;

    for (size_t row = 0; row < num_rows; ++row) {
        if (variant_viewer.is_null(row) || path_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        VariantPathReader* cur_reader;
        if (cached_path != nullptr) {
            cur_reader = &reader;
        } else {
            auto ps = VariantPathParser::parse(path_viewer.value(row).to_string());
            if (!ps.ok()) {
                result.append_null();
                continue;
            }
            row_path = std::move(ps).value();
            row_reader.prepare(variant_data_column, &row_path);
            cur_reader = &row_reader;
        }

        const size_t variant_row = columns[0]->is_constant() ? 0 : row;

        VariantReadResult read = cur_reader->read_row(variant_row);
        if (read.state != VariantReadState::kValue) {
            result.append_null();
            continue;
        }
        append_variant_field_to_result<ResultType>(context, std::move(read.value), &result);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> VariantFunctions::variant_typeof(FunctionContext* context, const Columns& columns) {
    const auto& variant_column = columns[0];
    auto variant_viewer = ColumnViewer<TYPE_VARIANT>(variant_column);
    size_t num_rows = variant_column->size();
    const auto* variant_data_column =
            down_cast<const VariantColumn*>(ColumnHelper::get_data_column(variant_column.get()));

    ColumnBuilder<TYPE_VARCHAR> result(num_rows);
    for (size_t row = 0; row < num_rows; ++row) {
        if (variant_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        const size_t variant_row = variant_column->is_constant() ? 0 : row;
        // Fast path: if any typed column is non-null for this row, the top-level type
        // is OBJECT (typed paths are always object-field paths, §1.3).
        // If all typed columns are null (all fields tombstoned), fall through to remain,
        // which may hold a scalar (e.g. a scalar row appended into a shredded column).
        {
            bool any_non_null = false;
            for (size_t i = 0; i < variant_data_column->shredded_paths().size(); ++i) {
                const Column* typed_col = variant_data_column->typed_column_by_index(i);
                if (typed_col != nullptr && !typed_col->is_null(variant_row)) {
                    any_non_null = true;
                    break;
                }
            }
            if (any_non_null) {
                result.append(VariantUtil::variant_type_to_string(VariantType::OBJECT));
                continue;
            }
        }

        // All typed columns null or no typed columns: read type from remain.
        VariantRowRef row_ref;
        if (variant_data_column->try_get_row_ref(variant_row, &row_ref)) {
            result.append(VariantUtil::variant_type_to_string(row_ref.get_value().type()));
            continue;
        }

        VariantRowValue variant_buffer;
        const VariantRowValue* variant = variant_data_column->get_row_value(variant_row, &variant_buffer);
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
