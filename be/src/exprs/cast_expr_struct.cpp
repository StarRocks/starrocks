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

#include <cctz/time_zone.h>
#include <fmt/format.h>

#include <optional>
#include <vector>

#include "base/simd/simd.h"
#include "base/string/slice.h"
#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/json_column.h"
#include "column/variant_column.h"
#include "column/variant_converter.h"
#include "column/variant_path_parser.h"
#include "exprs/cast_expr.h"
#include "exprs/expr_context.h"
#include "gutil/casts.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/substitute.h"
#include "jsonpath.h"
#include "runtime/exception.h"
#include "runtime/memory/memory_resource.h"
#include "types/logical_type.h"
#include "velocypack/Iterator.h"

namespace starrocks {

#define APPEND_NULL(json_columns, null_column) \
    for (auto& json_column : json_columns) {   \
        json_column.append_null();             \
    }                                          \
    null_column->append(1);

StatusOr<ColumnPtr> CastJsonToStruct::evaluate_checked(ExprContext* context, Chunk* input_chunk) {
    ASSIGN_OR_RETURN(ColumnPtr column, _children[0]->evaluate_checked(context, input_chunk));
    if (column->only_null()) {
        return ColumnHelper::create_const_null_column(column->size());
    }

    ColumnViewer<TYPE_JSON> src(column);
    NullColumn::MutablePtr null_column = NullColumn::create();

    // 1. Cast Json to json columns.
    size_t field_size = _type.children.size();
    DCHECK_EQ(field_size, _type.field_names.size());
    vector<ColumnBuilder<TYPE_JSON>> json_columns;
    for (size_t i = 0; i < field_size; i++) {
        ColumnBuilder<TYPE_JSON> json_column_builder(src.size());
        json_columns.emplace_back(json_column_builder);
    }
    for (size_t i = 0; i < src.size(); i++) {
        if (src.is_null(i)) {
            APPEND_NULL(json_columns, null_column);
            continue;
        }
        const JsonValue* json_value = src.value(i);
        if (json_value && json_value->get_type() == JsonType::JSON_ARRAY) {
            vpack::Slice json_slice = json_value->to_vslice();
            DCHECK(json_slice.isArray());
            size_t index = 0;
            for (const auto& element : vpack::ArrayIterator(json_slice)) {
                if (index >= field_size) {
                    break;
                }
                JsonValue element_value(element);
                json_columns[index].append(std::move(element_value));
                index++;
            }
            if (index < field_size) {
                // Fill the other field with null.
                for (; index < field_size; index++) {
                    json_columns[index].append_null();
                }
            }
            null_column->append(0);
        } else if (json_value && json_value->get_type() == JsonType::JSON_OBJECT) {
            // For json object, the names of the struct fields must match the json object keys.
            // Otherwise, the value of the field will be NULL.
            for (int path_index = 0; path_index < _type.field_names.size(); path_index++) {
                vpack::Builder builder;
                if (path_index >= _json_paths.size()) {
                    json_columns[path_index].append_null();
                    continue;
                }
                vpack::Slice json_slice = JsonPath::extract(json_value, _json_paths[path_index], &builder);
                if (json_slice.isNone()) {
                    json_columns[path_index].append_null();
                    continue;
                }
                JsonValue element_value(json_slice);
                json_columns[path_index].append(std::move(element_value));
            }
            null_column->append(0);
        } else {
            APPEND_NULL(json_columns, null_column);
        }
    }
    // 2. Cast json column to specified column
    MutableColumns casted_fields;
    for (size_t i = 0; i < field_size; i++) {
        ColumnPtr elements = json_columns[i].build_nullable_column();
        if (_field_casts[i] != nullptr) {
            Chunk field_chunk;
            field_chunk.append_column(std::move(elements), 0);
            ASSIGN_OR_RETURN(auto casted_field, _field_casts[i]->evaluate_checked(context, &field_chunk));
            casted_field = NullableColumn::wrap_if_necessary(std::move(casted_field));
            casted_fields.emplace_back(std::move(*casted_field).mutate());
        } else {
            casted_fields.emplace_back(NullableColumn::wrap_if_necessary(std::move(*elements).mutate()));
        }
        DCHECK(casted_fields[i]->is_nullable());
    }

    MutableColumnPtr res = StructColumn::create(std::move(casted_fields), _type.field_names);
    RETURN_IF_ERROR(res->unfold_const_children(_type));
    if (column->is_nullable()) {
        res = NullableColumn::create(std::move(res), std::move(null_column));
    }

    // Wrap constant column if source column is constant.
    if (column->is_constant()) {
        res = ConstColumn::create(std::move(res), column->size());
    }
    return std::move(res);
}

namespace {

// The object rows of one batch, with every struct field reachable as a view.
// A row that can be viewed in place keeps a view of its object, and its fields are sought on demand. A row that has
// to be materialized (e.g. a shredded row with typed values) keeps only copies of the struct's fields: the full row
// is released as soon as they are taken, so no more than one materialized row is alive at a time.
class VariantObjectRows {
public:
    VariantObjectRows(const std::vector<VariantPath>& field_paths, size_t num_rows)
            : _field_paths(field_paths), _rows(num_rows), _materialized_index(num_rows, -1) {}

    void set_view(size_t row, const VariantRowRef& object) { _rows[row] = object; }

    void set_materialized(size_t row, const VariantRowRef& object) {
        _materialized_index[row] = static_cast<int32_t>(_materialized_fields.size() / _field_paths.size());
        for (const VariantPath& path : _field_paths) {
            auto field = VariantPath::seek_view(object, path);
            _materialized_fields.emplace_back(field.ok() ? std::optional(field.value().to_owned()) : std::nullopt);
        }
    }

    // Returns false when the object has no such key.
    bool field(size_t row, size_t field_idx, VariantRowRef* out) const {
        if (int32_t index = _materialized_index[row]; index >= 0) {
            const auto& value = _materialized_fields[index * _field_paths.size() + field_idx];
            if (!value.has_value()) {
                return false;
            }
            *out = value->as_ref();
            return true;
        }
        auto field = VariantPath::seek_view(_rows[row], _field_paths[field_idx]);
        if (!field.ok()) {
            return false;
        }
        *out = field.value();
        return true;
    }

private:
    const std::vector<VariantPath>& _field_paths;
    std::vector<VariantRowRef> _rows;
    std::vector<int32_t> _materialized_index;
    std::vector<std::optional<VariantRowValue>> _materialized_fields;
};

// Reads one struct field from every object row and casts it straight into the field's column, in a single pass.
// It must behave exactly like the generic VARIANT -> scalar cast (cast_from_variant_fn) it replaces: the same
// converter, the same time zone, NULL on failure, and the same error in throw mode.
template <LogicalType ResultType, bool AllowThrowException>
MutableColumnPtr cast_variant_field_direct(const VariantObjectRows& rows, const NullColumn& row_nulls,
                                           size_t field_idx) {
    const size_t num_rows = row_nulls.size();
    const uint8_t* is_null_row = row_nulls.get_data().data();
    const cctz::time_zone& zone = cctz::local_time_zone();
    ColumnBuilder<ResultType> builder(num_rows);
    VariantRowRef field;
    for (size_t row = 0; row < num_rows; ++row) {
        if (is_null_row[row] || !rows.field(row, field_idx, &field)) {
            builder.append_null();
            continue;
        }
        Status status = VariantRowConverter::cast_to<ResultType, AllowThrowException>(field, zone, builder);
        if (!status.ok()) {
            if constexpr (AllowThrowException) {
                throw RuntimeException(fmt::format("cast from {}({}) to {} failed", type_to_string(TYPE_VARIANT),
                                                   field.to_owned().to_string(), type_to_string(ResultType)));
            }
            builder.append_null();
        }
    }
    return builder.build_nullable_column();
}

template <bool AllowThrowException>
MutableColumnPtr try_cast_variant_field_direct(LogicalType field_type, const VariantObjectRows& rows,
                                               const NullColumn& row_nulls, size_t field_idx) {
#define CAST_VARIANT_FIELD_DIRECT(TYPE) \
    case TYPE:                          \
        return cast_variant_field_direct<TYPE, AllowThrowException>(rows, row_nulls, field_idx);
    switch (field_type) {
        CAST_VARIANT_FIELD_DIRECT(TYPE_BOOLEAN)
        CAST_VARIANT_FIELD_DIRECT(TYPE_TINYINT)
        CAST_VARIANT_FIELD_DIRECT(TYPE_SMALLINT)
        CAST_VARIANT_FIELD_DIRECT(TYPE_INT)
        CAST_VARIANT_FIELD_DIRECT(TYPE_BIGINT)
        CAST_VARIANT_FIELD_DIRECT(TYPE_LARGEINT)
        CAST_VARIANT_FIELD_DIRECT(TYPE_FLOAT)
        CAST_VARIANT_FIELD_DIRECT(TYPE_DOUBLE)
        CAST_VARIANT_FIELD_DIRECT(TYPE_VARCHAR)
        CAST_VARIANT_FIELD_DIRECT(TYPE_DATE)
        CAST_VARIANT_FIELD_DIRECT(TYPE_DATETIME)
        CAST_VARIANT_FIELD_DIRECT(TYPE_TIME)
    default:
        // DECIMAL, CHAR, JSON, VARIANT and nested types keep the generic two-pass cast.
        return nullptr;
    }
#undef CAST_VARIANT_FIELD_DIRECT
}

} // namespace

StatusOr<ColumnPtr> CastVariantToStruct::evaluate_checked(ExprContext* context, Chunk* input_chunk) {
    ASSIGN_OR_RETURN(ColumnPtr column, _children[0]->evaluate_checked(context, input_chunk));
    if (column->only_null()) {
        return ColumnHelper::create_const_null_column(column->size());
    }

    ColumnViewer<TYPE_VARIANT> viewer(column);
    const auto* variant_data_column = down_cast<const VariantColumn*>(ColumnHelper::get_data_column(column.get()));
    const size_t num_rows = viewer.size();
    NullColumn::MutablePtr null_column = NullColumn::create(num_rows, 0);
    uint8_t* is_null_row = null_column->get_data().data();

    // 1. Resolve every row to its variant object once. A row that is NULL or not an object becomes a NULL struct.
    // The name of a struct field must match a variant object key, otherwise the field is NULL.
    const size_t field_size = _type.children.size();
    DCHECK_EQ(field_size, _type.field_names.size());
    VariantObjectRows rows(_variant_paths, num_rows);
    for (size_t row = 0; row < num_rows; row++) {
        if (viewer.is_null(row)) {
            is_null_row[row] = 1;
            continue;
        }
        const size_t variant_row = column->is_constant() ? 0 : row;
        VariantRowRef object;
        if (variant_data_column->try_get_row_ref(variant_row, &object)) {
            if (object.get_value().type() != VariantType::OBJECT) {
                is_null_row[row] = 1;
                continue;
            }
            rows.set_view(row, object);
            continue;
        }
        VariantRowValue variant_buffer;
        const VariantRowValue* variant = variant_data_column->get_row_value(variant_row, &variant_buffer);
        if (variant == nullptr || variant->get_value().type() != VariantType::OBJECT) {
            is_null_row[row] = 1;
            continue;
        }
        rows.set_materialized(row, variant->as_ref());
    }

    // 2. Build each field.
    MutableColumns casted_fields;
    for (size_t field_idx = 0; field_idx < field_size; field_idx++) {
        const LogicalType field_type = _type.children[field_idx].type;
        MutableColumnPtr direct =
                _allow_throw_exception
                        ? try_cast_variant_field_direct<true>(field_type, rows, *null_column, field_idx)
                        : try_cast_variant_field_direct<false>(field_type, rows, *null_column, field_idx);
        if (direct != nullptr) {
            casted_fields.emplace_back(std::move(direct));
            continue;
        }

        // Generic path: collect the field values as a VARIANT column, then run the field's cast over it.
        auto field_values = VariantColumn::create();
        auto field_nulls = NullColumn::create();
        field_values->reserve(num_rows);
        field_nulls->reserve(num_rows);
        VariantRowRef field;
        for (size_t row = 0; row < num_rows; row++) {
            if (!is_null_row[row] && rows.field(row, field_idx, &field)) {
                field_values->append(field);
                field_nulls->append(0);
            } else {
                field_values->append_default();
                field_nulls->append(1);
            }
        }
        ColumnPtr elements = NullableColumn::create(std::move(field_values), std::move(field_nulls));
        if (_field_casts[field_idx] != nullptr) {
            Chunk field_chunk;
            field_chunk.append_column(elements, 0);
            ASSIGN_OR_RETURN(auto casted_field, _field_casts[field_idx]->evaluate_checked(context, &field_chunk));
            casted_field = NullableColumn::wrap_if_necessary(casted_field);
            casted_fields.emplace_back(std::move(casted_field)->as_mutable_ptr());
        } else {
            casted_fields.emplace_back(NullableColumn::wrap_if_necessary(elements->clone())->as_mutable_ptr());
        }
        DCHECK(casted_fields[field_idx]->is_nullable());
    }

    // 4. Build struct column.
    MutableColumnPtr res = StructColumn::create(std::move(casted_fields), _type.field_names);
    RETURN_IF_ERROR(res->unfold_const_children(_type));
    // The cast itself yields NULL rows (non-object root, unreadable row), so a non-nullable input
    // can still produce NULLs. Wrap whenever any row was marked NULL, not only for nullable input.
    if (column->is_nullable() || SIMD::contain_nonzero(null_column->get_data())) {
        res = NullableColumn::create(std::move(res), std::move(null_column));
    }
    if (column->is_constant()) {
        res = ConstColumn::create(std::move(res), column->size());
    }

    return res;
}

} // namespace starrocks
