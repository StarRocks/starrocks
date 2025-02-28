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

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/json_column.h"
#include "exprs/cast_expr.h"
#include "exprs/expr_context.h"
#include "gutil/casts.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/substitute.h"
#include "jsonpath.h"
#include "runtime/memory/memory_resource.h"
#include "types/logical_type.h"
#include "util/slice.h"
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
            field_chunk.append_column(elements, 0);
            ASSIGN_OR_RETURN(auto casted_field, _field_casts[i]->evaluate_checked(context, &field_chunk));
            casted_field = NullableColumn::wrap_if_necessary(casted_field);
            casted_fields.emplace_back(std::move(casted_field)->as_mutable_ptr());
        } else {
            casted_fields.emplace_back(NullableColumn::wrap_if_necessary(elements->clone())->as_mutable_ptr());
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
    return res;
}

} // namespace starrocks