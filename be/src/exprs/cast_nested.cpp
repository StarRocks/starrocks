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

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "exprs/cast_expr.h"
#include "exprs/expr_context.h"

namespace starrocks {

StatusOr<ColumnPtr> CastMapExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    ASSIGN_OR_RETURN(ColumnPtr orig_column, _children[0]->evaluate_checked(context, ptr));
    if (ColumnHelper::count_nulls(orig_column) == orig_column->size()) {
        return ColumnHelper::create_const_null_column(orig_column->size());
    }
    // NOTE: const(nullable) case is handled by last if case
    const auto* map_column = down_cast<const MapColumn*>(ColumnHelper::get_data_column(orig_column.get()));

    ColumnPtr casted_key_column;
    ColumnPtr casted_value_column;

    auto& src_keys_column = map_column->keys_column();
    auto& src_values_column = map_column->values_column();
    auto& src_offsets_column = map_column->offsets_column();
    // cast key column
    if (_key_cast != nullptr) {
        Chunk field_chunk;
        field_chunk.append_column(src_keys_column, 0);
        ASSIGN_OR_RETURN(casted_key_column, _key_cast->evaluate_checked(context, &field_chunk));
    } else {
        casted_key_column = std::move(*src_keys_column).mutate();
    }
    casted_key_column = NullableColumn::wrap_if_necessary(std::move(casted_key_column));

    // cast value column
    if (_value_cast != nullptr) {
        Chunk field_chunk;
        field_chunk.append_column(src_values_column, 0);
        ASSIGN_OR_RETURN(casted_value_column, _value_cast->evaluate_checked(context, &field_chunk));
    } else {
        casted_value_column = std::move(*src_values_column).mutate();
    }
    casted_value_column = NullableColumn::wrap_if_necessary(std::move(casted_value_column));
    auto casted_map = MapColumn::create(std::move(casted_key_column), std::move(casted_value_column),
                                        ColumnHelper::as_column<UInt32Column>(std::move(*src_offsets_column).mutate()));
    RETURN_IF_ERROR(down_cast<MapColumn*>(casted_map->as_mutable_raw_ptr())->unfold_const_children(_type));
    if (!orig_column->is_nullable()) {
        return casted_map;
    }
    // if the original column is nullable
    return NullableColumn::create(
            std::move(casted_map),
            ColumnHelper::as_column<NullColumn>(
                    std::move(*ColumnHelper::as_column<NullableColumn>(orig_column)->null_column()).mutate()));
}

StatusOr<ColumnPtr> CastStructExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    ASSIGN_OR_RETURN(ColumnPtr orig_column, _children[0]->evaluate_checked(context, ptr));
    if (ColumnHelper::count_nulls(orig_column) == orig_column->size()) {
        return ColumnHelper::create_const_null_column(orig_column->size());
    }
    // NOTE: const(nullable) case is handled by last if case
    const auto* struct_column = down_cast<const StructColumn*>(ColumnHelper::get_data_column(orig_column.get()));
    Columns casted_fields;
    for (int i = 0; i < _field_casts.size(); ++i) {
        if (_field_casts[i] != nullptr) {
            Chunk field_chunk;
            field_chunk.append_column(struct_column->fields()[i], 0);
            ASSIGN_OR_RETURN(auto casted_field, _field_casts[i]->evaluate_checked(context, &field_chunk));
            casted_field = NullableColumn::wrap_if_necessary(std::move(casted_field));
            casted_fields.emplace_back(std::move(casted_field));
        } else {
            auto& field_column = struct_column->fields()[i];
            casted_fields.emplace_back(NullableColumn::wrap_if_necessary(std::move(*field_column).mutate()));
        }
        DCHECK(casted_fields[i]->is_nullable());
    }

    auto casted_struct = StructColumn::create(std::move(casted_fields), _type.field_names);
    RETURN_IF_ERROR(down_cast<StructColumn*>(casted_struct->as_mutable_raw_ptr())->unfold_const_children(_type));
    if (!orig_column->is_nullable()) {
        return std::move(casted_struct);
    }
    // if the original column is nullable
    return NullableColumn::create(
            std::move(casted_struct),
            ColumnHelper::as_column<NullColumn>(
                    std::move(*(ColumnHelper::as_column<NullableColumn>(orig_column)->null_column())).mutate()));
}

StatusOr<ColumnPtr> CastArrayExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    ASSIGN_OR_RETURN(ColumnPtr orig_column, _children[0]->evaluate_checked(context, ptr));
    if (ColumnHelper::count_nulls(orig_column) == orig_column->size()) {
        return ColumnHelper::create_const_null_column(orig_column->size());
    }
    // NOTE: const(nullable) case is handled by last if case
    const auto* array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(orig_column.get()));

    ColumnPtr casted_element_column;
    // cast element column
    if (_element_cast != nullptr) {
        Chunk field_chunk;
        field_chunk.append_column(array_column->elements_column(), 0);
        ASSIGN_OR_RETURN(casted_element_column, _element_cast->evaluate_checked(context, &field_chunk));
    } else {
        casted_element_column = std::move(*(array_column->elements_column())).mutate();
    }
    casted_element_column = NullableColumn::wrap_if_necessary(std::move(casted_element_column));

    auto casted_array = ArrayColumn::create(
            std::move(casted_element_column),
            ColumnHelper::as_column<UInt32Column>(std::move(*(array_column->offsets_column())).mutate()));
    RETURN_IF_ERROR(down_cast<ArrayColumn*>(casted_array->as_mutable_raw_ptr())->unfold_const_children(_type));
    if (orig_column->is_constant()) {
        return ConstColumn::create(casted_array, orig_column->size());
    }
    if (!orig_column->is_nullable()) {
        return std::move(casted_array);
    }
    // if the original column is nullable
    return NullableColumn::create(
            std::move(casted_array),
            ColumnHelper::as_column<NullColumn>(
                    std::move(*(ColumnHelper::as_column<NullableColumn>(orig_column)->null_column())).mutate()));
}

} // namespace starrocks
