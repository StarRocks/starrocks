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

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "column/map_column.h"
#include "exprs/cast_expr.h"

namespace starrocks {

StatusOr<ColumnPtr> CastJsonToMap::evaluate_checked(ExprContext* context, Chunk* ptr) {
    ASSIGN_OR_RETURN(ColumnPtr src_column, _children[0]->evaluate_checked(context, ptr));
    if (ColumnHelper::count_nulls(src_column) == src_column->size()) {
        return ColumnHelper::create_const_null_column(src_column->size());
    }

    ColumnViewer<TYPE_JSON> src_viewer(src_column);
    NullColumn::MutablePtr null_column = NullColumn::create();
    UInt32Column::MutablePtr offsets_column = UInt32Column::create();
    ColumnBuilder<TYPE_VARCHAR> keys_builder(src_column->size());
    ColumnBuilder<TYPE_JSON> values_builder(src_column->size());

    // 1. Cast JsonObject to MAP<VARCHAR,JSON>
    uint32_t offset = 0;
    for (size_t i = 0; i < src_viewer.size(); i++) {
        offsets_column->append(offset);
        if (src_viewer.is_null(i)) {
            null_column->append(1);
            continue;
        }
        const JsonValue* json_value = src_viewer.value(i);
        if (json_value && json_value->get_type() == JsonType::JSON_OBJECT) {
            vpack::Slice json_slice = json_value->to_vslice();
            DCHECK(json_slice.isObject());
            for (const auto& pair : vpack::ObjectIterator(json_slice)) {
                keys_builder.append(pair.key.copyString());
                JsonValue value(pair.value);
                values_builder.append(std::move(value));
            }
            offset += json_slice.length();
            null_column->append(0);
        } else {
            null_column->append(1);
        }
    }
    offsets_column->append(offset);
    auto keys_column = keys_builder.build_nullable_column();
    auto values_column = values_builder.build_nullable_column();

    // 2. Cast key and value if needed
    if (_key_cast_expr != nullptr) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        SlotId slot_id = down_cast<ColumnRef*>(_key_cast_expr->get_child(0))->slot_id();
        chunk->append_column(keys_column, slot_id);
        ASSIGN_OR_RETURN(auto result, _key_cast_expr->evaluate_checked(context, chunk.get()));
        keys_column = ColumnHelper::cast_to_nullable_column(result);
    }
    if (_value_cast_expr != nullptr) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        SlotId slot_id = down_cast<ColumnRef*>(_value_cast_expr->get_child(0))->slot_id();
        chunk->append_column(values_column, slot_id);
        ASSIGN_OR_RETURN(auto result, _value_cast_expr->evaluate_checked(context, chunk.get()));
        values_column = ColumnHelper::cast_to_nullable_column(result);
    }

    auto map_column = MapColumn::create(std::move(keys_column), std::move(values_column), std::move(offsets_column));
    map_column->remove_duplicated_keys();
    RETURN_IF_ERROR(map_column->unfold_const_children(_type));
    return NullableColumn::create(std::move(map_column), std::move(null_column));
}

} // namespace starrocks