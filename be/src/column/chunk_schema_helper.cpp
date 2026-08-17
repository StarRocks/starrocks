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

#include "column/chunk_schema_helper.h"

#include <algorithm>
#include <memory>
#include <string_view>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/field.h"
#include "column/schema.h"

namespace starrocks {

starrocks::SchemaPtr ChunkSchemaHelper::get_non_nullable_schema(const starrocks::SchemaPtr& schema,
                                                                const std::vector<int>* keys) {
    const auto& old_fields = schema->fields();
    Fields new_fields;
    new_fields.resize(old_fields.size());
    DCHECK(keys == nullptr || old_fields.size() == keys->size());

    int idx = 0;
    for (const auto& old_field : old_fields) {
        ColumnId id = old_field->id();
        std::string_view name = old_field->name();
        TypeInfoPtr type = old_field->type();
        starrocks::StorageAggregateType agg = old_field->aggregate_method();
        uint8_t short_key_length = old_field->short_key_length();
        bool is_key = old_field->is_key();
        bool nullable = false;

        auto new_field = std::make_shared<Field>(id, name, type, agg, short_key_length,
                                                 keys != nullptr ? static_cast<bool>((*keys)[idx]) : is_key, nullable);
        new_fields[idx] = new_field;
        ++idx;
    }

    return std::make_shared<starrocks::Schema>(new_fields, schema->keys_type(), schema->sort_key_idxes());
}

ColumnId ChunkSchemaHelper::max_column_id(const starrocks::Schema& schema) {
    ColumnId id = 0;
    for (const auto& field : schema.fields()) {
        id = std::max(id, field->id());
    }
    return id;
}

std::vector<size_t> ChunkSchemaHelper::get_char_field_indexes(const Schema& schema) {
    std::vector<size_t> char_field_indexes;
    for (size_t i = 0; i < schema.num_fields(); ++i) {
        const auto& field = schema.field(i);
        if (field->type()->type() == TYPE_CHAR) {
            char_field_indexes.push_back(i);
        }
    }
    return char_field_indexes;
}

ChunkPtr ChunkSchemaHelper::createDummyChunk() {
    ChunkPtr dummyChunk = std::make_shared<Chunk>();
    auto col = ColumnHelper::create_const_column<TYPE_INT>(1, 1);
    dummyChunk->append_column(std::move(col), 0);
    return dummyChunk;
}

} // namespace starrocks
