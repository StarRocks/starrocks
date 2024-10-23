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

#include <memory>

#include "column/vectorized_fwd.h"
#include "storage/olap_common.h"
#include "tablet_schema.h"

namespace starrocks {

class Status;
class TabletColumn;
class TabletSchema;
class SlotDescriptor;
class TupleDescriptor;

// TODO: rename it to SchemaHelper
class ChunkHelper {
public:
    // Convert TabletColumn to Field. This function will generate format
    // V2 type: DATE_V2, TIMESTAMP, DECIMAL_V2
    static Field convert_field(ColumnId id, const TabletColumn& c);

    // Convert TabletSchema to Schema with changing format v1 type to format v2 type.
    static Schema convert_schema(const TabletSchemaCSPtr& schema);

    // Convert TabletSchema to Schema with changing format v1 type to format v2 type.
    static Schema convert_schema(const TabletSchemaCSPtr& schema, const std::vector<ColumnId>& cids);

    // Convert TabletColumns to Schema order by col_names
    static SchemaPtr convert_schema(const std::vector<TabletColumn*>& columns,
                                    const std::vector<std::string_view>& col_names);

    // Get schema with format v2 type containing short key columns from TabletSchema.
    static Schema get_short_key_schema(const TabletSchemaCSPtr& schema);

    // Get schema with format v2 type containing sort key columns from TabletSchema.
    static Schema get_sort_key_schema(const TabletSchemaCSPtr& schema);

    // Get schema with format v2 type containing sort key columns filled by primary key columns from TabletSchema.
    static Schema get_sort_key_schema_by_primary_key(const starrocks::TabletSchemaCSPtr& tablet_schema);

    // Get non nullable version schema
    static SchemaPtr get_non_nullable_schema(const starrocks::SchemaPtr& schema, const std::vector<int>* keys);

    static ColumnId max_column_id(const Schema& schema);

    // Create an empty chunk according to the |schema| and reserve it of size |n|.
    static ChunkUniquePtr new_chunk(const Schema& schema, size_t n);

    // Create an empty chunk according to the |tuple_desc| and reserve it of size |n|.
    static ChunkUniquePtr new_chunk(const TupleDescriptor& tuple_desc, size_t n);

    // Create an empty chunk according to the |slots| and reserve it of size |n|.
    static ChunkUniquePtr new_chunk(const std::vector<SlotDescriptor*>& slots, size_t n);

    static Chunk* new_chunk_pooled(const Schema& schema, size_t n);

    // Create a vectorized column from field .
    // REQUIRE: |type| must be scalar type.
    static std::shared_ptr<Column> column_from_field_type(LogicalType type, bool nullable);

    // Create a vectorized column from field.
    static std::shared_ptr<Column> column_from_field(const Field& field);

    // Get char column indexes
    static std::vector<size_t> get_char_field_indexes(const Schema& schema);

    // Padding char columns
    static void padding_char_columns(const std::vector<size_t>& char_column_indexes, const Schema& schema,
                                     const TabletSchemaCSPtr& tschema, Chunk* chunk);

    // Reorder columns of `chunk` according to the order of |tuple_desc|.
    static void reorder_chunk(const TupleDescriptor& tuple_desc, Chunk* chunk);
    // Reorder columns of `chunk` according to the order of |slots|.
    static void reorder_chunk(const std::vector<SlotDescriptor*>& slots, Chunk* chunk);

    static ChunkPtr createDummyChunk();
};

typedef ChunkHelper SchemaHelper;

} // namespace starrocks
