// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>

#include "column/vectorized_fwd.h"
#include "storage/field.h"
#include "storage/olap_type_infra.h"

namespace starrocks {

class Status;
class TabletColumn;
class TabletSchema;
class SlotDescriptor;
class TupleDescriptor;

namespace vectorized {

class ChunkHelper {
public:
    static vectorized::Field convert_field(ColumnId id, const TabletColumn& c);

    static vectorized::Schema convert_schema(const starrocks::TabletSchema& schema);

    // Convert starrocks::TabletColumn to vectorized::Field. This function will generate format
    // V2 type: DATE_V2, TIMESTAMP, DECIMAL_V2
    static vectorized::Field convert_field_to_format_v2(ColumnId id, const TabletColumn& c);

    // Convert TabletSchema to vectorized::Schema with changing format v1 type to format v2 type.
    static vectorized::Schema convert_schema_to_format_v2(const starrocks::TabletSchema& schema);

    // Convert TabletSchema to vectorized::Schema with changing format v1 type to format v2 type.
    static vectorized::Schema convert_schema_to_format_v2(const starrocks::TabletSchema& schema,
                                                          const std::vector<ColumnId>& cids);

    // Get schema with format v2 type containing short key columns from TabletSchema.
    static vectorized::Schema get_short_key_schema_with_format_v2(const starrocks::TabletSchema& schema);

    static ColumnId max_column_id(const vectorized::Schema& schema);

    // Create an empty chunk according to the |schema| and reserve it of size |n|.
    static std::shared_ptr<Chunk> new_chunk(const vectorized::Schema& schema, size_t n);

    // Create an empty chunk according to the |tuple_desc| and reserve it of size |n|.
    static std::shared_ptr<Chunk> new_chunk(const TupleDescriptor& tuple_desc, size_t n);

    // Create an empty chunk according to the |slots| and reserve it of size |n|.
    static std::shared_ptr<Chunk> new_chunk(const std::vector<SlotDescriptor*>& slots, size_t n);

    static Chunk* new_chunk_pooled(const vectorized::Schema& schema, size_t n, bool force = true);

    // Create a vectorized column from field .
    // REQUIRE: |type| must be scalar type.
    static std::shared_ptr<Column> column_from_field_type(FieldType type, bool nullable);

    // Create a vectorized column from field.
    static std::shared_ptr<Column> column_from_field(const Field& field);

    // Get char column indexes
    static std::vector<size_t> get_char_field_indexes(const vectorized::Schema& schema);

    // Padding char columns
    static void padding_char_columns(const std::vector<size_t>& char_column_indexes, const vectorized::Schema& schema,
                                     const starrocks::TabletSchema& tschema, vectorized::Chunk* chunk);
};

} // namespace vectorized
} // namespace starrocks
