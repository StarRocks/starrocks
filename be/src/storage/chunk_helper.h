// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/field.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "storage/olap_type_infra.h"
#include "storage/schema.h"

namespace starrocks {

class Status;
class TabletColumn;
class TabletSchema;

namespace vectorized {

class Chunk;
class Field;
class Column;
class Schema;

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

inline ChunkPtr ChunkHelper::new_chunk(const vectorized::Schema& schema, size_t n) {
    size_t fields = schema.num_fields();
    Columns columns;
    columns.reserve(fields);
    for (size_t i = 0; i < fields; i++) {
        const vectorized::FieldPtr& f = schema.field(i);
        columns.emplace_back(column_from_field(*f));
        columns.back()->reserve(n);
    }
    return std::make_shared<Chunk>(std::move(columns), std::make_shared<vectorized::Schema>(schema));
}

inline std::shared_ptr<Chunk> ChunkHelper::new_chunk(const TupleDescriptor& tuple_desc, size_t n) {
    return new_chunk(tuple_desc.slots(), n);
}

inline std::shared_ptr<Chunk> ChunkHelper::new_chunk(const std::vector<SlotDescriptor*>& slots, size_t n) {
    auto chunk = std::make_shared<Chunk>();
    for (const auto slot : slots) {
        ColumnPtr column = ColumnHelper::create_column(slot->type(), slot->is_nullable());
        column->reserve(n);
        chunk->append_column(column, slot->id());
    }
    return chunk;
}

} // namespace vectorized
} // namespace starrocks
