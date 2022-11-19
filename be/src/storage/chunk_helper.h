// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <queue>

#include "column/vectorized_fwd.h"
#include "storage/field.h"
#include "storage/olap_type_infra.h"

namespace starrocks {

class Status;
class TabletColumn;
class TabletSchema;
class SlotDescriptor;
class TupleDescriptor;

class ChunkHelper {
public:
    static VectorizedField convert_field(ColumnId id, const TabletColumn& c);

    static VectorizedSchema convert_schema(const TabletSchema& schema);

    // Convert TabletColumn to VectorizedField. This function will generate format
    // V2 type: DATE_V2, TIMESTAMP, DECIMAL_V2
    static VectorizedField convert_field_to_format_v2(ColumnId id, const TabletColumn& c);

    // Convert TabletSchema to VectorizedSchema with changing format v1 type to format v2 type.
    static VectorizedSchema convert_schema_to_format_v2(const TabletSchema& schema);

    // Convert TabletSchema to VectorizedSchema with changing format v1 type to format v2 type.
    static VectorizedSchema convert_schema_to_format_v2(const TabletSchema& schema, const std::vector<ColumnId>& cids);

    // Get schema with format v2 type containing short key columns from TabletSchema.
    static VectorizedSchema get_short_key_schema_with_format_v2(const TabletSchema& tablet_schema);

    static ColumnId max_column_id(const VectorizedSchema& schema);

    // Create an empty chunk according to the |schema| and reserve it of size |n|.
    static std::shared_ptr<Chunk> new_chunk(const VectorizedSchema& schema, size_t n);

    // Create an empty chunk according to the |tuple_desc| and reserve it of size |n|.
    static std::shared_ptr<Chunk> new_chunk(const TupleDescriptor& tuple_desc, size_t n);

    // Create an empty chunk according to the |slots| and reserve it of size |n|.
    static std::shared_ptr<Chunk> new_chunk(const std::vector<SlotDescriptor*>& slots, size_t n);

    static Chunk* new_chunk_pooled(const VectorizedSchema& schema, size_t n, bool force = true);

    // Create a vectorized column from field .
    // REQUIRE: |type| must be scalar type.
    static std::shared_ptr<Column> column_from_field_type(LogicalType type, bool nullable);

    // Create a vectorized column from field.
    static std::shared_ptr<Column> column_from_field(const VectorizedField& field);

    // Get char column indexes
    static std::vector<size_t> get_char_field_indexes(const VectorizedSchema& schema);

    // Padding char columns
    static void padding_char_columns(const std::vector<size_t>& char_column_indexes, const VectorizedSchema& schema,
                                     const TabletSchema& tschema, Chunk* chunk);

    // Reorder columns of `chunk` according to the order of |tuple_desc|.
    static void reorder_chunk(const TupleDescriptor& tuple_desc, Chunk* chunk);
    // Reorder columns of `chunk` according to the order of |slots|.
    static void reorder_chunk(const std::vector<SlotDescriptor*>& slots, Chunk* chunk);

    // Convert a filter to select vector
    static void build_selective(const std::vector<uint8_t>& filter, std::vector<uint32_t>& selective);
};

// Accumulate small chunk into desired size
class ChunkAccumulator {
public:
    ChunkAccumulator() = default;
    ChunkAccumulator(size_t desired_size);
    void set_desired_size(size_t desired_size);
    void reset();
    Status push(ChunkPtr&& chunk);
    void finalize();
    bool empty() const;
    ChunkPtr pull();

private:
    size_t _desired_size;
    ChunkPtr _tmp_chunk;
    std::deque<ChunkPtr> _output;
};

class ChunkPipelineAccumulator {
public:
    ChunkPipelineAccumulator() = default;
    void set_max_size(size_t max_size) { _max_size = max_size; }
    void push(const ChunkPtr& chunk);
    ChunkPtr& pull();
    void finalize();
    void reset();

    bool has_output() const;
    bool need_input() const;
    bool is_finished() const;

private:
    static constexpr double LOW_WATERMARK_ROWS_RATE = 0.75;          // 0.75 * chunk_size
    static constexpr size_t LOW_WATERMARK_BYTES = 256 * 1024 * 1024; // 256MB.
    ChunkPtr _in_chunk = nullptr;
    ChunkPtr _out_chunk = nullptr;
    size_t _max_size = 4096;
    bool _finalized = false;
};

} // namespace starrocks
