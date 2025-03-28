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
#include <mutex>
#include <queue>

#include "column/column_visitor.h"
#include "column/column_visitor_adapter.h"
#include "column/datum.h"
#include "column/fixed_length_column_base.h"
#include "column/vectorized_fwd.h"
#include "storage/olap_common.h"
#include "storage/olap_type_infra.h"
#include "tablet_schema.h"

namespace starrocks {

class Status;
class TabletColumn;
class TabletSchema;
class SlotDescriptor;
class TupleDescriptor;

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
    static MutableColumnPtr column_from_field_type(LogicalType type, bool nullable);

    // Create a vectorized column from field.
    static MutableColumnPtr column_from_field(const Field& field);

    // Get char column indexes
    static std::vector<size_t> get_char_field_indexes(const Schema& schema);

    // Padding char columns
    static void padding_char_columns(const std::vector<size_t>& char_column_indexes, const Schema& schema,
                                     const TabletSchemaCSPtr& tschema, Chunk* chunk);

    // Padding one char column
    static void padding_char_column(const starrocks::TabletSchemaCSPtr& tschema, const Field& field, Column* column);

    // Reorder columns of `chunk` according to the order of |tuple_desc|.
    static void reorder_chunk(const TupleDescriptor& tuple_desc, Chunk* chunk);
    // Reorder columns of `chunk` according to the order of |slots|.
    static void reorder_chunk(const std::vector<SlotDescriptor*>& slots, Chunk* chunk);

    static ChunkPtr createDummyChunk();
};

// Accumulate small chunk into desired size
class ChunkAccumulator {
public:
    // Avoid accumulate too many chunks in case that chunks' selectivity is very low
    static inline size_t kAccumulateLimit = 64;

    ChunkAccumulator() = default;
    ChunkAccumulator(size_t desired_size);
    void set_desired_size(size_t desired_size);
    void reset();
    void finalize();
    bool empty() const;
    bool reach_limit() const;
    Status push(ChunkPtr&& chunk);
    ChunkPtr pull();

private:
    size_t _desired_size;
    ChunkPtr _tmp_chunk;
    std::deque<ChunkPtr> _output;
    size_t _accumulate_count = 0;
};

class ChunkPipelineAccumulator {
public:
    ChunkPipelineAccumulator() = default;
    void set_max_size(size_t max_size) { _max_size = max_size; }
    void push(const ChunkPtr& chunk);
    ChunkPtr& pull();
    void finalize();
    void reset();
    void reset_state();

    bool has_output() const;
    bool need_input() const;
    bool is_finished() const;

private:
    static bool _check_json_schema_equallity(const Chunk* one, const Chunk* two);

private:
    static constexpr double LOW_WATERMARK_ROWS_RATE = 0.75; // 0.75 * chunk_size
#ifdef BE_TEST
    static constexpr size_t LOW_WATERMARK_BYTES = 64 * 1024; // 64KB.
#else
    static constexpr size_t LOW_WATERMARK_BYTES = 256 * 1024 * 1024; // 256MB.
#endif
    ChunkPtr _in_chunk = nullptr;
    ChunkPtr _out_chunk = nullptr;
    size_t _max_size = 4096;
    // For bitmap columns, the cost of calculating mem_usage is relatively high,
    // so incremental calculation is used to avoid becoming a performance bottleneck.
    size_t _mem_usage = 0;
    bool _finalized = false;
};

class SegmentedColumn final : public std::enable_shared_from_this<SegmentedColumn> {
public:
    SegmentedColumn(const SegmentedChunkPtr& chunk, size_t column_index);
    SegmentedColumn(Columns columns, size_t segment_size);
    ~SegmentedColumn() = default;

    ColumnPtr clone_selective(const uint32_t* indexes, uint32_t from, uint32_t size);
    ColumnPtr materialize() const;

    bool is_nullable() const;
    bool has_null() const;
    size_t size() const;
    void upgrade_to_nullable();
    size_t segment_size() const;
    size_t num_segments() const;
    Columns columns() const;

private:
    SegmentedChunkWeakPtr _chunk; // The chunk it belongs to
    size_t _column_index;         // The index in original chunk
    const size_t _segment_size;

    Columns _cached_columns; // Only used for SelectiveCopy
};

// A big-chunk would be segmented into multi small ones, to avoid allocating large-continuous memory
// It's not a transparent replacement for Chunk, but must be aware of and set a reasonale chunk_size
class SegmentedChunk final : public std::enable_shared_from_this<SegmentedChunk> {
public:
    SegmentedChunk(size_t segment_size);
    ~SegmentedChunk() = default;

    static SegmentedChunkPtr create(size_t segment_size);

    void append_column(ColumnPtr column, SlotId slot_id);
    void append_chunk(const ChunkPtr& chunk, const std::vector<SlotId>& slots);
    void append_chunk(const ChunkPtr& chunk);
    void append(const SegmentedChunkPtr& chunk, size_t offset);
    void build_columns();

    SegmentedColumnPtr get_column_by_slot_id(SlotId slot_id);
    const SegmentedColumns& columns() const;
    SegmentedColumns& columns();
    size_t num_segments() const;
    const std::vector<ChunkPtr>& segments() const;
    std::vector<ChunkPtr>& segments();
    ChunkUniquePtr clone_empty(size_t reserve);

    size_t segment_size() const;
    void reset();
    size_t memory_usage() const;
    size_t num_rows() const;
    Status upgrade_if_overflow();
    Status downgrade();
    bool has_large_column() const;
    void check_or_die();

private:
    std::vector<ChunkPtr> _segments;
    SegmentedColumns _columns;

    const size_t _segment_size;
};

} // namespace starrocks
