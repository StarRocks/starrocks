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
#include <queue>

#include "column/vectorized_fwd.h"
#include "storage/olap_common.h"
#include "storage/olap_type_infra.h"

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
    static Schema convert_schema(const TabletSchema& schema);

    // Convert TabletSchema to Schema with changing format v1 type to format v2 type.
    static Schema convert_schema(const TabletSchema& schema, const std::vector<ColumnId>& cids);

    // Get schema with format v2 type containing short key columns from TabletSchema.
    static Schema get_short_key_schema(const TabletSchema& schema);

    // Get schema with format v2 type containing sort key columns from TabletSchema.
    static Schema get_sort_key_schema(const TabletSchema& schema);

    // Get schema with format v2 type containing sort key columns filled by primary key columns from TabletSchema.
    static Schema get_sort_key_schema_by_primary_key(const starrocks::TabletSchema& tablet_schema);

    static ColumnId max_column_id(const Schema& schema);

    // Create an empty chunk according to the |schema| and reserve it of size |n|.
    static ChunkUniquePtr new_chunk(const Schema& schema, size_t n);

    // Create an empty chunk according to the |tuple_desc| and reserve it of size |n|.
    static ChunkUniquePtr new_chunk(const TupleDescriptor& tuple_desc, size_t n);

    // Create an empty chunk according to the |slots| and reserve it of size |n|.
    static ChunkUniquePtr new_chunk(const std::vector<SlotDescriptor*>& slots, size_t n);

    static Chunk* new_chunk_pooled(const Schema& schema, size_t n, bool force = true);

    // Create a vectorized column from field .
    // REQUIRE: |type| must be scalar type.
    static std::shared_ptr<Column> column_from_field_type(LogicalType type, bool nullable);

    // Create a vectorized column from field.
    static std::shared_ptr<Column> column_from_field(const Field& field);

    // Get char column indexes
    static std::vector<size_t> get_char_field_indexes(const Schema& schema);

    // Padding char columns
    static void padding_char_columns(const std::vector<size_t>& char_column_indexes, const Schema& schema,
                                     const TabletSchema& tschema, Chunk* chunk);

    // Reorder columns of `chunk` according to the order of |tuple_desc|.
    static void reorder_chunk(const TupleDescriptor& tuple_desc, Chunk* chunk);
    // Reorder columns of `chunk` according to the order of |slots|.
    static void reorder_chunk(const std::vector<SlotDescriptor*>& slots, Chunk* chunk);
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
    static constexpr double LOW_WATERMARK_ROWS_RATE = 0.75;          // 0.75 * chunk_size
    static constexpr size_t LOW_WATERMARK_BYTES = 256 * 1024 * 1024; // 256MB.
    ChunkPtr _in_chunk = nullptr;
    ChunkPtr _out_chunk = nullptr;
    size_t _max_size = 4096;
    bool _finalized = false;
};

} // namespace starrocks
