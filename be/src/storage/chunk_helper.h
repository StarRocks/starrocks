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

// NOTE: This file is included by 200+ files. Be cautious when adding more includes to avoid unnecessary recompilation or increased build dependencies.
#include <memory>

#include "column/chunk.h"
#include "column/segmented_chunk.h"
#include "column/vectorized_fwd.h"
#include "storage/olap_common.h"
#include "tablet_schema.h"

namespace starrocks {

class Status;
class TabletSchema;

class ChunkHelper {
public:
    // Convert TabletSchema to Schema with changing format v1 type to format v2 type.
    static Schema convert_schema(const TabletSchemaCSPtr& schema);

    // Convert TabletSchema to Schema with changing format v1 type to format v2 type.
    static Schema convert_schema(const TabletSchemaCSPtr& schema, const std::vector<ColumnId>& cids);

    // Get schema with format v2 type containing short key columns from TabletSchema.
    static Schema get_short_key_schema(const TabletSchemaCSPtr& schema);

    // Get schema with format v2 type containing sort key columns from TabletSchema.
    static Schema get_sort_key_schema(const TabletSchemaCSPtr& schema);

    // Get schema with format v2 type containing sort key columns filled by primary key columns from TabletSchema.
    static Schema get_sort_key_schema_by_primary_key(const starrocks::TabletSchemaCSPtr& tablet_schema);

    // Padding char columns
    static void padding_char_columns(const std::vector<size_t>& char_column_indexes, const Schema& schema,
                                     const TabletSchemaCSPtr& tschema, Chunk* chunk);

    // Padding one char column
    static void padding_char_column(const starrocks::TabletSchemaCSPtr& tschema, const Field& field, Column* column);
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

class ExprContext;
/**
 * RAII guard for evaluating common expressions on a chunk.
 * 
 * This class provides automatic scope management for evaluating common expressions
 * that are temporarily used during expression computation. Common expressions are
 * computed once and reused across multiple expressions to avoid redundant computation,
 * but they are only needed during the computation phase and should be cleaned up
 * from the chunk after computation completes.
 * 
 * The destructor automatically removes the common expressions from the chunk
 * to prevent memory leaks and ensure proper cleanup.
 */
class CommonExprEvalScopeGuard {
public:
    CommonExprEvalScopeGuard(const ChunkPtr& chunk, const std::map<SlotId, ExprContext*>& common_expr_ctxs);
    ~CommonExprEvalScopeGuard();

    Status evaluate();

private:
    const ChunkPtr& _chunk;
    const std::map<SlotId, ExprContext*>& _common_expr_ctxs;
};

} // namespace starrocks
