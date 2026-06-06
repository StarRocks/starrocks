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
#include <vector>

#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "common/status.h"

namespace starrocks {

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

// A big-chunk would be segmented into multi small ones, to avoid allocating large-continuous memory.
// It's not a transparent replacement for Chunk, but must be aware of and set a reasonable chunk_size.
class SegmentedChunk final : public std::enable_shared_from_this<SegmentedChunk> {
public:
    explicit SegmentedChunk(size_t segment_size);
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
