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

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/column_aggregate_func.h"
#include "storage/row_source_mask.h"

namespace starrocks {

class ChunkAggregator {
private:
    ChunkAggregator(const Schema* schema, uint32_t reserve_rows, uint32_t max_aggregate_rows, double factor,
                    bool is_vertical_merge, bool is_key);

public:
    ChunkAggregator(const Schema* schema, uint32_t reserve_rows, uint32_t max_aggregate_rows, double factor);

    ChunkAggregator(const Schema* schema, uint32_t max_aggregate_rows, double factor);

    ChunkAggregator(const Schema* schema, uint32_t max_aggregate_rows, double factor, bool is_vertical_merge,
                    bool is_key);

    void update_source(ChunkPtr& chunk) { update_source(chunk, nullptr); }
    // |source_masks| is used if |_is_vertical_merge| is true.
    // row source mask sequence will be updated from _is_eq if _is_key is true
    // or used to update _is_eq if _is_key is false.
    void update_source(ChunkPtr& chunk, std::vector<RowSourceMask>* source_masks);

    void aggregate();

    bool is_do_aggregate() const { return _do_aggregate; }

    bool source_exhausted() const { return _source_row == _source_size; }

    bool has_aggregate_data() const { return _has_aggregate; }

    bool is_finish();

    void aggregate_reset();

    ChunkPtr aggregate_result();

    size_t memory_usage();

    size_t merged_rows() const { return _merged_rows; }

    size_t bytes_usage();

    void close();

private:
    bool _row_equal(const Chunk* lhs, size_t m, const Chunk* rhs, size_t n) const;

    // chunk mate
    const Schema* _schema;

    size_t _key_fields;

    size_t _num_fields;

    uint32_t _reserve_rows;

    uint32_t _max_aggregate_rows;

    uint32_t _source_row;

    size_t _source_size;

    // aggregate chunk info
    std::vector<uint8_t> _is_eq;

    std::vector<uint32_t> _selective_index;

    // use for calculate the aggregate range covered by each aggregate key
    std::vector<uint32_t> _aggregate_loops;

    ChunkPtr _aggregate_chunk;
    // the last row of non-key column is in aggregator (not in aggregate chunk) before finalize.
    // in vertical compaction, there maybe only non-key column in aggregate chunk,
    // so we cannot use _aggregate_chunk->num_rows() as aggregate rows.
    uint32_t _aggregate_rows = 0;

    // aggregate factor
    double _factor;

    bool _do_aggregate;

    std::vector<ColumnAggregatorPtr> _column_aggregator;

    // status
    bool _has_aggregate;

    size_t _merged_rows = 0;

    // element memory usage and bytes usage calculation cost of object column is high,
    // so cache calculated element memory usage and bytes usage to avoid repeated calculation.
    size_t _reference_memory_usage = 0;
    size_t _reference_memory_usage_num_rows = 0;
    size_t _bytes_usage = 0;
    size_t _bytes_usage_num_rows = 0;

    // used for vertical compaction
    bool _is_vertical_merge = false;
    bool _is_key = false;
};

} // namespace starrocks
