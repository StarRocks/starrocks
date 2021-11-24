// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>
#include <vector>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/chunk_iterator.h"
#include "storage/vectorized/column_aggregate_func.h"

namespace starrocks::vectorized {

using CompareFN = void (*)(const Column* col, uint8_t* flags);

class ChunkAggregator {
public:
    ChunkAggregator(const Schema* schema, uint32_t reserve_rows, uint32_t aggregate_rows, double factor);

    ChunkAggregator(const Schema* schema, uint32_t aggregate_rows, double factor);

    void update_source(ChunkPtr& chunk);

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
    CompareFN _choose_comparator(const FieldPtr& field);

    bool _row_equal(const Chunk* lhs, size_t m, const Chunk* rhs, size_t n) const;

    // chunk mate
    const Schema* _schema;

    uint16_t _key_fields;

    uint16_t _num_fields;

    uint32_t _reserve_rows;

    uint32_t _aggregate_rows;

    std::vector<CompareFN> _comparator;

    uint32_t _source_row;

    uint32_t _source_size;

    // aggregate chunk info
    std::vector<uint8_t> _is_eq;

    std::vector<uint32_t> _selective_index;

    // use for calculate the aggregate range covered by each aggregate key
    std::vector<uint32_t> _aggregate_loops;

    ChunkPtr _aggregate_chunk;

    // aggregate factor
    double _factor;

    bool _do_aggregate;

    // column aggregator
    std::vector<ColumnAggregatorPtr> _column_aggregator;

    // status
    bool _has_aggregate;

    size_t _merged_rows = 0;

    // element memory usage and bytes usage calculation cost of object column is high,
    // so cache calculated element memory usage and bytes usage to avoid repeated calculation.
    size_t _element_memory_usage = 0;
    size_t _element_memory_usage_num_rows = 0;
    size_t _bytes_usage = 0;
    size_t _bytes_usage_num_rows = 0;
};

} // namespace starrocks::vectorized
