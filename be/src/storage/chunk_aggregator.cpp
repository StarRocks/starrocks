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

#include "storage/chunk_aggregator.h"

#include "common/config.h"
#include "exec/sorting/sorting.h"
#include "gutil/casts.h"
#include "storage/column_aggregate_func.h"

namespace starrocks {

ChunkAggregator::ChunkAggregator(const starrocks::Schema* schema, uint32_t reserve_rows, uint32_t max_aggregate_rows,
                                 double factor, bool is_vertical_merge, bool is_key)
        : _schema(schema),
          _reserve_rows(reserve_rows),
          _max_aggregate_rows(max_aggregate_rows),
          _factor(factor),
          _has_aggregate(false),
          _is_vertical_merge(is_vertical_merge),
          _is_key(is_key) {
#ifndef NDEBUG
    // ensure that the key fields are sorted by id and placed before others.
    for (size_t i = 0; i < _schema->num_key_fields(); i++) {
        CHECK(_schema->field(i)->is_key());
    }
#endif
    _key_fields = _schema->num_key_fields();
    _num_fields = _schema->num_fields();

    _source_row = 0;
    _source_size = 0;
    _do_aggregate = true;

    _is_eq.reserve(_reserve_rows);
    _selective_index.reserve(_reserve_rows);
    _aggregate_loops.reserve(_reserve_rows);

    _column_aggregator.reserve(_num_fields);

    // column aggregator
    for (int i = 0; i < _key_fields; ++i) {
        _column_aggregator.emplace_back(ColumnAggregatorFactory::create_key_column_aggregator(_schema->field(i)));
    }
    for (int i = _key_fields; i < _num_fields; ++i) {
        _column_aggregator.emplace_back(ColumnAggregatorFactory::create_value_column_aggregator(_schema->field(i)));
    }

    aggregate_reset();
}

ChunkAggregator::ChunkAggregator(const Schema* schema, uint32_t max_aggregate_rows, double factor)
        : ChunkAggregator(schema, max_aggregate_rows, max_aggregate_rows, factor, false, false) {}

ChunkAggregator::ChunkAggregator(const Schema* schema, uint32_t reserve_rows, uint32_t max_aggregate_rows,
                                 double factor)
        : ChunkAggregator(schema, reserve_rows, max_aggregate_rows, factor, false, false) {}

ChunkAggregator::ChunkAggregator(const Schema* schema, uint32_t max_aggregate_rows, double factor,
                                 bool is_vertical_merge, bool is_key)
        : ChunkAggregator(schema, max_aggregate_rows, max_aggregate_rows, factor, is_vertical_merge, is_key) {}

bool ChunkAggregator::_row_equal(const Chunk* lhs, size_t m, const Chunk* rhs, size_t n) const {
    for (uint16_t i = 0; i < _key_fields; i++) {
        if (lhs->get_column_by_index(i)->compare_at(m, n, *rhs->get_column_by_index(i), -1) != 0) {
            return false;
        }
    }
    return true;
}

void ChunkAggregator::update_source(ChunkPtr& chunk, std::vector<RowSourceMask>* source_masks) {
    size_t is_eq_size = chunk->num_rows();
    _is_eq.assign(is_eq_size, 1);
    _source_row = 0;
    _source_size = 0;
    _do_aggregate = true;

    if (_is_vertical_merge && !_is_key) {
        // update _is_eq from source masks
        DCHECK(source_masks);
        size_t masks_size = source_masks->size();
        DCHECK_GE(masks_size, is_eq_size);
        size_t start_offset = masks_size - is_eq_size;
        for (int i = 0; i < is_eq_size; ++i) {
            _is_eq[i] = (*source_masks)[start_offset + i].get_agg_flag() ? 1 : 0;
        }
    } else {
        // update _is_eq by key comparison
        size_t factor = chunk->num_rows() * _factor;
        for (int i = _key_fields - 1; i >= 0; --i) {
            ColumnPtr column = chunk->get_column_by_index(i);
            build_tie_for_column(column, &_is_eq);

            if (_factor > 0 && SIMD::count_nonzero(_is_eq) < factor) {
                _do_aggregate = false;
                return;
            }
        }

        if (_aggregate_rows > 0 && _key_fields > 0) {
            _is_eq[0] = _row_equal(_aggregate_chunk.get(), _aggregate_rows - 1, chunk.get(), 0);
        } else {
            _is_eq[0] = 0;
        }
    }

    _merged_rows += SIMD::count_nonzero(_is_eq);

    // update source column
    for (int j = 0; j < _num_fields; ++j) {
        _column_aggregator[j]->update_source(chunk->get_column_by_index(j));
    }
    _source_size = chunk->num_rows();

    // update source masks from _is_eq
    if (_is_vertical_merge && _is_key) {
        DCHECK(source_masks);
        size_t masks_size = source_masks->size();
        DCHECK_GE(masks_size, is_eq_size);
        size_t start_offset = masks_size - is_eq_size;
        for (int i = 0; i < is_eq_size; ++i) {
            (*source_masks)[start_offset + i].set_agg_flag(_is_eq[i] != 0);
        }
    }
}

void ChunkAggregator::aggregate() {
    if (source_exhausted()) {
        return;
    }

    DCHECK(_source_row < _source_size) << "It's impossible";

    _selective_index.clear();
    _aggregate_loops.clear();

    // first key is not equal with last row in previous chunk
    bool previous_neq = !_is_eq[_source_row] && (_aggregate_rows > 0);

    // same with last row
    if (_is_eq[_source_row] == 1) {
        _aggregate_loops.emplace_back(0);
    }

    // 1. Calculate the key rows selective arrays
    // 2. Calculate the value rows that can be aggregated for each key row
    uint32_t row = _source_row;
    for (; row < _source_size; ++row) {
        if (_is_eq[row] == 0) {
            if (_aggregate_rows >= _max_aggregate_rows) {
                break;
            }
            ++_aggregate_rows;
            _selective_index.emplace_back(row);
            _aggregate_loops.emplace_back(1);
        } else {
            _aggregate_loops[_aggregate_loops.size() - 1] += 1;
        }
    }

    // 3. Copy the selected key rows
    // 4. Aggregate the value rows
    for (int i = 0; i < _key_fields; ++i) {
        _column_aggregator[i]->aggregate_keys(_source_row, _selective_index.size(), _selective_index.data());
    }

    for (int i = _key_fields; i < _num_fields; ++i) {
        _column_aggregator[i]->aggregate_values(_source_row, _aggregate_loops.size(), _aggregate_loops.data(),
                                                previous_neq);
    }

    _source_row = row;
    _has_aggregate = true;
}

bool ChunkAggregator::is_finish() {
    return (_aggregate_chunk == nullptr || _aggregate_rows >= _max_aggregate_rows);
}

void ChunkAggregator::aggregate_reset() {
    _aggregate_chunk = ChunkHelper::new_chunk(*_schema, _reserve_rows);
    _aggregate_rows = 0;

    for (int i = 0; i < _num_fields; ++i) {
        auto p = _aggregate_chunk->get_column_by_index(i).get();
        _column_aggregator[i]->update_aggregate(p);
    }
    _has_aggregate = false;

    _reference_memory_usage = 0;
    _reference_memory_usage_num_rows = 0;
    _bytes_usage = 0;
    _bytes_usage_num_rows = 0;
}

ChunkPtr ChunkAggregator::aggregate_result() {
    for (int i = 0; i < _num_fields; ++i) {
        _column_aggregator[i]->finalize();
    }
    _has_aggregate = false;
    return _aggregate_chunk;
}

size_t ChunkAggregator::memory_usage() {
    if (_aggregate_chunk == nullptr) {
        return 0;
    }

    size_t container_memory_usage = _aggregate_chunk->container_memory_usage();

    size_t num_rows = _aggregate_chunk->num_rows();
    // the last row of non-key column is in aggregator (not in aggregate chunk) before finalize,
    // the size of key columns is 1 greater that value columns,
    // so we use num_rows - 1 as chunk num rows.
    if (num_rows <= 1) {
        return container_memory_usage;
    }
    --num_rows;

    if (_reference_memory_usage_num_rows == num_rows) {
        return container_memory_usage + _reference_memory_usage;
    } else if (_reference_memory_usage_num_rows > num_rows) {
        _reference_memory_usage_num_rows = 0;
        _reference_memory_usage = 0;
    }
    _reference_memory_usage += _aggregate_chunk->reference_memory_usage(_reference_memory_usage_num_rows,
                                                                        num_rows - _reference_memory_usage_num_rows);
    _reference_memory_usage_num_rows = num_rows;
    return container_memory_usage + _reference_memory_usage;
}

size_t ChunkAggregator::bytes_usage() {
    if (_aggregate_chunk == nullptr) {
        return 0;
    }

    size_t num_rows = _aggregate_chunk->num_rows();
    // the last row of non-key column is in aggregator (not in aggregate chunk) before finalize,
    // the size of key columns is 1 greater that value columns,
    // so we use num_rows - 1 as chunk num rows.
    if (num_rows <= 1) {
        return 0;
    }
    --num_rows;

    if (_bytes_usage_num_rows == num_rows) {
        return _bytes_usage;
    } else if (_bytes_usage_num_rows > num_rows) {
        _bytes_usage_num_rows = 0;
        _bytes_usage = 0;
    }
    _bytes_usage += _aggregate_chunk->bytes_usage(_bytes_usage_num_rows, num_rows - _bytes_usage_num_rows);
    _bytes_usage_num_rows = num_rows;
    return _bytes_usage;
}

void ChunkAggregator::close() {}

} // namespace starrocks
