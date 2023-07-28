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

#include "storage/aggregate_iterator.h"

#include <memory>

#include "column/chunk.h"
#include "column/nullable_column.h"
#include "gutil/casts.h"
#include "storage/chunk_aggregator.h"
#include "storage/chunk_helper.h"
#include "util/defer_op.h"

namespace starrocks {

/**
 * Pre-Aggregate Iterator
 * Try to do aggregate in adjacent rows if the keys equal, will reduce the 
 * output rows
 */
class AggregateIterator final : public ChunkIterator {
public:
    explicit AggregateIterator(ChunkIteratorPtr child, int factor, bool is_vertical_merge, bool is_key)
            : ChunkIterator(child->schema(), child->chunk_size()),
              _child(std::move(child)),
              _pre_aggregate_factor(factor),

              _is_vertical_merge(is_vertical_merge),
              _is_key(is_key) {
        CHECK_LT(_schema.num_key_fields(), std::numeric_limits<uint16_t>::max());

#ifndef NDEBUG
        // ensure that the key fields are the first |num_key_fields| and sorted by id.
        for (size_t i = 0; i < _schema.num_key_fields(); i++) {
            CHECK(_schema.field(i)->is_key());
        }
#endif
    }

    ~AggregateIterator() override {
        VLOG_ROW << "RESULT CHUNK: " << _result_chunk << ", CHUNK NUMS: " << _chunk_nums
                 << ", AGG CHUNKS: " << _agg_chunk_nums << ", FACTOR: " << (int)_pre_aggregate_factor;
    }

    void close() override;

    size_t merged_rows() const override { return _aggregator->merged_rows(); }

    Status init_encoded_schema(ColumnIdToGlobalDictMap& dict_maps) override {
        RETURN_IF_ERROR(ChunkIterator::init_encoded_schema(dict_maps));
        RETURN_IF_ERROR(_child->init_encoded_schema(dict_maps));
        _curr_chunk = ChunkHelper::new_chunk(encoded_schema(), _chunk_size);
        _aggregator = std::make_unique<ChunkAggregator>(&encoded_schema(), _chunk_size, _pre_aggregate_factor / 100,
                                                        _is_vertical_merge, _is_key);
        return Status::OK();
    }

    Status init_output_schema(const std::unordered_set<uint32_t>& unused_output_column_ids) override {
        ChunkIterator::init_output_schema(unused_output_column_ids);
        RETURN_IF_ERROR(_child->init_output_schema(unused_output_column_ids));
        _curr_chunk = ChunkHelper::new_chunk(output_schema(), _chunk_size);
        _aggregator = std::make_unique<ChunkAggregator>(&output_schema(), _chunk_size, _pre_aggregate_factor / 100,
                                                        _is_vertical_merge, _is_key);
        return Status::OK();
    }

protected:
    Status do_get_next(Chunk* chunk) override { return do_get_next(chunk, nullptr); }
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override;

private:
    int _agg_chunk_nums = 0;
    int _chunk_nums = 0;
    int _result_chunk = 0;

private:
    ChunkIteratorPtr _child;

    ChunkPtr _curr_chunk;

    double _pre_aggregate_factor;

    std::unique_ptr<ChunkAggregator> _aggregator;

    bool _fetch_finish{false};

    bool _is_vertical_merge;
    bool _is_key;
};

Status AggregateIterator::do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) {
    while (!_fetch_finish) {
        // fetch chunk
        if (_aggregator->source_exhausted()) {
            _curr_chunk->reset();

            Status st;
            if (source_masks) {
                st = _child->get_next(_curr_chunk.get(), source_masks);
            } else {
                st = _child->get_next(_curr_chunk.get());
            }

            if (st.is_end_of_file()) {
                _fetch_finish = true;
                break;
            } else if (!st.ok()) {
                return st;
            }

            DCHECK(_curr_chunk->num_rows() != 0);

            _chunk_nums++;
            _aggregator->update_source(_curr_chunk, source_masks);

            if (!_aggregator->is_do_aggregate()) {
                chunk->swap_chunk(*_curr_chunk);
                _result_chunk++;
                return Status::OK();
            } else {
                _agg_chunk_nums++;
            }
        }

        // try aggregate
        _aggregator->aggregate();

        // if finish, return
        if (!_aggregator->source_exhausted() && _aggregator->is_finish()) {
            chunk->swap_chunk(*_aggregator->aggregate_result());
            _aggregator->aggregate_reset();
            _result_chunk++;

            return Status::OK();
        }
    }

    if (_aggregator->has_aggregate_data()) {
        _aggregator->aggregate();
        chunk->swap_chunk(*_aggregator->aggregate_result());
        _result_chunk++;

        return Status::OK();
    }

    return Status::EndOfFile("eof");
}

void AggregateIterator::close() {
    _curr_chunk.reset();
    _child->close();
    _aggregator->close();
}

ChunkIteratorPtr new_aggregate_iterator(ChunkIteratorPtr child, int factor) {
    return std::make_shared<AggregateIterator>(std::move(child), factor, false, false);
}

ChunkIteratorPtr new_aggregate_iterator(ChunkIteratorPtr child, bool is_key) {
    return std::make_shared<AggregateIterator>(std::move(child), 0, true, is_key);
}

} // namespace starrocks
