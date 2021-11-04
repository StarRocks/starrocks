// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/aggregate_iterator.h"

#include <memory>

#include "column/chunk.h"
#include "column/nullable_column.h"
#include "common/config.h"
#include "gutil/casts.h"
#include "storage/vectorized/chunk_aggregator.h"
#include "storage/vectorized/chunk_helper.h"
#include "util/defer_op.h"

namespace starrocks::vectorized {

/**
 * Pre-Aggregate Iterator
 * Try to do aggregate in adjacent rows if the keys equal, will reduce the 
 * output rows
 */
class AggregateIterator final : public ChunkIterator {
public:
    explicit AggregateIterator(ChunkIteratorPtr child, int factor)
            : ChunkIterator(child->schema(), child->chunk_size()),
              _child(std::move(child)),
              _pre_aggregate_factor(factor),
              _aggregator(&_schema, _chunk_size, _pre_aggregate_factor / 100),
              _fetch_finish(false) {
        CHECK_LT(_schema.num_key_fields(), std::numeric_limits<uint16_t>::max());

        _curr_chunk = ChunkHelper::new_chunk(_schema, _chunk_size);
#ifndef NDEBUG
        // ensure that the key fields are the first |num_key_fields| and sorted by id.
        for (size_t i = 0; i < _schema.num_key_fields(); i++) {
            CHECK(_schema.field(i)->is_key());
        }
        for (size_t i = 0; i + 1 < _schema.num_key_fields(); i++) {
            CHECK_LT(_schema.field(i)->id(), _schema.field(i + 1)->id());
        }
#endif
    }

    ~AggregateIterator() override {
        VLOG_ROW << "RESULT CHUNK: " << _result_chunk << ", CHUNK NUMS: " << _chunk_nums
                 << ", AGG CHUNKS: " << _agg_chunk_nums << ", FACTOR: " << (int)_pre_aggregate_factor;
    }

    void close() override;

    size_t merged_rows() const override { return _aggregator.merged_rows(); }

    virtual Status init_encoded_schema(ColumnIdToGlobalDictMap& dict_maps) override {
        ChunkIterator::init_encoded_schema(dict_maps);
        return _child->init_encoded_schema(dict_maps);
    }

protected:
    Status do_get_next(Chunk* chunk) override;

private:
    int _agg_chunk_nums = 0;
    int _chunk_nums = 0;
    int _result_chunk = 0;

private:
    ChunkIteratorPtr _child;

    ChunkPtr _curr_chunk;

    double _pre_aggregate_factor;

    ChunkAggregator _aggregator;

    bool _fetch_finish;
};

Status AggregateIterator::do_get_next(Chunk* chunk) {
    while (!_fetch_finish) {
        // fetch chunk
        if (_aggregator.source_exhausted()) {
            _curr_chunk->reset();

            Status st = _child->get_next(_curr_chunk.get());

            if (st.is_end_of_file()) {
                _fetch_finish = true;
                break;
            } else if (!st.ok()) {
                return st;
            }

            DCHECK(_curr_chunk->num_rows() != 0);

            _chunk_nums++;
            _aggregator.update_source(_curr_chunk);

            if (!_aggregator.is_do_aggregate()) {
                chunk->swap_chunk(*_curr_chunk);
                _result_chunk++;
                return Status::OK();
            } else {
                _agg_chunk_nums++;
            }
        }

        // try aggregate
        _aggregator.aggregate();

        // if finish, return
        if (!_aggregator.source_exhausted() && _aggregator.is_finish()) {
            chunk->swap_chunk(*_aggregator.aggregate_result());
            _aggregator.aggregate_reset();
            _result_chunk++;

            return Status::OK();
        }
    }

    if (_aggregator.has_aggregate_data()) {
        _aggregator.aggregate();
        chunk->swap_chunk(*_aggregator.aggregate_result());
        _result_chunk++;

        return Status::OK();
    }

    return Status::EndOfFile("eof");
}

void AggregateIterator::close() {
    _curr_chunk.reset();
    _child->close();
    _aggregator.close();
}

ChunkIteratorPtr new_aggregate_iterator(ChunkIteratorPtr child, int factor) {
    return std::make_shared<AggregateIterator>(std::move(child), factor);
}

} // namespace starrocks::vectorized
