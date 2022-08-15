// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "segment_chunk_iterator_adapter.h"

#include "storage/chunk_helper.h"

namespace starrocks::vectorized {

SegmentChunkIteratorAdapter::SegmentChunkIteratorAdapter(const TabletSchema& tablet_schema,
                                                         const std::vector<FieldType>& new_types,
                                                         const Schema& out_schema, int chunk_size)
        : ChunkIterator(out_schema, chunk_size),
          _tablet_schema(tablet_schema),
          _new_types(new_types),
          _convert_time(0),
          _convert_timer(nullptr) {}

Status SegmentChunkIteratorAdapter::prepare(const SegmentReadOptions& options) {
    _schema.convert_to(&_in_schema, _new_types);
    RETURN_IF_ERROR(options.convert_to(&_in_read_options, _new_types, &_obj_pool));
    RETURN_IF_ERROR(_converter.init(_in_schema, _schema));
    if (options.profile != nullptr) {
        _convert_timer = ADD_TIMER(options.profile, "ConvertV2Time");
    }
    return Status::OK();
}

Status SegmentChunkIteratorAdapter::do_get_next(Chunk* out_chunk) {
    if (_in_chunk == nullptr) {
        _in_chunk = ChunkHelper::new_chunk(_inner_iter->schema(), _chunk_size);
    }
    DCHECK_EQ(out_chunk->num_columns(), _in_chunk->num_columns());

    RETURN_IF_ERROR(_inner_iter->get_next(_in_chunk.get()));

    SCOPED_RAW_TIMER(&_convert_time);

    auto tmp = _converter.move_convert(_in_chunk.get());
    out_chunk->swap_chunk(*tmp);
    _in_chunk->reset();

    return Status::OK();
}

Status SegmentChunkIteratorAdapter::do_get_next(Chunk* out_chunk, vector<uint32_t>* rowid) {
    if (_in_chunk == nullptr) {
        auto reserve_size = config::vector_chunk_size;
        _in_chunk = ChunkHelper::new_chunk(_inner_iter->schema(), reserve_size);
    }
    DCHECK_EQ(out_chunk->num_columns(), _in_chunk->num_columns());

    RETURN_IF_ERROR(_inner_iter->get_next(_in_chunk.get(), rowid));

    SCOPED_RAW_TIMER(&_convert_time);

    auto tmp = _converter.move_convert(_in_chunk.get());
    out_chunk->swap_chunk(*tmp);
    _in_chunk->reset();

    return Status::OK();
}

void SegmentChunkIteratorAdapter::close() {
    if (_convert_timer != nullptr) {
        COUNTER_UPDATE(_convert_timer, _convert_time);
    }
    if (_inner_iter != nullptr) {
        _inner_iter->close();
        _inner_iter.reset();
    }

    if (_in_chunk != nullptr) {
        _in_chunk.reset();
    }
}

} // namespace starrocks::vectorized
