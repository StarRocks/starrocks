// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

#include "storage/row_iterator.h"
#include "storage/datum_row.h"
#include "storage/chunk_iterator.h"

namespace starrocks {

class DatumRowIterator: public RowIterator {

public:
    explicit DatumRowIterator(
            std::shared_ptr<vectorized::ChunkIterator> chunk_iterator, size_t chunk_size = 1024)
            : _chunk_iterator(std::move(chunk_iterator)),
              _chunk_size(chunk_size),
              _next_row_index_in_current_chunk(0) {}

    StatusOr<RowPtr> get_next() override;

    void close() override {
        _chunk_iterator->close();
    }

private:

    Status get_next_chunk();
    RowPtr get_next_row();

    const Status _status_ok;
    std::shared_ptr<vectorized::ChunkIterator> _chunk_iterator;
    const size_t _chunk_size;
    std::shared_ptr<vectorized::Chunk> _current_chunk;
    size_t _next_row_index_in_current_chunk;
};

} // namespace starrocks