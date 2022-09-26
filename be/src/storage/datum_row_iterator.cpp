// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/datum_row_iterator.h"
#include "storage/chunk_helper.h"

namespace starrocks {

Status DatumRowIterator::get_next_chunk() {
    if (_current_chunk == nullptr) {
        _current_chunk = ChunkHelper::new_chunk(_chunk_iterator->schema(), _chunk_size);
        _next_row_index_in_current_chunk = 0;
    }

    if (_next_row_index_in_current_chunk < _current_chunk->num_rows()) {
        return _status_ok;
    }
    _current_chunk->reset();
    auto status = _chunk_iterator->get_next(_current_chunk.get());
    if (status.ok()) {
        _next_row_index_in_current_chunk = 0;
    }
    return status;
}

RowSharedPtr DatumRowIterator::get_next_row() {
    std::shared_ptr<DatumRow> row = std::make_shared<DatumRow>(_current_chunk->num_columns());
    size_t row_index = _next_row_index_in_current_chunk;
    for (int i = 0; i < _current_chunk->num_columns(); i++) {
        row->set_datum(i, _current_chunk->get_column_by_index(i)->get(row_index));
    }
    _next_row_index_in_current_chunk++;
    return row;
}

StatusOr<RowSharedPtr> DatumRowIterator::get_next() {
    auto status = get_next_chunk();
    if (!status.ok()) {
        return status;
    }
    return get_next_row();
}

} // namespace starrocks