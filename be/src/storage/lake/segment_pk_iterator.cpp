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

#include "storage/lake/segment_pk_iterator.h"

#include "base/debug/trace.h"
#include "column/chunk_factory.h"
#include "common/config_primary_key_fwd.h"
#include "runtime/current_thread.h"
#include "storage/primary_key_encoder.h"

namespace starrocks::lake {

Status SegmentPKIterator::_load() {
    TRY_CATCH_BAD_ALLOC(_pk_column_chunk = ChunkFactory::new_chunk(_pkey_schema, 4096));
    auto chunk_container = _pk_column_chunk->clone_empty();
    if (_iter != nullptr) {
        while (true) {
            chunk_container->reset();
            Status st;
            // Capture the segment-wide physical rowid base (= iterator's range_start)
            // from the first non-empty emit only. Subsequent emits use the plain form
            // since the base is already known and contiguity is structurally guaranteed
            // by get_each_segment_iterator (no delvec / predicates / sparse ranges).
            if (!_physical_rowid_base.has_value()) {
                std::vector<uint32_t> rowid_buffer;
                {
                    TRACE_COUNTER_SCOPE_LATENCY_US("segment_get_next_us");
                    st = _iter->get_next(chunk_container.get(), &rowid_buffer);
                }
                if (st.ok() && !rowid_buffer.empty()) {
                    _physical_rowid_base = rowid_buffer.front();
                }
            } else {
                TRACE_COUNTER_SCOPE_LATENCY_US("segment_get_next_us");
                st = _iter->get_next(chunk_container.get());
            }
            if (st.is_end_of_file()) {
                break;
            }
            if (!st.ok()) {
                return st;
            }
            TRY_CATCH_BAD_ALLOC(_pk_column_chunk->append(*chunk_container));
            if (_lazy_load && (_pk_column_chunk->memory_usage() >= config::pk_column_lazy_load_threshold_bytes ||
                               _pk_column_chunk->num_rows() >= config::pk_index_parallel_execution_min_rows)) {
                break;
            }
        }
    }
    if (!_lazy_load && _standalone_pk_column == nullptr) {
        // In some place, like partial update handler, we need to get standalone pk column,
        // so we can't use lazy load mode.
        ASSIGN_OR_RETURN(_standalone_pk_column, encoded_pk_column(_pk_column_chunk.get()));
    }
    if (_pk_column_chunk->num_rows() == 0) {
        return Status::OK();
    }
    _current_rows += _pk_column_chunk->num_rows();
    _begin_rowid_offsets.push_back(_current_rows);
    return Status::OK();
}

Status SegmentPKIterator::init(const ChunkIteratorPtr& iter, const Schema& pkey_schema, bool lazy_load,
                               PrimaryKeyEncodingType encoding_type, bool defer_data_load) {
    _iter = iter;
    _pkey_schema = pkey_schema;
    _lazy_load = lazy_load;
    _defer_data_load = defer_data_load;
    _begin_rowid_offsets.push_back(0);
    RETURN_IF(encoding_type == PrimaryKeyEncodingType::PK_ENCODING_TYPE_NONE,
              Status::InvalidArgument("PK_ENCODING_TYPE_NONE is not a valid encoding type"));
    _encoding_type = encoding_type;
    if (!_defer_data_load) {
        _status = _load();
        if (_status.ok()) {
            _memory_usage = _pk_column_chunk->memory_usage() +
                            (_standalone_pk_column ? _standalone_pk_column->memory_usage() : 0);
        }
    }
    return _status;
}

bool SegmentPKIterator::done() {
    // Deferred first load: trigger on first done() call
    if (_defer_data_load) {
        _defer_data_load = false;
        _status = _load();
        if (_status.ok() && _pk_column_chunk != nullptr) {
            _memory_usage = _pk_column_chunk->memory_usage() +
                            (_standalone_pk_column ? _standalone_pk_column->memory_usage() : 0);
        }
    }
    return (_pk_column_chunk == nullptr || _pk_column_chunk->is_empty()) || !_status.ok();
}

Status SegmentPKIterator::status() {
    return _status;
}

void SegmentPKIterator::next() {
    _status = _load();
    if (_status.ok()) {
        _current_pk_column_idx++;
    }
}

SegmentPKChunkRef SegmentPKIterator::current() {
    SegmentPKChunkRef ref;
    const size_t logical_rowid_offset = _begin_rowid_offsets[_current_pk_column_idx];
    ref.physical_rowid_offset = _physical_rowid_base.value_or(0) + static_cast<uint32_t>(logical_rowid_offset);
    ref.chunk = std::move(_pk_column_chunk);
    return ref;
}

StatusOr<MutableColumnPtr> SegmentPKIterator::encoded_pk_column(const Chunk* chunk) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pk_encode_us");
    MutableColumnPtr pk_column;
    RETURN_IF(_encoding_type == PrimaryKeyEncodingType::PK_ENCODING_TYPE_NONE,
              Status::InvalidArgument("PK_ENCODING_TYPE_NONE is not a valid encoding type"));
    RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(_pkey_schema, &pk_column, _encoding_type));
    TRY_CATCH_BAD_ALLOC(
            PrimaryKeyEncoder::encode(_pkey_schema, *chunk, 0, chunk->num_rows(), pk_column.get(), _encoding_type));
    return std::move(pk_column);
}

void SegmentPKIterator::close() {
    _iter->close();
}

} // namespace starrocks::lake
