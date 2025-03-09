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

#include "exec/except_hash_set.h"

#include "exec/aggregate/agg_hash_set.h"
#include "exec/exec_node.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

template <typename HashSet>
Status ExceptHashSet<HashSet>::BufferState::init(RuntimeState* state) {
    buffer = mem_pool.allocate(max_one_row_size * state->chunk_size());
    RETURN_IF_UNLIKELY_NULL(buffer, Status::MemoryAllocFailed("alloc mem of except hash set failed"));
    slice_sizes.reserve(state->chunk_size());
    return Status::OK();
}

template <typename HashSet>
void ExceptHashSet<HashSet>::build_set(RuntimeState* state, const ChunkPtr& chunk,
                                       const std::vector<ExprContext*>& exprs, MemPool* pool,
                                       BufferState* buffer_state) {
    size_t chunk_size = chunk->num_rows();
    buffer_state->slice_sizes.assign(state->chunk_size(), 0);

    size_t cur_max_one_row_size = _get_max_serialize_size(chunk, exprs);
    if (UNLIKELY(cur_max_one_row_size > buffer_state->max_one_row_size)) {
        buffer_state->max_one_row_size = cur_max_one_row_size;
        buffer_state->mem_pool.clear();
        buffer_state->buffer = buffer_state->mem_pool.allocate(buffer_state->max_one_row_size * state->chunk_size() +
                                                               SLICE_MEMEQUAL_OVERFLOW_PADDING);
    }

    _serialize_columns(chunk, exprs, chunk_size, buffer_state);

    for (size_t i = 0; i < chunk_size; ++i) {
        ExceptSliceFlag key(buffer_state->buffer + i * buffer_state->max_one_row_size, buffer_state->slice_sizes[i]);
        _hash_set->lazy_emplace(key, [&](const auto& ctor) {
            uint8_t* pos = pool->allocate_with_reserve(key.slice.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
            memcpy(pos, key.slice.data, key.slice.size);
            ctor(pos, key.slice.size);
        });
    }
}

template <typename HashSet>
Status ExceptHashSet<HashSet>::init(RuntimeState* state) {
    _hash_set = std::make_unique<HashSet>();
    return Status::OK();
}

template <typename HashSet>
Status ExceptHashSet<HashSet>::erase_duplicate_row(RuntimeState* state, const ChunkPtr& chunk,
                                                   const std::vector<ExprContext*>& exprs, BufferState* buffer_state) {
    size_t chunk_size = chunk->num_rows();
    buffer_state->slice_sizes.assign(state->chunk_size(), 0);

    size_t cur_max_one_row_size = _get_max_serialize_size(chunk, exprs);
    if (UNLIKELY(cur_max_one_row_size > buffer_state->max_one_row_size)) {
        buffer_state->max_one_row_size = cur_max_one_row_size;
        buffer_state->mem_pool.clear();
        buffer_state->buffer = buffer_state->mem_pool.allocate(buffer_state->max_one_row_size * state->chunk_size());
        if (UNLIKELY(buffer_state->buffer == nullptr)) {
            return Status::InternalError("Mem usage has exceed the limit of BE");
        }
        RETURN_IF_LIMIT_EXCEEDED(state, "Except, while probe hash table.");
    }

    _serialize_columns(chunk, exprs, chunk_size, buffer_state);

    for (size_t i = 0; i < chunk_size; ++i) {
        ExceptSliceFlag key(buffer_state->buffer + i * buffer_state->max_one_row_size, buffer_state->slice_sizes[i]);
        auto iter = _hash_set->find(key);
        if (iter != _hash_set->end()) {
            iter->deleted = true;
        }
    }

    return Status::OK();
}

template <typename HashSet>
void ExceptHashSet<HashSet>::deserialize_to_columns(KeyVector& keys, Columns& key_columns, size_t chunk_size) {
    for (auto& key_column : key_columns) {
        DCHECK(!key_column->is_constant());
        // Because the serialized key is always nullable,
        // drop the null byte of the key if the dest column is non-nullable.
        if (!key_column->is_nullable()) {
            for (auto& key : keys) {
                key.data += sizeof(bool);
            }
        }

        key_column->deserialize_and_append_batch(keys, chunk_size);
    }
}

template <typename HashSet>
int64_t ExceptHashSet<HashSet>::mem_usage(BufferState* buffer_state) {
    int64_t size = 0;
    if (_hash_set != nullptr) {
        size += _hash_set->dump_bound();
    }
    if (buffer_state != nullptr) {
        size += buffer_state->mem_pool.total_reserved_bytes();
    }

    return size;
}

template <typename HashSet>
size_t ExceptHashSet<HashSet>::_get_max_serialize_size(const ChunkPtr& chunk, const std::vector<ExprContext*>& exprs) {
    size_t max_size = 0;
    for (auto expr : exprs) {
        ColumnPtr key_column = EVALUATE_NULL_IF_ERROR(expr, expr->root(), chunk.get());
        max_size += key_column->max_one_element_serialize_size();
        if (!key_column->is_nullable()) {
            max_size += sizeof(bool);
        }
    }
    return max_size;
}

template <typename HashSet>
void ExceptHashSet<HashSet>::_serialize_columns(const ChunkPtr& chunk, const std::vector<ExprContext*>& exprs,
                                                size_t chunk_size, BufferState* buffer_state) {
    for (auto expr : exprs) {
        ColumnPtr key_column = EVALUATE_NULL_IF_ERROR(expr, expr->root(), chunk.get());

        // The serialized buffer is always nullable.
        if (key_column->is_nullable()) {
            key_column->serialize_batch(buffer_state->buffer, buffer_state->slice_sizes, chunk_size,
                                        buffer_state->max_one_row_size);
        } else {
            key_column->serialize_batch_with_null_masks(buffer_state->buffer, buffer_state->slice_sizes, chunk_size,
                                                        buffer_state->max_one_row_size, nullptr, false);
        }
    }
}

template class ExceptHashSet<phmap::flat_hash_set<ExceptSliceFlag, ExceptSliceFlagHash, ExceptSliceFlagEqual>>;

} // namespace starrocks
