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

#include "base/failpoint/fail_point.h"
#include "exec/aggregate/agg_hash_set.h"
#include "exec_primitive/exec_node.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// Forces the by-rows path regardless of how wide the rows actually are, so ordinary data can
// exercise it: reaching the INT32_MAX bound for real needs a row of half a megabyte or more.
DEFINE_FAIL_POINT(except_hash_set_force_by_rows);

template <typename HashSet>
Status ExceptHashSet<HashSet>::BufferState::init(RuntimeState* state) {
    buffer = mem_pool.allocate(max_one_row_size * state->chunk_size());
    RETURN_IF_UNLIKELY_NULL(buffer, Status::MemoryAllocFailed("alloc mem of except hash set failed"));
    slice_sizes.reserve(state->chunk_size());
    return Status::OK();
}

template <typename HashSet>
Status ExceptHashSet<HashSet>::build_set(RuntimeState* state, const ChunkPtr& chunk,
                                         const std::vector<ExprContext*>& exprs, MemPool* pool,
                                         BufferState* buffer_state) {
    size_t chunk_size = chunk->num_rows();
    buffer_state->slice_sizes.assign(state->chunk_size(), 0);

    ASSIGN_OR_RETURN(size_t cur_max_one_row_size, _get_max_serialize_size(chunk, exprs));
    // The trigger is hoisted out of the growth check on purpose: a narrow key never widens the
    // stride, so testing it inside would leave the failpoint unable to fire at all.
    bool force_by_rows = false;
    FAIL_POINT_TRIGGER_EXECUTE(except_hash_set_force_by_rows, { force_by_rows = true; });
    if (UNLIKELY(force_by_rows || cur_max_one_row_size > buffer_state->max_one_row_size)) {
        size_t batch_allocate_size = cur_max_one_row_size * state->chunk_size() + SLICE_MEMEQUAL_OVERFLOW_PADDING;
        // too large, process by rows
        if (force_by_rows || batch_allocate_size > kMaxBatchSerializeSize) {
            // Reset the high-water mark so the next chunk is sized on its own merits instead of
            // inheriting this row's stride and being forced down the by-rows path forever.
            buffer_state->max_one_row_size = 0;
            buffer_state->mem_pool.clear();
            buffer_state->buffer =
                    buffer_state->mem_pool.allocate(cur_max_one_row_size + SLICE_MEMEQUAL_OVERFLOW_PADDING);
            return _build_set_by_rows(chunk, exprs, pool, chunk_size, buffer_state);
        }
        buffer_state->max_one_row_size = cur_max_one_row_size;
        buffer_state->mem_pool.clear();
        buffer_state->buffer = buffer_state->mem_pool.allocate(batch_allocate_size);
    }

    RETURN_IF_ERROR(_serialize_columns(chunk, exprs, chunk_size, buffer_state));

    for (size_t i = 0; i < chunk_size; ++i) {
        ExceptSliceFlag key(buffer_state->buffer + i * buffer_state->max_one_row_size, buffer_state->slice_sizes[i]);
        _hash_set->lazy_emplace(key, [&](const auto& ctor) {
            uint8_t* pos = pool->allocate_with_reserve(key.slice.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
            memcpy(pos, key.slice.data, key.slice.size);
            ctor(pos, key.slice.size);
        });
    }
    return Status::OK();
}

// There may be additional virtual function overhead, but the bottleneck of this branch is serialization.
template <typename HashSet>
ALWAYS_NOINLINE Status ExceptHashSet<HashSet>::_build_set_by_rows(const ChunkPtr& chunk,
                                                                  const std::vector<ExprContext*>& exprs, MemPool* pool,
                                                                  size_t chunk_size, BufferState* buffer_state) {
    ASSIGN_OR_RETURN(Columns key_columns, _evaluate_key_columns(chunk, exprs));
    for (size_t i = 0; i < chunk_size; ++i) {
        ExceptSliceFlag key(buffer_state->buffer, _serialize_one_row(key_columns, i, buffer_state));
        _hash_set->lazy_emplace(key, [&](const auto& ctor) {
            uint8_t* pos = pool->allocate_with_reserve(key.slice.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
            memcpy(pos, key.slice.data, key.slice.size);
            ctor(pos, key.slice.size);
        });
    }
    return Status::OK();
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

    ASSIGN_OR_RETURN(size_t cur_max_one_row_size, _get_max_serialize_size(chunk, exprs));
    // The trigger is hoisted out of the growth check on purpose: a narrow key never widens the
    // stride, so testing it inside would leave the failpoint unable to fire at all.
    bool force_by_rows = false;
    FAIL_POINT_TRIGGER_EXECUTE(except_hash_set_force_by_rows, { force_by_rows = true; });
    if (UNLIKELY(force_by_rows || cur_max_one_row_size > buffer_state->max_one_row_size)) {
        // The padding is what build_set already reserves; both paths share this buffer and
        // ExceptSliceFlagEqual reads past the key with SIMD, so the probe side needs it too.
        size_t batch_allocate_size = cur_max_one_row_size * state->chunk_size() + SLICE_MEMEQUAL_OVERFLOW_PADDING;
        // too large, process by rows
        if (force_by_rows || batch_allocate_size > kMaxBatchSerializeSize) {
            buffer_state->max_one_row_size = 0;
            buffer_state->mem_pool.clear();
            buffer_state->buffer =
                    buffer_state->mem_pool.allocate(cur_max_one_row_size + SLICE_MEMEQUAL_OVERFLOW_PADDING);
            if (UNLIKELY(buffer_state->buffer == nullptr)) {
                return Status::InternalError("Mem usage has exceed the limit of BE");
            }
            RETURN_IF_LIMIT_EXCEEDED(state, "Except, while probe hash table.");
            return _erase_duplicate_row_by_rows(chunk, exprs, chunk_size, buffer_state);
        }
        buffer_state->max_one_row_size = cur_max_one_row_size;
        buffer_state->mem_pool.clear();
        buffer_state->buffer = buffer_state->mem_pool.allocate(batch_allocate_size);
        if (UNLIKELY(buffer_state->buffer == nullptr)) {
            return Status::InternalError("Mem usage has exceed the limit of BE");
        }
        RETURN_IF_LIMIT_EXCEEDED(state, "Except, while probe hash table.");
    }

    RETURN_IF_ERROR(_serialize_columns(chunk, exprs, chunk_size, buffer_state));

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
ALWAYS_NOINLINE Status ExceptHashSet<HashSet>::_erase_duplicate_row_by_rows(const ChunkPtr& chunk,
                                                                            const std::vector<ExprContext*>& exprs,
                                                                            size_t chunk_size,
                                                                            BufferState* buffer_state) {
    ASSIGN_OR_RETURN(Columns key_columns, _evaluate_key_columns(chunk, exprs));
    for (size_t i = 0; i < chunk_size; ++i) {
        ExceptSliceFlag key(buffer_state->buffer, _serialize_one_row(key_columns, i, buffer_state));
        auto iter = _hash_set->find(key);
        if (iter != _hash_set->end()) {
            iter->deleted = true;
        }
    }
    return Status::OK();
}

template <typename HashSet>
Status ExceptHashSet<HashSet>::deserialize_to_columns(KeyVector& keys, MutableColumns& key_columns, size_t chunk_size) {
    for (auto& key_column : key_columns) {
        DCHECK(!key_column->is_constant());
        // Because the serialized key is always nullable,
        // drop the null byte of the key if the dest column is non-nullable.
        if (!key_column->is_nullable()) {
            for (auto& key : keys) {
                key.data += sizeof(bool);
            }
        }

        TRY_CATCH_BAD_ALLOC(key_column->deserialize_and_append_batch(keys, chunk_size));
    }
    return Status::OK();
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
StatusOr<size_t> ExceptHashSet<HashSet>::_get_max_serialize_size(const ChunkPtr& chunk,
                                                                 const std::vector<ExprContext*>& exprs) {
    size_t max_size = 0;
    for (auto expr : exprs) {
        ASSIGN_OR_RETURN(ColumnPtr key_column, expr->evaluate(chunk.get()));
        max_size += key_column->max_one_element_serialize_size();
        if (!key_column->is_nullable()) {
            max_size += sizeof(bool);
        }
    }
    return max_size;
}

template <typename HashSet>
StatusOr<Columns> ExceptHashSet<HashSet>::_evaluate_key_columns(const ChunkPtr& chunk,
                                                                const std::vector<ExprContext*>& exprs) {
    Columns key_columns;
    key_columns.reserve(exprs.size());
    for (auto expr : exprs) {
        ASSIGN_OR_RETURN(auto key_column, expr->evaluate(chunk.get()));
        key_columns.emplace_back(std::move(key_column));
    }
    return key_columns;
}

// Mirrors _serialize_columns for a single row. The serialized key is always nullable, so a
// non-nullable column contributes a false null byte ahead of its value -- the same layout
// Column::serialize_batch_with_null_masks(..., nullptr, false) produces.
template <typename HashSet>
size_t ExceptHashSet<HashSet>::_serialize_one_row(const Columns& key_columns, size_t idx, BufferState* buffer_state) {
    uint8_t* cursor = buffer_state->buffer;
    for (const auto& key_column : key_columns) {
        if (key_column->is_nullable()) {
            cursor += key_column->serialize(idx, cursor);
        } else {
            constexpr bool kNotNull = false;
            memcpy(cursor, &kNotNull, sizeof(bool));
            cursor += sizeof(bool) + key_column->serialize(idx, cursor + sizeof(bool));
        }
    }
    return cursor - buffer_state->buffer;
}

template <typename HashSet>
Status ExceptHashSet<HashSet>::_serialize_columns(const ChunkPtr& chunk, const std::vector<ExprContext*>& exprs,
                                                  size_t chunk_size, BufferState* buffer_state) {
    for (auto expr : exprs) {
        ASSIGN_OR_RETURN(ColumnPtr key_column, expr->evaluate(chunk.get()));

        // The serialized buffer is always nullable.
        if (key_column->is_nullable()) {
            key_column->serialize_batch(buffer_state->buffer, buffer_state->slice_sizes, chunk_size,
                                        buffer_state->max_one_row_size);
        } else {
            key_column->serialize_batch_with_null_masks(buffer_state->buffer, buffer_state->slice_sizes, chunk_size,
                                                        buffer_state->max_one_row_size, nullptr, false);
        }
    }
    return Status::OK();
}

template class ExceptHashSet<phmap::flat_hash_set<ExceptSliceFlag, ExceptSliceFlagHash, ExceptSliceFlagEqual>>;

} // namespace starrocks
