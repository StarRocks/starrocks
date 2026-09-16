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

#include "exec/intersect_hash_set.h"

#include "base/failpoint/fail_point.h"
#include "base/phmap/phmap_dump.h"
#include "exec/aggregate/agg_hash_set.h"
#include "exec_primitive/exec_node.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// Forces the by-rows path regardless of how wide the rows actually are, so ordinary data can
// exercise it: reaching the INT32_MAX bound for real needs a row of half a megabyte or more.
DEFINE_FAIL_POINT(intersect_hash_set_force_by_rows);

template <typename HashSet>
Status IntersectHashSet<HashSet>::init(RuntimeState* state) {
    _hash_set = std::make_unique<HashSet>();
    _mem_pool = std::make_unique<MemPool>();
    _buffer = _mem_pool->allocate(_max_one_row_size * state->chunk_size() + SLICE_MEMEQUAL_OVERFLOW_PADDING);
    RETURN_IF_UNLIKELY_NULL(_buffer, Status::MemoryAllocFailed("alloc mem for intersect hash set failed"));
    return Status::OK();
}

template <typename HashSet>
void IntersectHashSet<HashSet>::build_set(RuntimeState* state, const ChunkPtr& chunkPtr,
                                          const std::vector<ExprContext*>& exprs, MemPool* pool) {
    size_t chunk_size = chunkPtr->num_rows();

    _slice_sizes.assign(state->chunk_size(), 0);
    size_t cur_max_one_row_size = _get_max_serialize_size(chunkPtr, exprs);
    // The trigger is hoisted out of the growth check on purpose: a narrow key never widens the
    // stride, so testing it inside would leave the failpoint unable to fire at all.
    bool force_by_rows = false;
    FAIL_POINT_TRIGGER_EXECUTE(intersect_hash_set_force_by_rows, { force_by_rows = true; });
    if (UNLIKELY(force_by_rows || cur_max_one_row_size > _max_one_row_size)) {
        size_t batch_allocate_size = cur_max_one_row_size * state->chunk_size() + SLICE_MEMEQUAL_OVERFLOW_PADDING;
        // too large, process by rows
        if (force_by_rows || batch_allocate_size > kMaxBatchSerializeSize) {
            // Reset the high-water mark so the next chunk is sized on its own merits instead of
            // inheriting this row's stride and being forced down the by-rows path forever.
            _max_one_row_size = 0;
            _mem_pool->clear();
            _buffer = _mem_pool->allocate(cur_max_one_row_size + SLICE_MEMEQUAL_OVERFLOW_PADDING);
            return _build_set_by_rows(chunkPtr, exprs, pool, chunk_size);
        }
        _max_one_row_size = cur_max_one_row_size;
        _mem_pool->clear();
        _buffer = _mem_pool->allocate(batch_allocate_size);
    }

    _serialize_columns(chunkPtr, exprs, chunk_size);

    for (size_t i = 0; i < chunk_size; ++i) {
        IntersectSliceFlag key(_buffer + i * _max_one_row_size, _slice_sizes[i]);
        _hash_set->lazy_emplace(key, [&](const auto& ctor) {
            // we must persist the slice before insert
            uint8_t* pos = pool->allocate_with_reserve(key.slice.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
            memcpy(pos, key.slice.data, key.slice.size);
            ctor(pos, key.slice.size);
        });
    }
}

// There may be additional virtual function overhead, but the bottleneck of this branch is serialization.
template <typename HashSet>
ALWAYS_NOINLINE void IntersectHashSet<HashSet>::_build_set_by_rows(const ChunkPtr& chunkPtr,
                                                                   const std::vector<ExprContext*>& exprs,
                                                                   MemPool* pool, size_t chunk_size) {
    Columns key_columns = _evaluate_key_columns(chunkPtr, exprs);
    for (size_t i = 0; i < chunk_size; ++i) {
        IntersectSliceFlag key(_buffer, _serialize_one_row(key_columns, i));
        _hash_set->lazy_emplace(key, [&](const auto& ctor) {
            // we must persist the slice before insert
            uint8_t* pos = pool->allocate_with_reserve(key.slice.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
            memcpy(pos, key.slice.data, key.slice.size);
            ctor(pos, key.slice.size);
        });
    }
}

template <typename HashSet>
Status IntersectHashSet<HashSet>::refine_intersect_row(RuntimeState* state, const ChunkPtr& chunkPtr,
                                                       const std::vector<ExprContext*>& exprs, const int hit_times) {
    size_t chunk_size = chunkPtr->num_rows();
    _slice_sizes.assign(state->chunk_size(), 0);
    size_t cur_max_one_row_size = _get_max_serialize_size(chunkPtr, exprs);
    // The trigger is hoisted out of the growth check on purpose: a narrow key never widens the
    // stride, so testing it inside would leave the failpoint unable to fire at all.
    bool force_by_rows = false;
    FAIL_POINT_TRIGGER_EXECUTE(intersect_hash_set_force_by_rows, { force_by_rows = true; });
    if (UNLIKELY(force_by_rows || cur_max_one_row_size > _max_one_row_size)) {
        size_t batch_allocate_size = cur_max_one_row_size * state->chunk_size() + SLICE_MEMEQUAL_OVERFLOW_PADDING;
        // too large, process by rows
        if (force_by_rows || batch_allocate_size > kMaxBatchSerializeSize) {
            _max_one_row_size = 0;
            _mem_pool->clear();
            _buffer = _mem_pool->allocate(cur_max_one_row_size + SLICE_MEMEQUAL_OVERFLOW_PADDING);
            if (UNLIKELY(_buffer == nullptr)) {
                return Status::InternalError("Mem usage has exceed the limit of BE");
            }
            RETURN_IF_LIMIT_EXCEEDED(state, "Intersect, while probe hash table.");
            _refine_intersect_row_by_rows(chunkPtr, exprs, chunk_size, hit_times);
            return Status::OK();
        }
        _max_one_row_size = cur_max_one_row_size;
        _mem_pool->clear();
        _buffer = _mem_pool->allocate(batch_allocate_size);
        if (UNLIKELY(_buffer == nullptr)) {
            return Status::InternalError("Mem usage has exceed the limit of BE");
        }
        RETURN_IF_LIMIT_EXCEEDED(state, "Intersect, while probe hash table.");
    }

    _serialize_columns(chunkPtr, exprs, chunk_size);

    for (size_t i = 0; i < chunk_size; ++i) {
        IntersectSliceFlag key(_buffer + i * _max_one_row_size, _slice_sizes[i]);
        auto iter = _hash_set->find(key);
        if (iter != _hash_set->end() && iter->hit_times == hit_times - 1) {
            iter->hit_times = hit_times;
        }
    }
    return Status::OK();
}

template <typename HashSet>
ALWAYS_NOINLINE void IntersectHashSet<HashSet>::_refine_intersect_row_by_rows(const ChunkPtr& chunkPtr,
                                                                              const std::vector<ExprContext*>& exprs,
                                                                              size_t chunk_size, int hit_times) {
    Columns key_columns = _evaluate_key_columns(chunkPtr, exprs);
    for (size_t i = 0; i < chunk_size; ++i) {
        IntersectSliceFlag key(_buffer, _serialize_one_row(key_columns, i));
        auto iter = _hash_set->find(key);
        if (iter != _hash_set->end() && iter->hit_times == hit_times - 1) {
            iter->hit_times = hit_times;
        }
    }
}

template <typename HashSet>
void IntersectHashSet<HashSet>::deserialize_to_columns(KeyVector& keys, MutableColumns& key_columns,
                                                       size_t chunk_size) {
    for (auto& key_column : key_columns) {
        // Because the serialized key is always nullable,
        // drop the null byte of the key if the dest column is non-nullable.
        if (!key_column->is_nullable()) {
            for (auto& key : keys) {
                key.data += sizeof(bool);
            }
        } else if (key_column->is_constant()) {
            continue;
        }

        key_column->deserialize_and_append_batch(keys, chunk_size);
    }
}

template <typename HashSet>
int64_t IntersectHashSet<HashSet>::mem_usage() const {
    int64_t size = 0;
    if (_hash_set != nullptr) {
        size += _hash_set->dump_bound();
    }
    if (_mem_pool != nullptr) {
        size += _mem_pool->total_reserved_bytes();
    }
    return size;
}

template <typename HashSet>
size_t IntersectHashSet<HashSet>::_get_max_serialize_size(const ChunkPtr& chunkPtr,
                                                          const std::vector<ExprContext*>& exprs) {
    size_t max_size = 0;
    for (auto* expr : exprs) {
        ColumnPtr key_column = EVALUATE_NULL_IF_ERROR(expr, expr->root(), chunkPtr.get());
        max_size += key_column->max_one_element_serialize_size();
        if (!key_column->is_nullable()) {
            max_size += sizeof(bool);
        }
    }
    return max_size;
}

template <typename HashSet>
Columns IntersectHashSet<HashSet>::_evaluate_key_columns(const ChunkPtr& chunkPtr,
                                                         const std::vector<ExprContext*>& exprs) {
    Columns key_columns;
    key_columns.reserve(exprs.size());
    for (auto* expr : exprs) {
        key_columns.emplace_back(EVALUATE_NULL_IF_ERROR(expr, expr->root(), chunkPtr.get()));
    }
    return key_columns;
}

// Mirrors _serialize_columns for a single row. The serialized key is always nullable, so a
// non-nullable column contributes a false null byte ahead of its value -- the same layout
// Column::serialize_batch_with_null_masks(..., nullptr, false) produces.
template <typename HashSet>
size_t IntersectHashSet<HashSet>::_serialize_one_row(const Columns& key_columns, size_t idx) {
    uint8_t* cursor = _buffer;
    for (const auto& key_column : key_columns) {
        if (key_column->is_nullable()) {
            cursor += key_column->serialize(idx, cursor);
        } else {
            constexpr bool kNotNull = false;
            memcpy(cursor, &kNotNull, sizeof(bool));
            cursor += sizeof(bool) + key_column->serialize(idx, cursor + sizeof(bool));
        }
    }
    return cursor - _buffer;
}

template <typename HashSet>
void IntersectHashSet<HashSet>::_serialize_columns(const ChunkPtr& chunkPtr, const std::vector<ExprContext*>& exprs,
                                                   size_t chunk_size) {
    for (auto expr : exprs) {
        ColumnPtr key_column = EVALUATE_NULL_IF_ERROR(expr, expr->root(), chunkPtr.get());

        // The serialized buffer is always nullable.
        if (key_column->is_nullable()) {
            key_column->serialize_batch(_buffer, _slice_sizes, chunk_size, _max_one_row_size);
        } else {
            key_column->serialize_batch_with_null_masks(_buffer, _slice_sizes, chunk_size, _max_one_row_size, nullptr,
                                                        false);
        }
    }
}

// instantiation
template class IntersectHashSet<
        phmap::flat_hash_set<IntersectSliceFlag, IntersectSliceFlagHash, IntersectSliceFlagEqual>>;

} // namespace starrocks
