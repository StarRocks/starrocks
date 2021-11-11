// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/intersect_hash_set.h"

#include "exec/exec_node.h"
#include "util/phmap/phmap_dump.h"

namespace starrocks::vectorized {

template <typename HashSet>
Status IntersectHashSet<HashSet>::build_set(RuntimeState* state, const ChunkPtr& chunkPtr,
                                            const std::vector<ExprContext*>& exprs, MemPool* pool) {
    size_t chunk_size = chunkPtr->num_rows();

    _slice_sizes.assign(config::vector_chunk_size, 0);
    size_t cur_max_one_row_size = _get_max_serialize_size(chunkPtr, exprs);
    if (UNLIKELY(cur_max_one_row_size > _max_one_row_size)) {
        _max_one_row_size = cur_max_one_row_size;
        _mem_pool->clear();
        _buffer = _mem_pool->allocate(_max_one_row_size * config::vector_chunk_size);
        if (UNLIKELY(_buffer == nullptr)) {
            return Status::InternalError("Mem usage has exceed the limit of BE");
        }
    }

    _serialize_columns(chunkPtr, exprs, chunk_size);

    for (size_t i = 0; i < chunk_size; ++i) {
        IntersectSliceFlag key(_buffer + i * _max_one_row_size, _slice_sizes[i]);
        _hash_set->lazy_emplace(key, [&](const auto& ctor) {
            // we must persist the slice before insert
            uint8_t* pos = pool->allocate(key.slice.size);
            memcpy(pos, key.slice.data, key.slice.size);
            ctor(pos, key.slice.size);
        });
    }
    RETURN_IF_LIMIT_EXCEEDED(state, "Intersect, while build hash table.");
    return Status::OK();
}

template <typename HashSet>
Status IntersectHashSet<HashSet>::refine_intersect_row(RuntimeState* state, const ChunkPtr& chunkPtr,
                                                       const std::vector<ExprContext*>& exprs, const int hit_times) {
    size_t chunk_size = chunkPtr->num_rows();
    _slice_sizes.assign(config::vector_chunk_size, 0);
    size_t cur_max_one_row_size = _get_max_serialize_size(chunkPtr, exprs);
    if (UNLIKELY(cur_max_one_row_size > _max_one_row_size)) {
        _max_one_row_size = cur_max_one_row_size;
        _mem_pool->clear();
        _buffer = _mem_pool->allocate(_max_one_row_size * config::vector_chunk_size);
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
void IntersectHashSet<HashSet>::deserialize_to_columns(KeyVector& keys, const Columns& key_columns, size_t batch_size) {
    for (const auto& key_column : key_columns) {
        // Because the serialized key is always nullable,
        // drop the null byte of the key if the dest column is non-nullable.
        if (!key_column->is_nullable()) {
            for (auto& key : keys) {
                key.data += sizeof(bool);
            }
        } else if (key_column->is_constant()) {
            continue;
        }

        key_column->deserialize_and_append_batch(keys, batch_size);
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
        ColumnPtr key_column = expr->evaluate(chunkPtr.get());
        max_size += key_column->max_one_element_serialize_size();
        if (!key_column->is_nullable()) {
            max_size += sizeof(bool);
        }
    }
    return max_size;
}

template <typename HashSet>
void IntersectHashSet<HashSet>::_serialize_columns(const ChunkPtr& chunkPtr, const std::vector<ExprContext*>& exprs,
                                                   size_t chunk_size) {
    for (auto expr : exprs) {
        ColumnPtr key_column = expr->evaluate(chunkPtr.get());

        // The serialized buffer is always nullable.
        if (key_column->is_nullable()) {
            key_column->serialize_batch(_buffer, _slice_sizes, chunk_size, _max_one_row_size);
        } else {
            key_column->serialize_batch_with_null_masks(_buffer, _slice_sizes, chunk_size, _max_one_row_size, nullptr,
                                                        false);
        }
    }
}

} // namespace starrocks::vectorized