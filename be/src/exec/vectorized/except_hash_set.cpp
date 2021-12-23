// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/except_hash_set.h"

#include "exec/exec_node.h"
#include "runtime/mem_tracker.h"

namespace starrocks::vectorized {

template <typename HashSet>
Status ExceptHashSet<HashSet>::build_set(RuntimeState* state, const ChunkPtr& chunk,
                                         const std::vector<ExprContext*>& exprs, MemPool* pool) {
    size_t chunk_size = chunk->num_rows();
    _slice_sizes.assign(config::vector_chunk_size, 0);

    size_t cur_max_one_row_size = _get_max_serialize_size(chunk, exprs);
    if (UNLIKELY(cur_max_one_row_size > _max_one_row_size)) {
        _max_one_row_size = cur_max_one_row_size;
        _mem_pool->clear();
        _buffer = _mem_pool->allocate(_max_one_row_size * config::vector_chunk_size);
        if (UNLIKELY(_buffer == nullptr)) {
            return Status::InternalError("Mem usage has exceed the limit of BE");
        }
    }

    _serialize_columns(chunk, exprs, chunk_size);

    for (size_t i = 0; i < chunk_size; ++i) {
        ExceptSliceFlag key(_buffer + i * _max_one_row_size, _slice_sizes[i]);
        _hash_set->lazy_emplace(key, [&](const auto& ctor) {
            uint8_t* pos = pool->allocate(key.slice.size);
            memcpy(pos, key.slice.data, key.slice.size);
            ctor(pos, key.slice.size);
        });
    }

    RETURN_IF_LIMIT_EXCEEDED(state, "Except, while build hash table.");
    return Status::OK();
}

template <typename HashSet>
Status ExceptHashSet<HashSet>::erase_duplicate_row(RuntimeState* state, const ChunkPtr& chunk,
                                                   const std::vector<ExprContext*>& exprs) {
    size_t chunk_size = chunk->num_rows();
    _slice_sizes.assign(config::vector_chunk_size, 0);

    size_t cur_max_one_row_size = _get_max_serialize_size(chunk, exprs);
    if (UNLIKELY(cur_max_one_row_size > _max_one_row_size)) {
        _max_one_row_size = cur_max_one_row_size;
        _mem_pool->clear();
        _buffer = _mem_pool->allocate(_max_one_row_size * config::vector_chunk_size);
        if (UNLIKELY(_buffer == nullptr)) {
            return Status::InternalError("Mem usage has exceed the limit of BE");
        }
        RETURN_IF_LIMIT_EXCEEDED(state, "Except, while probe hash table.");
    }

    _serialize_columns(chunk, exprs, chunk_size);

    for (size_t i = 0; i < chunk_size; ++i) {
        ExceptSliceFlag key(_buffer + i * _max_one_row_size, _slice_sizes[i]);
        auto iter = _hash_set->find(key);
        if (iter != _hash_set->end()) {
            iter->deleted = true;
        }
    }

    return Status::OK();
}

template <typename HashSet>
void ExceptHashSet<HashSet>::deserialize_to_columns(KeyVector& keys, const Columns& key_columns, size_t batch_size) {
    for (auto& key_column : key_columns) {
        DCHECK(!key_column->is_constant());
        // Because the serialized key is always nullable,
        // drop the null byte of the key if the dest column is non-nullable.
        if (!key_column->is_nullable()) {
            for (auto& key : keys) {
                key.data += sizeof(bool);
            }
        }

        key_column->deserialize_and_append_batch(keys, batch_size);
    }
}

template <typename HashSet>
size_t ExceptHashSet<HashSet>::_get_max_serialize_size(const ChunkPtr& chunk, const std::vector<ExprContext*>& exprs) {
    size_t max_size = 0;
    for (auto expr : exprs) {
        ColumnPtr key_column = expr->evaluate(chunk.get());
        max_size += key_column->max_one_element_serialize_size();
        if (!key_column->is_nullable()) {
            max_size += sizeof(bool);
        }
    }
    return max_size;
}

template <typename HashSet>
void ExceptHashSet<HashSet>::_serialize_columns(const ChunkPtr& chunk, const std::vector<ExprContext*>& exprs,
                                                size_t chunk_size) {
    for (auto expr : exprs) {
        ColumnPtr key_column = expr->evaluate(chunk.get());

        // The serialized buffer is always nullable.
        if (key_column->is_nullable()) {
            key_column->serialize_batch(_buffer, _slice_sizes, chunk_size, _max_one_row_size);
        } else {
            key_column->serialize_batch_with_null_masks(_buffer, _slice_sizes, chunk_size, _max_one_row_size, nullptr,
                                                        false);
        }
    }
}

template class ExceptHashSet<phmap::flat_hash_set<ExceptSliceFlag, ExceptSliceFlagHash, ExceptSliceFlagEqual>>;

} // namespace starrocks::vectorized
