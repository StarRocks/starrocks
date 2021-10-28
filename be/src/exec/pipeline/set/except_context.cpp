// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/set/except_context.h"

namespace starrocks::pipeline {

Status ExceptContext::prepare(RuntimeState* state, MemTracker* mem_tracker) {
    _build_pool = std::make_unique<MemPool>(mem_tracker);

    _dst_tuple_desc = state->desc_tbl().get_tuple_descriptor(_dst_tuple_id);
    _dst_nullables.assign(_dst_tuple_desc->slots().size(), false);

    return Status::OK();
}

void ExceptContext::close(RuntimeState* state) {
    if (_build_pool != nullptr) {
        _build_pool->free_all();
    }
}

Status ExceptContext::append_chunk_to_ht(RuntimeState* state, const ChunkPtr& chunk,
                                         const std::vector<ExprContext*>& dst_exprs) {
    vectorized::ExceptHashSerializeSet::GetTypeFunc get_type_func;
    if (!_has_set_dst_nullables) {
        _has_set_dst_nullables = true;
        get_type_func = [this](const ColumnPtr& column, int i) -> void { _dst_nullables[i] = column->is_nullable(); };
    } else {
        get_type_func = [](const ColumnPtr& column, int i) -> void {};
    }

    return _hash_set->build_set(state, chunk, dst_exprs, _build_pool.get(), get_type_func);
}

Status ExceptContext::erase_chunk_from_ht(RuntimeState* state, const ChunkPtr& chunk,
                                          const std::vector<ExprContext*>& dst_exprs) {
    return _hash_set->erase_duplicate_row(state, chunk, dst_exprs);
}

StatusOr<vectorized::ChunkPtr> ExceptContext::pull_chunk(RuntimeState* state) {
    // 1. Get at most *config::vector_chunk_size* remained keys from ht.
    size_t remained_keys_num = 0;
    _remained_keys.resize(config::vector_chunk_size);
    while (_next_processed_iter != _hash_set->end() && remained_keys_num < config::vector_chunk_size) {
        if (!_next_processed_iter->deleted) {
            _remained_keys[remained_keys_num++] = _next_processed_iter->slice;
        }
        ++_next_processed_iter;
    }

    ChunkPtr dst_chunk = std::make_shared<vectorized::Chunk>();
    if (remained_keys_num > 0) {
        // 2. Create dest columns.
        vectorized::Columns dst_columns(_dst_nullables.size());
        for (size_t i = 0; i < _dst_nullables.size(); ++i) {
            dst_columns[i] =
                    vectorized::ColumnHelper::create_column(_dst_tuple_desc->slots()[i]->type(), _dst_nullables[i]);
            dst_columns[i]->reserve(remained_keys_num);
        }

        // 3. Serialize remained keys to the dest columns.
        _hash_set->deserialize_to_columns(_remained_keys, dst_columns, remained_keys_num);

        // 4. Add dest columns to the dest chunk.
        for (size_t i = 0; i < dst_columns.size(); i++) {
            dst_chunk->append_column(std::move(dst_columns[i]), _dst_tuple_desc->slots()[i]->id());
        }
    }

    return std::move(dst_chunk);
}

} // namespace starrocks::pipeline
