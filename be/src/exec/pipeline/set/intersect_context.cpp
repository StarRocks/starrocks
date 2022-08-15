// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/set/intersect_context.h"

#include "runtime/current_thread.h"

namespace starrocks::pipeline {

Status IntersectContext::prepare(RuntimeState* state, const std::vector<ExprContext*>& build_exprs) {
    RETURN_IF_ERROR(_hash_set->init(state));
    _build_pool = std::make_unique<MemPool>();

    _dst_tuple_desc = state->desc_tbl().get_tuple_descriptor(_dst_tuple_id);
    _dst_nullables.reserve(build_exprs.size());
    for (auto build_expr : build_exprs) {
        _dst_nullables.emplace_back(build_expr->root()->is_nullable());
    }

    return Status::OK();
}

void IntersectContext::close(RuntimeState* state) {
    _hash_set.reset();
    if (_build_pool != nullptr) {
        _build_pool->free_all();
    }
}

Status IntersectContext::append_chunk_to_ht(RuntimeState* state, const ChunkPtr& chunk,
                                            const std::vector<ExprContext*>& dst_exprs) {
    TRY_CATCH_BAD_ALLOC(_hash_set->build_set(state, chunk, dst_exprs, _build_pool.get()));
    return Status::OK();
}

Status IntersectContext::refine_chunk_from_ht(RuntimeState* state, const ChunkPtr& chunk,
                                              const std::vector<ExprContext*>& dst_exprs, const int hit_times) {
    return _hash_set->refine_intersect_row(state, chunk, dst_exprs, hit_times);
}

StatusOr<vectorized::ChunkPtr> IntersectContext::pull_chunk(RuntimeState* state) {
    // 1. Get at most *state->chunk_size()* remained keys from ht.
    size_t num_remained_keys = 0;
    _remained_keys.resize(state->chunk_size());
    while (_next_processed_iter != _hash_set->end() && num_remained_keys < state->chunk_size()) {
        if (_next_processed_iter->hit_times == _intersect_times) {
            _remained_keys[num_remained_keys++] = _next_processed_iter->slice;
        }
        ++_next_processed_iter;
    }

    ChunkPtr dst_chunk = std::make_shared<vectorized::Chunk>();
    if (num_remained_keys > 0) {
        // 2. Create dest columns.
        vectorized::Columns dst_columns(_dst_nullables.size());
        for (size_t i = 0; i < _dst_nullables.size(); ++i) {
            const auto& slot = _dst_tuple_desc->slots()[i];
            dst_columns[i] = vectorized::ColumnHelper::create_column(slot->type(), _dst_nullables[i]);
            dst_columns[i]->reserve(num_remained_keys);
        }

        // 3. Serialize remained keys to the dest columns.
        _hash_set->deserialize_to_columns(_remained_keys, dst_columns, num_remained_keys);

        // 4. Add dest columns to the dest chunk.
        for (size_t i = 0; i < dst_columns.size(); i++) {
            dst_chunk->append_column(std::move(dst_columns[i]), _dst_tuple_desc->slots()[i]->id());
        }
    }

    return std::move(dst_chunk);
}

} // namespace starrocks::pipeline
