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

#include "exec/pipeline/set/except_context.h"

#include "runtime/current_thread.h"

namespace starrocks::pipeline {

/// ExceptContext.
Status ExceptContext::prepare(RuntimeState* state, const std::vector<ExprContext*>& build_exprs) {
    _build_pool = std::make_unique<MemPool>();

    RETURN_IF_ERROR(_hash_set->init(state));

    _dst_tuple_desc = state->desc_tbl().get_tuple_descriptor(_dst_tuple_id);
    _dst_nullables.reserve(build_exprs.size());
    for (auto build_expr : build_exprs) {
        _dst_nullables.emplace_back(build_expr->root()->is_nullable());
    }

    return Status::OK();
}

void ExceptContext::close(RuntimeState* state) {
    _hash_set.reset();
    if (_build_pool != nullptr) {
        _build_pool->free_all();
    }
}

void ExceptContext::incr_prober(size_t factory_idx) {
    ++_num_probers_per_factory[factory_idx];
}
void ExceptContext::finish_probe_ht(size_t factory_idx) {
    ++_num_finished_probers_per_factory[factory_idx];
}

bool ExceptContext::is_build_finished() const {
    return _is_build_finished;
}
bool ExceptContext::is_probe_finished() const {
    for (int i = 0; i < _num_probers_per_factory.size(); ++i) {
        if (_num_probers_per_factory[i] == 0 || _num_finished_probers_per_factory[i] < _num_probers_per_factory[i]) {
            return false;
        }
    }
    return true;
}

Status ExceptContext::append_chunk_to_ht(RuntimeState* state, const ChunkPtr& chunk,
                                         const std::vector<ExprContext*>& dst_exprs, ExceptBufferState* buffer_state) {
    TRY_CATCH_BAD_ALLOC(_hash_set->build_set(state, chunk, dst_exprs, _build_pool.get(), buffer_state));
    return Status::OK();
}

Status ExceptContext::erase_chunk_from_ht(RuntimeState* state, const ChunkPtr& chunk,
                                          const std::vector<ExprContext*>& dst_exprs, ExceptBufferState* buffer_state) {
    return _hash_set->erase_duplicate_row(state, chunk, dst_exprs, buffer_state);
}

StatusOr<ChunkPtr> ExceptContext::pull_chunk(RuntimeState* state) {
    // 1. Get at most *state->chunk_size()* remained keys from ht.
    size_t num_remained_keys = 0;
    _remained_keys.resize(state->chunk_size());
    while (_next_processed_iter != _hash_set->end() && num_remained_keys < state->chunk_size()) {
        if (!_next_processed_iter->deleted) {
            _remained_keys[num_remained_keys++] = _next_processed_iter->slice;
        }
        ++_next_processed_iter;
    }

    ChunkPtr dst_chunk = std::make_shared<Chunk>();
    if (num_remained_keys > 0) {
        // 2. Create dest columns.
        Columns dst_columns(_dst_nullables.size());
        for (size_t i = 0; i < _dst_nullables.size(); ++i) {
            const auto& slot = _dst_tuple_desc->slots()[i];
            dst_columns[i] = ColumnHelper::create_column(slot->type(), _dst_nullables[i]);
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

/// ExceptPartitionContextFactory.
ExceptContextPtr ExceptPartitionContextFactory::get(const int partition_id) {
    return _partition_id2ctx[partition_id % _partition_id2ctx.size()];
}

} // namespace starrocks::pipeline
