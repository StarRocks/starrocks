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

#include "exec/pipeline/aggregate/spillable_aggregate_skew_compactor.h"

#include <memory>
#include <utility>
#include <vector>

#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

spill::SpillSkewChunkCompactor make_spill_aggregate_skew_compactor(AggregatorParamsPtr aggregator_params) {
    return [aggregator_params = std::move(aggregator_params)](
                   std::vector<ChunkPtr>& chunks, const spill::SpillSkewChunkCompactContext& context) -> Status {
        auto merger = std::make_shared<Aggregator>(aggregator_params);
        merger->set_aggr_mode(AM_STREAMING_POST_CACHE);
        RuntimeProfile* profile = context.state->runtime_profile()->create_child("spillable_pw_skew_elimination", true);
        RETURN_IF_ERROR(merger->prepare(context.state, profile));
        RETURN_IF_ERROR(merger->open(context.state));
        DeferOp defer([&merger, state = context.state]() { merger->close(state); });

        std::vector<ChunkPtr> new_chunks;
        for (auto& chunk : chunks) {
            if (chunk == nullptr || chunk->is_empty()) {
                continue;
            }
            auto* hash_column = down_cast<const UInt32Column*>(chunk->columns().back().get());
            auto& hash_values = hash_column->get_data();
            std::vector<uint32_t> indices;
            Filter filter;
            indices.reserve(chunk->num_rows());
            filter.reserve(chunk->num_rows());
            for (uint32_t i = 0; i < hash_values.size(); ++i) {
                if (hash_values[i] == context.target_hash_value) {
                    indices.push_back(i);
                    filter.push_back(0);
                } else {
                    filter.push_back(1);
                }
            }
            auto chunk_merging = chunk->clone_empty_with_slot(indices.size());
            chunk_merging->append_selective(*chunk, indices.data(), 0, indices.size());
            chunk->filter(filter);
            if (!chunk_merging->is_empty()) {
                RETURN_IF_ERROR(merger->evaluate_groupby_exprs(chunk_merging.get()));
                if (merger->only_group_by_exprs()) {
                    merger->build_hash_set(chunk_merging->num_rows());
                } else {
                    merger->build_hash_map(chunk_merging->num_rows(), false);
                    RETURN_IF_ERROR(merger->compute_batch_agg_states(chunk_merging.get(), chunk_merging->num_rows()));
                }
            }
            if (!chunk->is_empty()) {
                new_chunks.emplace_back(std::move(chunk));
            }
        }

        auto chunk_merged = std::make_shared<Chunk>();
        if (merger->only_group_by_exprs()) {
            merger->hash_set_variant().visit(
                    [&](auto& hash_set_with_key) { merger->it_hash() = hash_set_with_key->hash_set.begin(); });
            auto hash_set_sz = merger->hash_set_variant().size();
            merger->convert_hash_set_to_chunk(hash_set_sz, &chunk_merged);
        } else {
            merger->it_hash() = merger->state_allocator().begin();
            auto hash_map_sz = merger->hash_map_variant().size();
            RETURN_IF_ERROR(merger->convert_hash_map_to_chunk(hash_map_sz, &chunk_merged, true));
        }
        if (chunk_merged != nullptr && !chunk_merged->is_empty()) {
            auto hash_column = UInt32Column::create(chunk_merged->num_rows(), context.target_hash_value);
            chunk_merged->append_column(hash_column, Chunk::HASH_AGG_SPILL_HASH_SLOT_ID);
            new_chunks.emplace_back(std::move(chunk_merged));
        }
        chunks = std::move(new_chunks);
        return Status::OK();
    };
}

} // namespace starrocks::pipeline
