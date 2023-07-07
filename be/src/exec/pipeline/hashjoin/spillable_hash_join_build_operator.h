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

#pragma once

#include <exprs/predicate.h>

#include <atomic>
#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/hashjoin/hash_join_build_operator.h"
#include "exec/spill/spiller.h"
#include "exprs/expr_context.h"

namespace starrocks::pipeline {

class SpillableHashJoinBuildOperator final : public HashJoinBuildOperator {
public:
    template <class... Args>
    SpillableHashJoinBuildOperator(Args&&... args) : HashJoinBuildOperator(std::forward<Args>(args)...) {}

    ~SpillableHashJoinBuildOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool need_input() const override;

    Status set_finishing(RuntimeState* state) override;

    bool is_finished() const override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    bool spillable() const override { return true; }
    void set_execute_mode(int performance_level) override;

    size_t estimated_memory_reserved(const ChunkPtr& chunk) override;
    size_t estimated_memory_reserved() override;

private:
    void set_spill_strategy(spill::SpillStrategy strategy) { _join_builder->set_spill_strategy(strategy); }
    spill::SpillStrategy spill_strategy() const { return _join_builder->spill_strategy(); }

    std::function<StatusOr<ChunkPtr>()> _convert_hash_map_to_chunk();

    Status publish_runtime_filters(RuntimeState* state);

    Status append_hash_columns(const ChunkPtr& chunk);

    Status init_spiller_partitions(RuntimeState* state, JoinHashTable& ht);

    ChunkSharedSlice _hash_table_build_chunk_slice;
    std::function<StatusOr<ChunkPtr>()> _hash_table_slice_iterator;
    bool _is_first_time_spill = true;
    size_t _push_numbers = 0;
};

class SpillableHashJoinBuildOperatorFactory final : public HashJoinBuildOperatorFactory {
public:
    template <class... Args>
    SpillableHashJoinBuildOperatorFactory(Args&&... args) : HashJoinBuildOperatorFactory(std::forward<Args>(args)...) {}

    ~SpillableHashJoinBuildOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    const std::vector<ExprContext*>& build_side_partition() { return _build_side_partition; }

private:
    ObjectPool _pool;

    std::vector<ExprContext*> _build_side_partition;

    std::shared_ptr<spill::SpilledOptions> _spill_options;
    std::shared_ptr<spill::SpillerFactory> _spill_factory = std::make_shared<spill::SpillerFactory>();
};

} // namespace starrocks::pipeline
