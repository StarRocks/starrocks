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

#include "exec/hash_joiner.h"
#include "exec/pipeline/hashjoin/hash_joiner_factory.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"

namespace starrocks::pipeline {

using HashJoiner = starrocks::HashJoiner;

class HashJoinBuildOperator : public Operator {
public:
    HashJoinBuildOperator(OperatorFactory* factory, int32_t id, const string& name, int32_t plan_node_id,
                          int32_t driver_sequence, HashJoinerPtr join_builder,
                          PartialRuntimeFilterMerger* partial_rf_merger, TJoinDistributionMode::type distribution_mode);
    ~HashJoinBuildOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override {
        CHECK(false) << "has_output not supported in HashJoinBuildOperator";
        return false;
    }

    bool need_input() const override { return !is_finished(); }

    Status set_finishing(RuntimeState* state) override;
    bool is_finished() const override { return _is_finished || _join_builder->is_finished(); }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    std::string get_name() const override {
        return strings::Substitute("$0(HashJoiner=$1)", Operator::get_name(), _join_builder.get());
    }

    size_t output_amplification_factor() const override;

protected:
    HashJoinerPtr _join_builder;
    PartialRuntimeFilterMerger* _partial_rf_merger;
    mutable size_t _avg_keys_perf_bucket = 0;
    std::atomic<bool> _is_finished = false;

    const TJoinDistributionMode::type _distribution_mode;
};

class HashJoinBuildOperatorFactory : public OperatorFactory {
public:
    HashJoinBuildOperatorFactory(int32_t id, int32_t plan_node_id, HashJoinerFactoryPtr hash_joiner_factory,
                                 std::unique_ptr<PartialRuntimeFilterMerger>&& partial_rf_merger,
                                 TJoinDistributionMode::type distribution_mode,
                                 SpillProcessChannelFactoryPtr spill_channel_factory);
    ~HashJoinBuildOperatorFactory() override = default;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;
    void retain_string_key_columns(int32_t driver_sequence, Columns&& columns);

    const auto& hash_joiner_factory() { return _hash_joiner_factory; }

protected:
    HashJoinerFactoryPtr _hash_joiner_factory;
    std::unique_ptr<PartialRuntimeFilterMerger> _partial_rf_merger;
    std::vector<Columns> _string_key_columns;
    const TJoinDistributionMode::type _distribution_mode;
    SpillProcessChannelFactoryPtr _spill_channel_factory;
};

} // namespace starrocks::pipeline
