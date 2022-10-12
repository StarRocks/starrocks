// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <exprs/predicate.h>

#include <atomic>

#include "exec/pipeline/hashjoin/hash_joiner_factory.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/vectorized/hash_joiner.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"

namespace starrocks {
namespace pipeline {

using HashJoiner = starrocks::vectorized::HashJoiner;

class HashJoinBuildOperator final : public Operator {
public:
    HashJoinBuildOperator(OperatorFactory* factory, int32_t id, const string& name, int32_t plan_node_id,
                          int32_t driver_sequence, HashJoinerPtr join_builder,
                          const std::vector<HashJoinerPtr>& only_probers, PartialRuntimeFilterMerger* partial_rf_merger,
                          TJoinDistributionMode::type distribution_mode);
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

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    std::string get_name() const override {
        return strings::Substitute("$0(HashJoiner=$1)", Operator::get_name(), _join_builder.get());
    }

private:
    HashJoinerPtr _join_builder;
    // Assign the readable hash table from _join_builder to each only probe hash_joiner,
    // when _join_builder finish building the hash tbale.
    const std::vector<HashJoinerPtr>& _read_only_join_probers;
    PartialRuntimeFilterMerger* _partial_rf_merger;
    bool _is_finished = false;

    const TJoinDistributionMode::type _distribution_mode;
};

class HashJoinBuildOperatorFactory final : public OperatorFactory {
public:
    HashJoinBuildOperatorFactory(int32_t id, int32_t plan_node_id, HashJoinerFactoryPtr hash_joiner_factory,
                                 std::unique_ptr<PartialRuntimeFilterMerger>&& partial_rf_merger,
                                 TJoinDistributionMode::type distribution_mode);
    ~HashJoinBuildOperatorFactory() override = default;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;
    void retain_string_key_columns(int32_t driver_sequence, vectorized::Columns&& columns);

private:
    HashJoinerFactoryPtr _hash_joiner_factory;
    std::unique_ptr<PartialRuntimeFilterMerger> _partial_rf_merger;
    std::vector<vectorized::Columns> _string_key_columns;
    const TJoinDistributionMode::type _distribution_mode;
};

} // namespace pipeline
} // namespace starrocks
