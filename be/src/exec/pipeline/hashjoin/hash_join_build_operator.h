// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

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
                          HashJoinerPtr hash_joiner, size_t driver_sequence,
                          PartialRuntimeFilterMerger* partial_rf_merger,
                          const TJoinDistributionMode::type distribution_mode,
                          std::atomic<bool>& any_broadcast_builder_finished);
    ~HashJoinBuildOperator() = default;

    Status prepare(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    bool has_output() const override {
        CHECK(false) << "has_output not supported in HashJoinBuildOperator";
        return false;
    }
    bool need_input() const override { return !is_finished(); }

    void set_finishing(RuntimeState* state) override;
    bool is_finished() const override { return _is_finished || _hash_joiner->is_finished(); }

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    std::string get_name() const override {
        return strings::Substitute("$0(HashJoiner=$1)", Operator::get_name(), _hash_joiner.get());
    }

private:
    HashJoinerPtr _hash_joiner;
    size_t _driver_sequence;
    PartialRuntimeFilterMerger* _partial_rf_merger;
    bool _is_finished = false;

    const TJoinDistributionMode::type _distribution_mode;
    std::atomic<bool>& _any_broadcast_builder_finished;
};

class HashJoinBuildOperatorFactory final : public OperatorFactory {
public:
    HashJoinBuildOperatorFactory(int32_t id, int32_t plan_node_id, HashJoinerFactoryPtr hash_joiner_factory,
                                 std::unique_ptr<PartialRuntimeFilterMerger>&& partial_rf_merger,
                                 const TJoinDistributionMode::type distribution_mode);
    ~HashJoinBuildOperatorFactory() = default;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    HashJoinerFactoryPtr _hash_joiner_factory;
    RuntimeFilterPort* _runtime_filter_port;
    std::unique_ptr<PartialRuntimeFilterMerger> _partial_rf_merger;

    const TJoinDistributionMode::type _distribution_mode;
    std::atomic<bool> _any_broadcast_builder_finished{false};
};

} // namespace pipeline
} // namespace starrocks
