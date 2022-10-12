// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/pipeline/hashjoin/hash_joiner_factory.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/vectorized/hash_joiner.h"

namespace starrocks {
namespace pipeline {

using HashJoiner = starrocks::vectorized::HashJoiner;

class HashJoinProbeOperator final : public OperatorWithDependency {
public:
    HashJoinProbeOperator(OperatorFactory* factory, int32_t id, const string& name, int32_t plan_node_id,
                          int32_t driver_sequence, HashJoinerPtr join_prober, HashJoinerPtr join_builder);
    ~HashJoinProbeOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;
    bool need_input() const override;

    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    bool is_ready() const override;
    std::string get_name() const override {
        return strings::Substitute("$0(HashJoiner=$1)", Operator::get_name(), _join_prober.get());
    }

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk);
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state);

private:
    const HashJoinerPtr _join_prober;
    // For non-broadcast join, _join_builder is identical to _join_prober.
    // For broadcast join, _join_prober references the hash table owned by _join_builder,
    // so increase the reference number of _join_builder to prevent it closing early.
    const HashJoinerPtr _join_builder;
    bool _is_finished = false;
};

class HashJoinProbeOperatorFactory final : public OperatorFactory {
public:
    HashJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id, HashJoinerFactoryPtr hash_joiner);

    ~HashJoinProbeOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    HashJoinerFactoryPtr _hash_joiner_factory;
};

} // namespace pipeline
} // namespace starrocks
