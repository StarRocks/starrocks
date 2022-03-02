// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
                          HashJoinerPtr probe_hash_joiner, HashJoinerPtr build_hash_joiner);
    ~HashJoinProbeOperator() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_output() const override;
    bool need_input() const override;

    bool is_finished() const override;
    void set_finishing(RuntimeState* state) override;
    void set_finished(RuntimeState* state) override;

    bool is_ready() const override;
    std::string get_name() const override {
        return strings::Substitute("$0(HashJoiner=$1)", Operator::get_name(), _probe_hash_joiner.get());
    }

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk);
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state);

private:
    const HashJoinerPtr _probe_hash_joiner;
    // For non-broadcast join, _build_hash_joiner is identical to _probe_hash_joiner.
    // For broadcast join, _probe_hash_joiner references the hash table owned by _build_hash_joiner,
    // so increase the reference number of _build_hash_joiner to prevent it closing early.
    const HashJoinerPtr _build_hash_joiner;
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
