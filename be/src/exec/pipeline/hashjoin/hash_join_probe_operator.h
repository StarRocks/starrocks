// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

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
    HashJoinProbeOperator(int32_t id, const string& name, int32_t plan_node_id, HashJoinerPtr hash_joiner);
    ~HashJoinProbeOperator() = default;

    Status prepare(RuntimeState* state) override { return OperatorWithDependency::prepare(state); }

    Status close(RuntimeState* state) override {
        RETURN_IF_ERROR(_hash_joiner->unref(state));
        return OperatorWithDependency::close(state);
    }

    bool has_output() const override;
    bool need_input() const override;

    bool is_finished() const override;
    void set_finishing(RuntimeState* state) override;
    void set_finished(RuntimeState* state) override;

    bool is_ready() const override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk);
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state);

private:
    HashJoinerPtr _hash_joiner;
    bool _is_finished = false;
};

class HashJoinProbeOperatorFactory final : public OperatorFactory {
public:
    HashJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id, HashJoinerFactoryPtr hash_joiner);

    ~HashJoinProbeOperatorFactory() = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    HashJoinerFactoryPtr _hash_joiner_factory;
};
} // namespace pipeline
} // namespace starrocks
