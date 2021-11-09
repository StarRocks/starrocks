// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/vectorized/hash_joiner.h"
namespace starrocks {
namespace pipeline {
using HashJoiner = starrocks::vectorized::HashJoiner;
class HashJoinProbeOperator final : public OperatorWithDependency {
public:
    HashJoinProbeOperator(int32_t id, const string& name, int32_t plan_node_id, HashJoiner* hash_joiner);
    ~HashJoinProbeOperator() = default;

    Status prepare(RuntimeState* state) override { return OperatorWithDependency::prepare(state); }

    Status close(RuntimeState* state) override { return OperatorWithDependency::close(state); }

    bool has_output() const override;
    bool need_input() const override;
    bool is_finished() const override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk);
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state);
    void finish(RuntimeState* state) override;
    bool is_ready() const override;

private:
    HashJoiner* _hash_joiner;
    bool _is_finished = false;
};

class HashJoinProbeOperatorFactory final : public OperatorFactory {
public:
    HashJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id, std::unique_ptr<HashJoiner>&& hash_joiner);

    ~HashJoinProbeOperatorFactory() = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    std::unique_ptr<HashJoiner> _hash_joiner;
};
} // namespace pipeline
} // namespace starrocks
