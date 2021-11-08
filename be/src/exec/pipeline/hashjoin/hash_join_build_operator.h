// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once
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
    HashJoinBuildOperator(int32_t id, const string& name, int32_t plan_node_id, HashJoiner* hash_joiner);
    ~HashJoinBuildOperator() = default;
    Status prepare(RuntimeState* state) override { return Operator::prepare(state); };
    Status close(RuntimeState* state) override { return Operator::close(state); };

    bool has_output() const override {
        CHECK(false) << "has_output not supported in HashJoinBuildOperator";
        return false;
    }

    bool need_input() const override { return !is_finished(); }

    bool is_finished() const override { return _is_finished; }

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    void finish(RuntimeState* state) override;

private:
    HashJoiner* _hash_joiner;
    bool _is_finished = false;
};
class HashJoinBuildOperatorFactory final : public OperatorFactory {
public:
    HashJoinBuildOperatorFactory(int32_t id, int32_t plan_node_id, HashJoiner* hash_joiner);
    ~HashJoinBuildOperatorFactory() = default;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    HashJoiner* _hash_joiner;
};
} // namespace pipeline
} // namespace starrocks
