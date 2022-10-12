// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {

// EmptySetOperator returns an empty result set.
class EmptySetOperator final : public SourceOperator {
public:
    EmptySetOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "empty_set", plan_node_id, driver_sequence) {}

    ~EmptySetOperator() override = default;

    bool has_output() const override { return false; }

    bool is_finished() const override { return true; }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override {
        return Status::InternalError("Shouldn't pull chunk from empty set operator");
    }
};

class EmptySetOperatorFactory final : public SourceOperatorFactory {
public:
    EmptySetOperatorFactory(int32_t id, int32_t plan_node_id) : SourceOperatorFactory(id, "empty_set", plan_node_id) {}

    ~EmptySetOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<EmptySetOperator>(this, _id, _plan_node_id, driver_sequence);
    }
};

} // namespace starrocks::pipeline
