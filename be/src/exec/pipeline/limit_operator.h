// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/operator.h"

namespace starrocks {
namespace pipeline {
class LimitOperator final : public Operator {
public:
    LimitOperator(int32_t id, int32_t plan_node_id, int64_t limit)
            : Operator(id, "limit", plan_node_id), _limit(limit) {}

    ~LimitOperator() override = default;

    bool has_output() const override { return _cur_chunk != nullptr; }

    bool need_input() const override { return _limit != 0 && _cur_chunk == nullptr; }

    bool is_finished() const override { return _limit == 0 && _cur_chunk == nullptr; }

    void finish(RuntimeState* state) override { _limit = 0; }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    int64_t _limit = 0;
    vectorized::ChunkPtr _cur_chunk = nullptr;
};

class LimitOperatorFactory final : public OperatorFactory {
public:
    LimitOperatorFactory(int32_t id, int32_t plan_node_id, int64_t limit)
            : OperatorFactory(id, "limit", plan_node_id), _limit(limit) {}

    ~LimitOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<LimitOperator>(_id, _plan_node_id, _limit);
    }

private:
    int64_t _limit = 0;
};

} // namespace pipeline
} // namespace starrocks
