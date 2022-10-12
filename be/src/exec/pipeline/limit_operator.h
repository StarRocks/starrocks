// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/pipeline/operator.h"

namespace starrocks {
namespace pipeline {
class LimitOperator final : public Operator {
public:
    LimitOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                  std::atomic<int64_t>& limit)
            : Operator(factory, id, "limit", plan_node_id, driver_sequence), _limit(limit) {}

    ~LimitOperator() override = default;

    bool has_output() const override { return _cur_chunk != nullptr; }

    bool need_input() const override { return !_is_finished && _limit != 0 && _cur_chunk == nullptr; }

    bool is_finished() const override { return (_is_finished || _limit == 0) && _cur_chunk == nullptr; }

    Status set_finishing(RuntimeState* state) override {
        _is_finished = true;
        return Status::OK();
    }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    bool _is_finished = false;
    std::atomic<int64_t>& _limit;
    vectorized::ChunkPtr _cur_chunk = nullptr;
};

class LimitOperatorFactory final : public OperatorFactory {
public:
    LimitOperatorFactory(int32_t id, int32_t plan_node_id, int64_t limit)
            : OperatorFactory(id, "limit", plan_node_id), _limit(limit) {}

    ~LimitOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<LimitOperator>(this, _id, _plan_node_id, driver_sequence, _limit);
    }

private:
    std::atomic<int64_t> _limit;
};

} // namespace pipeline
} // namespace starrocks
