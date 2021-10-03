// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/analytor.h"

namespace starrocks::pipeline {
class AnalyticSourceOperator : public SourceOperator {
public:
    AnalyticSourceOperator(int32_t id, int32_t plan_node_id, AnalytorPtr analytor)
            : SourceOperator(id, "analytic_source", plan_node_id), _analytor(std::move(analytor)) {}
    ~AnalyticSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;
    void finish(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    // It is used to perform analytic algorithms
    // shared by AnalyticSinkOperator
    AnalytorPtr _analytor = nullptr;
    // Whether prev operator has no output
    bool _is_finished = false;
};

class AnalyticSourceOperatorFactory final : public SourceOperatorFactory {
public:
    AnalyticSourceOperatorFactory(int32_t id, int32_t plan_node_id, AnalytorPtr analytor)
            : SourceOperatorFactory(id, plan_node_id), _analytor(std::move(analytor)) {}

    ~AnalyticSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<AnalyticSourceOperator>(_id, _plan_node_id, _analytor);
    }

private:
    AnalytorPtr _analytor = nullptr;
};
} // namespace starrocks::pipeline
