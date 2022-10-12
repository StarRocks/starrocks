// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/analytor.h"

namespace starrocks::pipeline {
class AnalyticSourceOperator : public SourceOperator {
public:
    AnalyticSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                           AnalytorPtr&& analytor)
            : SourceOperator(factory, id, "analytic_source", plan_node_id, driver_sequence),
              _analytor(std::move(analytor)) {
        _analytor->ref();
    }
    ~AnalyticSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;

    Status set_finished(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    // It is used to perform analytic algorithms
    // shared by AnalyticSinkOperator
    AnalytorPtr _analytor = nullptr;
};

class AnalyticSourceOperatorFactory final : public SourceOperatorFactory {
public:
    AnalyticSourceOperatorFactory(int32_t id, int32_t plan_node_id, const AnalytorFactoryPtr& analytor_factory)
            : SourceOperatorFactory(id, "analytic_source", plan_node_id), _analytor_factory(analytor_factory) {}

    ~AnalyticSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        auto analytor = _analytor_factory->create(driver_sequence);
        return std::make_shared<AnalyticSourceOperator>(this, _id, _plan_node_id, driver_sequence, std::move(analytor));
    }

private:
    AnalytorFactoryPtr _analytor_factory = nullptr;
};
} // namespace starrocks::pipeline
