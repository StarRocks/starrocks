// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <utility>

#include "exec/analytor.h"
#include "exec/pipeline/source_operator.h"

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

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    // It is used to perform analytic algorithms
    // shared by AnalyticSinkOperator
    AnalytorPtr _analytor = nullptr;
};

class AnalyticSourceOperatorFactory final : public SourceOperatorFactory {
public:
    AnalyticSourceOperatorFactory(int32_t id, int32_t plan_node_id, AnalytorFactoryPtr analytor_factory)
            : SourceOperatorFactory(id, "analytic_source", plan_node_id),
              _analytor_factory(std::move(analytor_factory)) {}

    ~AnalyticSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        auto analytor = _analytor_factory->create(driver_sequence);
        return std::make_shared<AnalyticSourceOperator>(this, _id, _plan_node_id, driver_sequence, std::move(analytor));
    }

private:
    AnalytorFactoryPtr _analytor_factory = nullptr;
};
} // namespace starrocks::pipeline
