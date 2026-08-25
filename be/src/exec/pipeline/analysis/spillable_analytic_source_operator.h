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

#include "exec/pipeline/analysis/analytic_source_operator.h"

namespace starrocks::pipeline {

// Source side of the spillable analytic operator: pops the raw chunks back
// from the shared input run in arrival order, authorizes them against the
// sealed-partition descriptors and attaches the per-partition window results.
// The streams' can_pop gates has_output, so blocks flushed to disk are
// prefetched asynchronously and this operator never waits on IO.
class SpillableAnalyticSourceOperator final : public AnalyticSourceOperator {
public:
    SpillableAnalyticSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                    AnalytorPtr&& analytor)
            : AnalyticSourceOperator(factory, id, plan_node_id, driver_sequence, std::move(analytor),
                                     "spillable_analytic_source") {}
    ~SpillableAnalyticSourceOperator() override = default;

    void close(RuntimeState* state) override;

    bool has_output() const override;
    bool is_finished() const override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    // Sticky: set once this operator has a conjunct / runtime filter that would
    // rewrite the output chunk in place, which forbids the zero-copy replay.
    bool _output_may_be_filtered = false;
};

class SpillableAnalyticSourceOperatorFactory final : public AnalyticSourceOperatorFactory {
public:
    SpillableAnalyticSourceOperatorFactory(int32_t id, int32_t plan_node_id, AnalytorFactoryPtr analytor_factory)
            : AnalyticSourceOperatorFactory(id, plan_node_id, std::move(analytor_factory),
                                            "spillable_analytic_source") {}
    ~SpillableAnalyticSourceOperatorFactory() override = default;

    // See SpillableAnalyticSinkOperatorFactory: the run store has no
    // event-scheduler wakeups, the polling scheduler drives these pipelines.
    bool support_event_scheduler() const override { return false; }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        auto analytor = _analytor_factory->create(driver_sequence);
        return std::make_shared<SpillableAnalyticSourceOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                 std::move(analytor));
    }
};

} // namespace starrocks::pipeline
