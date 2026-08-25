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

#include "exec/pipeline/analysis/analytic_sink_operator.h"
#include "exec/pipeline/exchange/mem_limited_chunk_queue.h"

namespace starrocks::pipeline {

// Sink side of the spillable analytic operator (see the analytic-spill comment
// block in analytor.h). Window states update chunk by chunk, the raw chunks go
// into the shared input run and every sealed partition publishes a descriptor
// record; the stream memory limit is the spill policy (force -> 0, auto ->
// threshold). Backpressure and cold-block flushing are the streams' business;
// this operator never waits on IO.
class SpillableAnalyticSinkOperator final : public AnalyticSinkOperator {
public:
    SpillableAnalyticSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                  const TPlanNode& tnode, AnalytorPtr&& analytor)
            : AnalyticSinkOperator(factory, id, plan_node_id, driver_sequence, tnode, std::move(analytor),
                                   "spillable_analytic_sink") {}
    ~SpillableAnalyticSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    bool need_input() const override {
        if (!AnalyticSinkOperator::need_input()) {
            return false;
        }
        // False while a stream flush is in flight (completion is observed by
        // the next poll), while the sealed-but-unconsumed backlog exceeds its
        // limit, or once the consumer side closed early (LIMIT).
        if (_analytor->partition_streams_enabled() && !_analytor->store_can_push()) {
            return false;
        }
        return true;
    }

    Status set_finishing(RuntimeState* state) override;
};

class SpillableAnalyticSinkOperatorFactory final : public AnalyticSinkOperatorFactory {
public:
    SpillableAnalyticSinkOperatorFactory(int32_t id, int32_t plan_node_id, const TPlanNode& tnode,
                                         AnalytorFactoryPtr analytor_factory)
            : AnalyticSinkOperatorFactory(id, plan_node_id, tnode, std::move(analytor_factory),
                                          "spillable_analytic_sink") {}
    ~SpillableAnalyticSinkOperatorFactory() override = default;

    // The run store has no event-scheduler wakeups for can_push/can_pop; the
    // polling scheduler drives these pipelines.
    bool support_event_scheduler() const override { return false; }

    Status prepare(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    RuntimeState* _state = nullptr;
    MemLimitedChunkQueue::Options _input_run_options;
};

} // namespace starrocks::pipeline
