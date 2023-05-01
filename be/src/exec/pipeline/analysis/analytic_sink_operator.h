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
#include "exec/pipeline/operator.h"

namespace starrocks::pipeline {
class AnalyticSinkOperator : public Operator {
public:
    AnalyticSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                         const TPlanNode& tnode, AnalytorPtr&& analytor)
            : Operator(factory, id, "analytic_sink", plan_node_id, driver_sequence),
              _tnode(tnode),
              _analytor(std::move(analytor)) {
        _analytor->ref();
    }
    ~AnalyticSinkOperator() override = default;

    bool has_output() const override { return false; }
    bool need_input() const override { return !is_finished(); }
    bool is_finished() const override { return _is_finished || _analytor->reached_limit() || _analytor->is_finished(); }
    Status set_finishing(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    using ProcessByPartitionIfNecessaryFunc = Status (AnalyticSinkOperator::*)();
    using ProcessByPartitionFunc = void (AnalyticSinkOperator::*)(size_t chunk_size, bool is_new_partition);

    // Process partition when all the data of current partition is reached
    Status _process_by_partition_if_necessary_materializing();
    // For window frame `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
    Status _process_by_partition_if_necessary_for_unbounded_preceding_rows_frame_streaming();
    // For window frame `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
    Status _process_by_partition_if_necessary_for_unbounded_preceding_range_frame_streaming();
    ProcessByPartitionIfNecessaryFunc _process_by_partition_if_necessary = nullptr;

    // For window frame `ROWS|RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
    void _process_by_partition_for_unbounded_frame(size_t chunk_size, bool is_new_partition);
    // For window frame `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
    // materializing means that although the frame is `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, we
    // cannot evaluate window function until all the data of current partition is reached
    // For example, `ntile` need all the data to calculate the bucket step
    void _process_by_partition_for_unbounded_preceding_rows_frame_materializing(size_t chunk_size,
                                                                                bool is_new_partition);
    // For window frame `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
    // materializing means that although the frame is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, we
    // cannot evaluate window function until all the data of current partition is reached
    // For example, `cume_dist` need all the data to calculate
    void _process_by_partition_for_unbounded_preceding_range_frame_materializing(size_t chunk_size,
                                                                                 bool is_new_partition);
    // For window frame `ROWS BETWEEN N PRECEDING AND CURRENT ROW`
    void _process_by_partition_for_sliding_frame(size_t chunk_size, bool is_new_partition);
    ProcessByPartitionFunc _process_by_partition = nullptr;

    TPlanNode _tnode;
    // It is used to perform analytic algorithms
    // shared by AnalyticSourceOperator
    AnalytorPtr _analytor = nullptr;
    // Whether prev operator has no output
    bool _is_finished = false;
};

class AnalyticSinkOperatorFactory final : public OperatorFactory {
public:
    AnalyticSinkOperatorFactory(int32_t id, int32_t plan_node_id, const TPlanNode& tnode,
                                AnalytorFactoryPtr analytor_factory)
            : OperatorFactory(id, "analytic_sink", plan_node_id),
              _tnode(tnode),
              _analytor_factory(std::move(analytor_factory)) {}

    ~AnalyticSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        auto analytor = _analytor_factory->create(driver_sequence);
        return std::make_shared<AnalyticSinkOperator>(this, _id, _plan_node_id, driver_sequence, _tnode,
                                                      std::move(analytor));
    }

private:
    TPlanNode _tnode;
    AnalytorFactoryPtr _analytor_factory;
};
} // namespace starrocks::pipeline
