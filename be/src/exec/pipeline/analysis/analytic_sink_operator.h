// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/operator.h"
#include "exec/vectorized/analytor.h"

namespace starrocks::pipeline {
class AnalyticSinkOperator : public Operator {
public:
    AnalyticSinkOperator(int32_t id, int32_t plan_node_id, const TPlanNode& tnode, AnalytorPtr analytor)
            : Operator(id, "analytic_sink", plan_node_id), _tnode(tnode), _analytor(analytor) {}
    ~AnalyticSinkOperator() = default;

    bool has_output() const override { return false; }
    bool need_input() const override { return true; }
    bool is_finished() const override;
    void finish(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    Status _process_by_partition_if_necessary();
    void _process_by_partition_for_unbounded_frame(size_t chunk_size, bool is_new_partition);
    void _process_by_partition_for_unbounded_preceding_range_frame(size_t chunk_size, bool is_new_partition);
    void _process_by_partition_for_unbounded_preceding_rows_frame(size_t chunk_size, bool is_new_partition);
    void _process_by_partition_for_sliding_frame(size_t chunk_size, bool is_new_partition);
    void (AnalyticSinkOperator::*_process_by_partition)(size_t chunk_size, bool is_new_partition) = nullptr;

    TPlanNode _tnode;
    // It is used to perform analytic algorithms
    // shared by AnalyticSourceOperator
    AnalytorPtr _analytor = nullptr;
    // Whether prev operator has no output
    bool _is_finished = false;
};

class AnalyticSinkOperatorFactory final : public OperatorFactory {
public:
    AnalyticSinkOperatorFactory(int32_t id, int32_t plan_node_id, const TPlanNode& tnode, AnalytorPtr analytor)
            : OperatorFactory(id, "analytic_sink", plan_node_id), _tnode(tnode), _analytor(analytor) {}

    ~AnalyticSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<AnalyticSinkOperator>(_id, _plan_node_id, _tnode, _analytor);
    }

private:
    TPlanNode _tnode;
    AnalytorPtr _analytor = nullptr;
};
} // namespace starrocks::pipeline
