// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/operator.h"
#include "exec/vectorized/analytor.h"

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

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    using ProcessByPartitionIfNecessaryFunc = Status (AnalyticSinkOperator::*)();
    using ProcessByPartitionFunc = void (AnalyticSinkOperator::*)(size_t chunk_size, bool is_new_partition);

    Status _process_by_partition_if_necessary_for_other();
    // As for the frame `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROWS`, some window functions needn't
    // use the partition end. For them, we could process before the current partition is finished.
    Status _process_by_partition_if_necessary_for_unbounded_preceding_rows_frame_without_partition_end();
    ProcessByPartitionIfNecessaryFunc _process_by_partition_if_necessary = nullptr;

    void _process_by_partition_for_unbounded_frame(size_t chunk_size, bool is_new_partition);
    void _process_by_partition_for_unbounded_preceding_range_frame(size_t chunk_size, bool is_new_partition);
    void _process_by_partition_for_unbounded_preceding_rows_frame_with_partition_end(size_t chunk_size,
                                                                                     bool is_new_partition);
    void _process_by_partition_for_unbounded_preceding_rows_frame_without_partition_end(size_t chunk_size);
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
                                const AnalytorFactoryPtr& analytor_factory)
            : OperatorFactory(id, "analytic_sink", plan_node_id), _tnode(tnode), _analytor_factory(analytor_factory) {}

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
