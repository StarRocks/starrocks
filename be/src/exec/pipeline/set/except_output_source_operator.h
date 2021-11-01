// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/operator.h"
#include "exec/pipeline/set/except_context.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {

// ExceptNode is decomposed to ExceptBuildSinkOperator, ExceptProbeSinkOperator, and ExceptOutputSourceOperator.
// - ExceptBuildSinkOperator (BUILD) builds the hast set from the output rows of ExceptNode's first child.
// - ExceptProbeSinkOperator (PROBE) labels keys as deleted in the hash set from the output rows of reset children.
//   PROBE depends on BUILD, which means it should wait for BUILD to finish building the hast set.
//   Multiple PROBEs from multiple children can be parallelized to label keys as deleted.
// - ExceptOutputSourceOperator (OUTPUT) traverses the hast set and outputs undeleted rows.
//   OUTPUT depends on all the PROBEs, which means it should wait for PROBEs to finish labeling keys as delete.
//
// The input chunks of BUILD and PROBE are shuffled by the local shuffle operator.
// The number of shuffled partitions is the degree of parallelism (DOP), which means
// the number of partition hash sets and the number of BUILD drivers, PROBE drivers of one child, OUTPUT drivers
// are both DOP. And each pair of BUILD/PROBE/OUTPUT drivers shares a same except partition context.
class ExceptOutputSourceOperator final : public SourceOperator {
public:
    ExceptOutputSourceOperator(int32_t id, int32_t plan_node_id, std::shared_ptr<ExceptContext> except_ctx)
            : SourceOperator(id, "except_output_source", plan_node_id), _except_ctx(std::move(except_ctx)) {}

    bool has_output() const override {
        return _except_ctx->is_erase_ht_finished() && !_except_ctx->is_output_finished();
    }

    bool is_finished() const override {
        return _except_ctx->is_erase_ht_finished() && _except_ctx->is_output_finished();
    }

    // Finish is noop.
    void finish(RuntimeState* state) override {}

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

private:
    std::shared_ptr<ExceptContext> _except_ctx;
};

class ExceptOutputSourceOperatorFactory final : public SourceOperatorFactory {
public:
    ExceptOutputSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                      ExceptPartitionContextFactoryPtr except_partition_ctx_factory)
            : SourceOperatorFactory(id, "except_output_source", plan_node_id),
              _except_partition_ctx_factory(std::move(except_partition_ctx_factory)) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<ExceptOutputSourceOperator>(
                _id, _plan_node_id, _except_partition_ctx_factory->get_or_create(driver_sequence));
    }

    void close(RuntimeState* state) override;

private:
    ExceptPartitionContextFactoryPtr _except_partition_ctx_factory;
};

} // namespace starrocks::pipeline
