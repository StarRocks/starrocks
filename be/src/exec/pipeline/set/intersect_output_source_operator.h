// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/operator.h"
#include "exec/pipeline/set/intersect_context.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {

// IntersectOutputSourceOperator traverses the hast set and picks up entries hit by all
// IntersectProbeSinkOperators after probe phase is finished.
// For more detail information, see the comments of class IntersectBuildSinkOperator.
class IntersectOutputSourceOperator final : public SourceOperator {
public:
    IntersectOutputSourceOperator(int32_t id, int32_t plan_node_id, std::shared_ptr<IntersectContext> intersect_ctx,
                                  const int32_t dependency_index)
            : SourceOperator(id, "intersect_output_source", plan_node_id),
              _intersect_ctx(std::move(intersect_ctx)),
              _dependency_index(dependency_index) {}

    bool has_output() const override {
        return _intersect_ctx->is_dependency_finished(_dependency_index) && !_intersect_ctx->is_output_finished();
    }

    bool is_finished() const override {
        return _intersect_ctx->is_dependency_finished(_dependency_index) && _intersect_ctx->is_output_finished();
    }

    // Finish is noop.
    void finish(RuntimeState* state) override {}

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

private:
    std::shared_ptr<IntersectContext> _intersect_ctx;
    const int32_t _dependency_index;
};

class IntersectOutputSourceOperatorFactory final : public SourceOperatorFactory {
public:
    IntersectOutputSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                         IntersectPartitionContextFactoryPtr intersect_partition_ctx_factory,
                                         const int32_t dependency_index)
            : SourceOperatorFactory(id, "intersect_output_source", plan_node_id),
              _intersect_partition_ctx_factory(std::move(intersect_partition_ctx_factory)),
              _dependency_index(dependency_index) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<IntersectOutputSourceOperator>(
                _id, _plan_node_id, _intersect_partition_ctx_factory->get_or_create(driver_sequence),
                _dependency_index);
    }

    void close(RuntimeState* state) override;

private:
    IntersectPartitionContextFactoryPtr _intersect_partition_ctx_factory;
    const int32_t _dependency_index;
};

} // namespace starrocks::pipeline
