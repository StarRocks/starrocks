// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/operator.h"
#include "exec/pipeline/set/intersect_context.h"

namespace starrocks::pipeline {

// IntersectNode is decomposed to IntersectBuildSinkOperator, IntersectProbeSinkOperator, and IntersectOutputSourceOperator.
// - IntersectBuildSinkOperator (BUILD) builds the hast set from the output rows of IntersectNode's first child.
// - IntersectProbeSinkOperator (PROBE) labels the hit times of a key as i in the hash set,if the original hit
//   times is i-1. The first PROBE depends on BUILD, and the rest i-th PROBE depends on the (i-1)-th PROBE.
// - IntersectOutputSourceOperator (OUTPUT) traverses the hast set and outputs rows,
//   whose hit times are equal to the number of PROBEs. OUTPUT depends on the last PROBE,
//   which means it should wait for all the PROBEs to finish labeling keys as delete.
//
// The input chunks of BUILD and PROBE are shuffled by the local shuffle operator.
// The number of shuffled partitions is the degree of parallelism (DOP), which means
// the number of partition hash sets and the number of BUILD drivers, PROBE drivers of one child, OUTPUT drivers
// are both DOP. And each pair of BUILD/PROBE/OUTPUT drivers shares a same intersect partition context.
class IntersectBuildSinkOperator final : public Operator {
public:
    IntersectBuildSinkOperator(int32_t id, int32_t plan_node_id, std::shared_ptr<IntersectContext> intersect_ctx,
                               const std::vector<ExprContext*>& dst_exprs)
            : Operator(id, "intersect_build_sink", plan_node_id),
              _intersect_ctx(std::move(intersect_ctx)),
              _dst_exprs(dst_exprs) {}

    bool need_input() const override { return !is_finished(); }

    bool has_output() const override { return false; }

    bool is_finished() const override { return _is_finished; }

    void finish(RuntimeState* state) override {
        if (!_is_finished) {
            _is_finished = true;
            _intersect_ctx->finish_build_ht();
        }
    }

    Status prepare(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    std::shared_ptr<IntersectContext> _intersect_ctx;

    const std::vector<ExprContext*>& _dst_exprs;

    bool _is_finished = false;
};

class IntersectBuildSinkOperatorFactory final : public OperatorFactory {
public:
    IntersectBuildSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                      IntersectPartitionContextFactoryPtr intersect_partition_ctx_factory,
                                      const std::vector<ExprContext*>& dst_exprs)
            : OperatorFactory(id, "intersect_build_sink", plan_node_id),
              _intersect_partition_ctx_factory(std::move(intersect_partition_ctx_factory)),
              _dst_exprs(dst_exprs) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<IntersectBuildSinkOperator>(
                _id, _plan_node_id, _intersect_partition_ctx_factory->get_or_create(driver_sequence), _dst_exprs);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    IntersectPartitionContextFactoryPtr _intersect_partition_ctx_factory;
    const std::vector<ExprContext*>& _dst_exprs;
};

} // namespace starrocks::pipeline