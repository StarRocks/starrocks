// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/operator.h"
#include "exec/pipeline/set/except_context.h"

namespace starrocks::pipeline {

// ExceptNode is decomposed to ExceptBuildSinkOperator, ExceptEraseSinkOperator, and ExceptOutputSourceOperator.
// - ExceptBuildSinkOperator (BUILD) builds the hast set from the output rows of ExceptNode's first child.
// - ExceptEraseSinkOperator (ERASE) labels keys as deleted in the hash set from the output rows of reset children.
//   ERASE depends on BUILD, which means it should wait for BUILD to finish building the hast set.
//   Multiple ERASEs from multiple children can be parallelized to label keys as deleted.
// - ExceptOutputSourceOperator (OUTPUT) traverses the hast set and outputs undeleted rows.
//   OUTPUT depends on all the ERASEs, which means it should wait for ERASEs to finish labeling keys as delete.
//
// The input trunks of BUILD and ERASE are shuffled by the local shuffle operator.
// The number of shuffled partitions is the degree of parallelism (DOP), which means
// the number of partition hash sets and the number of BUILD drivers, ERASE drivers of one child, OUTPUT drivers
// are both DOP. And each pair of BUILD/ERASE/OUTPUT drivers shares a same except partition context.
class ExceptBuildSinkOperator final : public Operator {
public:
    ExceptBuildSinkOperator(int32_t id, int32_t plan_node_id, std::shared_ptr<ExceptContext> except_ctx,
                            const std::vector<ExprContext*>& dst_exprs)
            : Operator(id, "except_build_sink", plan_node_id),
              _except_ctx(std::move(except_ctx)),
              _dst_exprs(dst_exprs) {}

    bool need_input() const override { return !is_finished(); }

    bool has_output() const override { return false; }

    bool is_finished() const override { return _is_finished; }

    void finish(RuntimeState* state) override {
        if (!_is_finished) {
            _is_finished = true;
            _except_ctx->finish_build_ht();
        }
    }

    Status prepare(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    std::shared_ptr<ExceptContext> _except_ctx;

    const std::vector<ExprContext*>& _dst_exprs;

    bool _is_finished = false;
};

class ExceptBuildSinkOperatorFactory final : public OperatorFactory {
public:
    ExceptBuildSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                   ExceptPartitionContextFactoryPtr except_partition_ctx_factory,
                                   const std::vector<ExprContext*>& dst_exprs)
            : OperatorFactory(id, "except_build_sink", plan_node_id),
              _except_partition_ctx_factory(std::move(except_partition_ctx_factory)),
              _dst_exprs(dst_exprs) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<ExceptBuildSinkOperator>(
                _id, _plan_node_id, _except_partition_ctx_factory->get_or_create(driver_sequence), _dst_exprs);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    ExceptPartitionContextFactoryPtr _except_partition_ctx_factory;

    const std::vector<ExprContext*>& _dst_exprs;
};

} // namespace starrocks::pipeline
