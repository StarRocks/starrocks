// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/pipeline/operator.h"
#include "exec/pipeline/set/except_context.h"

namespace starrocks::pipeline {

// Each ExceptProbeSinkOperator probes the hash set built by ExceptBuildSinkOperator and labels the key as deleted.
// For more detail information, see the comments of class ExceptBuildSinkOperator.
class ExceptProbeSinkOperator final : public Operator {
public:
    ExceptProbeSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                            std::shared_ptr<ExceptContext> except_ctx, const std::vector<ExprContext*>& dst_exprs,
                            const int32_t dependency_index)
            : Operator(factory, id, "except_probe_sink", plan_node_id, driver_sequence),
              _except_ctx(std::move(except_ctx)),
              _dst_exprs(dst_exprs),
              _dependency_index(dependency_index) {
        _except_ctx->ref();
    }

    void close(RuntimeState* state) override;

    bool need_input() const override {
        return _except_ctx->is_dependency_finished(_dependency_index) && !(_is_finished || _except_ctx->is_ht_empty());
    }

    bool has_output() const override { return false; }

    bool is_finished() const override {
        if (_except_ctx->is_finished()) {
            return true;
        }
        return _except_ctx->is_dependency_finished(_dependency_index) && (_is_finished || _except_ctx->is_ht_empty());
    }

    Status set_finishing(RuntimeState* state) override {
        _is_finished = true;
        _except_ctx->finish_probe_ht();
        return Status::OK();
    }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    ExceptContextPtr _except_ctx;

    const std::vector<ExprContext*>& _dst_exprs;

    bool _is_finished = false;
    const int32_t _dependency_index;
};

class ExceptProbeSinkOperatorFactory final : public OperatorFactory {
public:
    ExceptProbeSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                   ExceptPartitionContextFactoryPtr except_partition_ctx_factory,
                                   const std::vector<ExprContext*>& dst_exprs, const int32_t dependency_index)
            : OperatorFactory(id, "except_probe_sink", plan_node_id),
              _except_partition_ctx_factory(std::move(except_partition_ctx_factory)),
              _dst_exprs(dst_exprs),
              _dependency_index(dependency_index) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        ExceptContextPtr except_ctx = _except_partition_ctx_factory->get_or_create(driver_sequence);
        return std::make_shared<ExceptProbeSinkOperator>(this, _id, _plan_node_id, driver_sequence,
                                                         std::move(except_ctx), _dst_exprs, _dependency_index);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    ExceptPartitionContextFactoryPtr _except_partition_ctx_factory;

    const std::vector<ExprContext*>& _dst_exprs;
    const int32_t _dependency_index;
};

} // namespace starrocks::pipeline
