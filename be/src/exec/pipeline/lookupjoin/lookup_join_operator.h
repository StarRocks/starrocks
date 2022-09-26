// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/lookupjoin/lookup_join_context.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/literal.h"
#include "runtime/descriptors.h"

namespace starrocks::pipeline {

// TODO: Merge with IndexSeekOperator
// LookupJoinOperator
class LookupJoinOperator final : public OperatorWithDependency {
public:
    LookupJoinOperator(OperatorFactory* factory, int32_t id,
                       int32_t plan_node_id, const int32_t driver_sequence,
                       const std::shared_ptr<LookupJoinContext>& lookup_join_context)
            : OperatorWithDependency(factory, id, "lookup_join", plan_node_id, driver_sequence),
              _lookup_join_context(lookup_join_context),
              _lookup_join_params(_lookup_join_context->params()),
              _other_join_conjuncts(_lookup_join_params.other_join_conjunct_ctxs) {}

    ~LookupJoinOperator() override = default;

    void close(RuntimeState* state) override;
    bool is_ready() const override { return _lookup_join_context->is_ready(); }
    bool has_output() const override { return true; }
    bool need_input() const override { return !is_finished(); }
    bool is_finished() const override { return _is_finished; }

    Status set_finishing(RuntimeState* state) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    const std::shared_ptr<LookupJoinContext>& _lookup_join_context;
    const LookupJoinContextParams& _lookup_join_params;
    const std::vector<ExprContext*>& _other_join_conjuncts;

    ChunkPtr _cur_chunk = nullptr;
    bool _is_finished = false;
};

class LookupJoinOperatorFactory final : public OperatorFactory {
public:
    LookupJoinOperatorFactory(int32_t id, int32_t plan_node_id,
                              const std::shared_ptr<LookupJoinContext>& lookup_join_context)
            : OperatorFactory(id, "lookupjoin", plan_node_id),
              _lookup_join_context(lookup_join_context){
        _lookup_join_context->ref();
    }

    ~LookupJoinOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<LookupJoinOperator>(this, _id, _plan_node_id, driver_sequence,
                                                    _lookup_join_context);
    }

private:
    const std::shared_ptr<LookupJoinContext>& _lookup_join_context;
};

} // namespace starrocks::pipeline
