// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "exec/stream/lookupjoin/lookup_join_context.h"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {

class LookupJoinProbeOperator final : public Operator {
public:
    LookupJoinProbeOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                            const std::shared_ptr<LookupJoinContext>& lookup_join_context);

    ~LookupJoinProbeOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    // Control flow
    bool is_finished() const override { return _is_finished && _lookup_join_context->is_finished(); }
    bool has_output() const override { return false; }
    bool need_input() const override { return !is_finished(); }
    Status set_finishing(RuntimeState* state) override;

    // Data flow
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
private:
    const std::shared_ptr<LookupJoinContext>& _lookup_join_context;
    bool _is_finished;
};

class LookupJoinProbeOperatorFactory final : public pipeline::OperatorFactory {
public:
    LookupJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id,
                                   const std::shared_ptr<LookupJoinContext>& lookup_join_context)
            : OperatorFactory(id, "stream_join", plan_node_id), _lookup_join_context(lookup_join_context) {}

    ~LookupJoinProbeOperatorFactory() override = default;

    pipeline::OperatorPtr create(int32_t dop, int32_t driver_seq) override {
        return std::make_shared<LookupJoinProbeOperator>(this, _id, _plan_node_id, driver_seq, _lookup_join_context);
    }

private:
    const std::shared_ptr<LookupJoinContext>& _lookup_join_context;
};

} // namespace starrocks::pipeline