// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/exec_node.h"
#include "exprs/expr_context.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {
class ProjectNode final : public ExecNode {
public:
    ProjectNode(ObjectPool* pool, const TPlanNode& node, const DescriptorTbl& desc);

    ~ProjectNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status reset(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    // Only for compatibility
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;

    void push_down_predicate(RuntimeState* state, std::list<ExprContext*>* expr_ctxs, bool is_vectorized) override;
    void push_down_join_runtime_filter(RuntimeState* state,
                                       vectorized::RuntimeFilterProbeCollector* collector) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

private:
    std::vector<SlotId> _slot_ids;
    std::vector<ExprContext*> _expr_ctxs;
    std::vector<bool> _type_is_nullable;

    std::vector<SlotId> _common_sub_slot_ids;
    std::vector<ExprContext*> _common_sub_expr_ctxs;

    RuntimeProfile::Counter* _expr_compute_timer = nullptr;
    RuntimeProfile::Counter* _common_sub_expr_compute_timer = nullptr;

    DictOptimizeParser _dict_optimize_parser;
};

} // namespace starrocks::vectorized